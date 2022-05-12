open Core
open Ast
module A = Abslayout
open Abslayout
module V = Visitors
open Schema
open Match.Query
module P = Pred.Infix
include (val Log.make ~level:(Some Warning) "castor.simplify-tactic")
module Config = Ops.Config

module Make (C : Config.S) = struct
  open C
  module O = Ops.Make (C)
  open O

  let filter_const r =
    match r.node with
    | Filter (`Bool true, r') -> Some r'
    | Filter (`Bool false, _) -> Some empty
    | Filter (p, r') -> Some (A.filter (Pred.simplify p) r')
    | _ -> None

  let filter_const = of_func filter_const ~name:"filter-const"

  let join_simplify r =
    match r.node with
    | Join { pred; r1; r2 } -> Some (A.join (Pred.simplify pred) r1 r2)
    | _ -> None

  let join_simplify = of_func join_simplify ~name:"join-simplify"

  let elim_structure r =
    let r = strip_meta r in
    let r' =
      match r.node with
      | AHashIdx h -> Some (Layout_to_depjoin.hash_idx h)
      | AOrderedIdx o -> Some (Layout_to_depjoin.ordered_idx o)
      | AList l -> Some (Layout_to_depjoin.list l)
      | _ -> None
    in
    Option.map r' ~f:dep_join'

  let elim_structure = of_func elim_structure ~name:"elim-structure"

  let concat_select ps ps' =
    let module Sl = Select_list in
    let ns = Sl.names ps |> Iter.to_list |> Set.of_list (module String) in
    let ps' = Sl.filter ps' ~f:(fun _ n -> not (Set.mem ns n)) in
    ps @ ps'

  let mk_unscoped_select scope ps =
    List.map ps ~f:(fun (p, n) -> (Pred.unscoped scope p, n))

  let elim_depjoin r =
    let open Option.Let_syntax in
    let%bind { d_lhs; d_alias; d_rhs } = to_depjoin r in
    match d_rhs.node with
    | AScalar s ->
        return
        @@ select (mk_unscoped_select d_alias [ (s.s_pred, s.s_name) ]) d_lhs
    | ATuple (rs, Cross) ->
        let%bind s =
          List.map rs ~f:(fun r ->
              match r.node with
              | AScalar p -> Some (p.s_pred, p.s_name)
              | _ -> None)
          |> Option.all
        in
        return (select (mk_unscoped_select d_alias s) d_lhs)
        (* depjoin(r, select(ps, atuple(ps'))) -> select(ps, select(ps', r)) *)
    | Select (ps, { node = ATuple (rs, Cross); _ }) ->
        let%bind ps' =
          List.map rs ~f:(fun r ->
              match r.node with
              | AScalar p -> Some (p.s_pred, p.s_name)
              | _ -> None)
          |> Option.all
        in
        (* Ensure that no fields are dropped by the first select by concatting on all of the fields. *)
        let ps' =
          concat_select
            (mk_unscoped_select d_alias ps')
            (schema d_lhs |> to_select_list)
        in
        return (select (mk_unscoped_select d_alias ps) (select ps' d_lhs))
    | Select (ps, { node = AScalar { s_pred = `Null None; _ }; _ }) ->
        return (select (mk_unscoped_select d_alias ps) d_lhs)
    | _ -> None

  let elim_depjoin = of_func elim_depjoin ~name:"elim-depjoin"

  let flatten_select r =
    let open Option.Let_syntax in
    let%bind ps, r' = to_select r in
    let%bind ps', r'' = to_select r' in
    let%bind () =
      match (A.select_kind ps, A.select_kind ps') with
      | `Agg, `Agg -> None
      | _ -> Some ()
    in
    let ctx =
      List.map ps' ~f:(fun (p, n) -> (Name.create n, p))
      |> Map.of_alist_exn (module Name)
    in
    let ps = Select_list.map ps ~f:(fun p _ -> Pred.subst ctx p) in
    return @@ A.select ps r''

  let flatten_select = of_func flatten_select ~name:"flatten-select"

  let flatten_dedup r =
    match r.node with
    | Dedup { node = Dedup r'; _ } -> Some (A.dedup r')
    | _ -> None

  let flatten_dedup = of_func flatten_dedup ~name:"flatten-dedup"

  let elim_filter_above_join r =
    let open Option.Let_syntax in
    let%bind p, r = to_filter r in
    let%map p', r1, r2 = to_join r in
    join P.(p && p') r1 r2

  let elim_filter_above_join =
    of_func elim_filter_above_join ~name:"elim-filter-above-join"

  (** Eliminate dedup(groupby(...)) if all of the groupby keys appear in the select list. *)
  let elim_dedup_above_groupby r =
    let open Option.Let_syntax in
    let%bind r' = to_dedup r in
    let%bind sel, key, _ = to_groupby r' in
    let sel_names =
      Select_list.names sel |> Iter.map Name.create |> Iter.to_list
      |> Set.of_list (module Name)
    in
    if List.for_all key ~f:(Set.mem sel_names) then return r' else None

  let elim_dedup_above_groupby =
    of_func elim_dedup_above_groupby ~name:"elim-dedup-above-groupby"

  let simplify =
    seq_many
      [
        (* Drop constant filters if possible. *)
        try_ @@ for_all filter_const Path.(all >>? is_filter);
        try_ @@ for_all join_simplify Path.(all >>? is_join);
        (* Eliminate complex structures in compile time position. *)
        try_ @@ fix
        @@ at_ elim_structure
             Path.(
               all >>? is_compile_time
               >>? Infix.(is_list || is_hash_idx || is_ordered_idx)
               >>| shallowest);
        try_ @@ for_all elim_depjoin Path.(all >>? is_depjoin);
        try_ @@ for_all flatten_select Path.(all >>? is_select);
        try_ @@ for_all flatten_dedup Path.(all >>? is_dedup);
        try_ @@ for_all flatten_select Path.(all >>? is_select);
        try_ @@ for_all elim_filter_above_join Path.(all >>? is_filter);
        try_ @@ for_all elim_dedup_above_groupby Path.(all >>? is_dedup);
      ]

  let project =
    let project r =
      Some (r |> Resolve.resolve_exn ~params |> Project.project_once ~params)
    in
    of_func project ~name:"project"

  let unnest_and_simplify _ r =
    let simplify q = Option.value_exn (apply simplify Path.root q) in
    let start_time = Time.now () in
    let ret =
      (r :> Ast.t)
      |> simplify |> Unnest.unnest ~params
      |> Cardinality.extend ~dedup:false
      |> Join_elim.remove_joins |> strip_meta |> simplify
    in
    let end_time = Time.now () in
    info (fun m ->
        m "Simplify ran in %a" Time.Span.pp (Time.diff end_time start_time));
    Some ret

  let unnest_and_simplify = global unnest_and_simplify "unnest-and-simplify"
end

let simplify ?(params = Set.empty (module Name)) conn r =
  let module C = struct
    let conn = conn
    let params = params
  end in
  let module O = Ops.Make (C) in
  let module S = Make (C) in
  Option.value_exn (O.apply S.unnest_and_simplify Path.root r)

