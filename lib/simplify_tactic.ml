open Core
open Ast
module A = Abslayout
open Abslayout
module V = Visitors
open Schema
open Match
module P = Pred.Infix
include (val Log.make ~level:(Some Warning) "castor.simplify-tactic")

module Config = struct
  module type S = sig
    include Ops.Config.S
  end
end

module Make (C : Config.S) = struct
  open C
  module O = Ops.Make (C)
  open O

  let filter_const r =
    match r.node with
    | Filter (Bool true, r') -> Some r'
    | Filter (Bool false, _) -> Some empty
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
    let ns = List.filter_map ps ~f:Pred.to_name |> Set.of_list (module Name) in
    let ps' =
      List.filter ps' ~f:(fun p ->
          Option.map (Pred.to_name p) ~f:(fun n -> not (Set.mem ns n))
          |> Option.value ~default:true)
    in
    ps @ ps'

  let elim_depjoin r =
    match r.node with
    | DepJoin { d_lhs; d_alias; d_rhs = { node = AScalar p; _ } } ->
        Some (select [ Pred.unscoped d_alias p ] d_lhs)
    | DepJoin { d_lhs; d_alias; d_rhs = { node = ATuple (rs, Cross); _ } } ->
        let open Option.Let_syntax in
        let%bind s =
          List.map rs ~f:(fun r ->
              match r.node with AScalar p -> Some p | _ -> None)
          |> Option.all
        in
        let s = List.map ~f:(Pred.unscoped d_alias) s in
        Some (select s d_lhs)
    (* depjoin(r, select(ps, atuple(ps'))) -> select(ps, select(ps', r)) *)
    | DepJoin
        {
          d_lhs;
          d_alias;
          d_rhs = { node = Select (ps, { node = ATuple (rs, Cross); _ }); _ };
        } ->
        let open Option.Let_syntax in
        let%bind ps' =
          List.map rs ~f:(fun r ->
              match r.node with AScalar p -> Some p | _ -> None)
          |> Option.all
        in
        let ps' = List.map ~f:(Pred.unscoped d_alias) ps' in
        (* Ensure that no fields are dropped by the first select. *)
        let ps' = concat_select ps' (schema d_lhs |> to_select_list) in
        let ps = List.map ~f:(Pred.unscoped d_alias) ps in
        Some (select ps (select ps' d_lhs))
    | DepJoin
        {
          d_lhs;
          d_alias;
          d_rhs = { node = Select (ps, { node = AScalar (Null None); _ }); _ };
        } ->
        Some (select (List.map ~f:(Pred.unscoped d_alias) ps) d_lhs)
    | _ -> None

  let elim_depjoin = of_func elim_depjoin ~name:"elim-depjoin"

  let dealias_select r =
    match r.node with
    | Select (ps, r') ->
        let ps' =
          List.map ps ~f:(function
            | As_pred (Name n, a) when String.(Name.name n = a) -> Name n
            | p -> p)
        in
        Some (select ps' r')
    | _ -> None

  let dealias_select = of_func dealias_select ~name:"de-alias-select"

  module Subst = struct
    let select_list_open pred ctx ps =
      List.map ps ~f:(fun p ->
          match Pred.to_name p with
          | Some n -> P.as_ p @@ Name.name n
          | None -> p)
      |> List.map ~f:(pred ctx)
      |> Select_list.of_list

    let query_open annot pred ctx = function
      | Select (ps, r) ->
          let ps' = select_list_open pred ctx ps in
          Select (ps', annot ctx r)
      | GroupBy (ps, key, r) ->
          let ps' = select_list_open pred ctx ps
          and key' =
            List.map key ~f:(fun n ->
                match Map.find ctx n with Some n' -> n' | None -> n)
            |> List.dedup_and_sort ~compare:[%compare: Name.t]
          and r' = annot ctx r in
          GroupBy (ps', key', r')
      | q -> V.Map.query (annot ctx) (pred ctx) q

    let rec pred ctx = function
      | Name n as p -> (
          match Map.find ctx n with Some p' -> Name p' | None -> p)
      | p -> V.Map.pred (annot ctx) (pred ctx) p

    and query ctx q = query_open annot pred ctx q
    and annot ctx r = V.Map.annot (query ctx) r
  end

  let subst_path ctx path r =
    let subst_top r =
      V.Map.annot (Subst.query_open (fun _ r -> r) Subst.pred ctx) r
    in
    Path.get_exn path r |> subst_top |> Path.set_exn path r

  let rec subst_along_path ctx path r =
    let r' = subst_path ctx path r in
    let is_select =
      match r'.node with Select _ | GroupBy _ -> true | _ -> false
    in
    if is_select then r'
    else
      match Path.parent path with
      | Some path' -> subst_along_path ctx path' r'
      | None -> r'

  let elim_select p r =
    let open Option.Let_syntax in
    let%bind sel, r' = Path.get_exn p r |> to_select in
    let%bind subst =
      List.map sel ~f:(function
        | As_pred (Name n, n') -> Some (Name.create n', n)
        | _ -> None)
      |> Option.all
    in
    let subst_ctx = Map.of_alist_exn (module Name) subst in
    return @@ Path.set_exn p (subst_along_path subst_ctx p r) r'

  let elim_select = global elim_select "flatten-select"

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
      List.filter_map ps' ~f:(fun p ->
          Option.map (Pred.to_name p) ~f:(fun n -> (n, p)))
      |> Map.of_alist_exn (module Name)
    in
    let ps = List.map ps ~f:(Pred.subst ctx) in
    return @@ A.select ps r''

  let flatten_select = of_func flatten_select ~name:"flatten-select"

  let flatten_dedup r =
    match r.node with
    | Dedup { node = Dedup r'; _ } -> Some (A.dedup r')
    | _ -> None

  let flatten_dedup = of_func flatten_dedup ~name:"flatten-dedup"

  let flatten_tuple r =
    match r.node with
    | ATuple (ts, k) ->
        let ts =
          List.concat_map ts ~f:(fun r' ->
              match r'.node with
              | ATuple (ts', k') when Poly.(k = k') -> ts'
              | _ -> [ r' ])
        in
        Some (tuple ts k)
    | _ -> None

  let flatten_tuple = of_func flatten_tuple ~name:"flatten-tuple"

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
      List.filter_map sel ~f:Pred.to_name |> Set.of_list (module Name)
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
        try_ @@ for_all dealias_select Path.(all >>? is_select);
        try_ @@ for_all flatten_select Path.(all >>? is_select);
        try_ @@ for_all elim_filter_above_join Path.(all >>? is_filter);
        try_ @@ for_all elim_dedup_above_groupby Path.(all >>? is_dedup);
      ]

  let join_elim =
    let f _ r =
      Cardinality.annotate r |> Join_elim.remove_joins |> strip_meta
      |> Option.return
    in
    global f "join-elim"

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
