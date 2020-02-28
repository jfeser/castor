open Core
open Castor
open Ast
open Abslayout
open Abslayout_visitors
open Schema

module Config = struct
  module type S = sig
    include Ops.Config.S
  end
end

module Make (C : Config.S) = struct
  module Ops = Ops.Make (C)
  open Ops

  let filter_const r =
    match r.node with
    | Filter (Bool true, r') -> Some r'
    | Filter (Bool false, _) -> Some empty
    | _ -> None

  let filter_const = of_func filter_const ~name:"filter-const"

  let elim_structure r =
    let r = map_meta (fun _ -> ()) r in
    let r' =
      match r.node with
      | AHashIdx h -> Some (hash_idx_to_depjoin h)
      | AOrderedIdx (rk, rv, m) -> Some (ordered_idx_to_depjoin rk rv m)
      | AList (rk, rv) -> Some (list_to_depjoin rk rv)
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
        Some (select [ Pred.unscoped d_alias p ] (strip_scope d_lhs))
    | DepJoin { d_lhs; d_alias; d_rhs = { node = ATuple (rs, Cross); _ } } ->
        let open Option.Let_syntax in
        let%bind s =
          List.map rs ~f:(fun r ->
              match r.node with AScalar p -> Some p | _ -> None)
          |> Option.all
        in
        let s = List.map ~f:(Pred.unscoped d_alias) s in
        Some (select s (strip_scope d_lhs))
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
        Some (select ps (select ps' (strip_scope d_lhs)))
    | DepJoin
        {
          d_lhs;
          d_alias;
          d_rhs = { node = Select (ps, { node = AScalar (Null None); _ }); _ };
        } ->
        Some
          (select (List.map ~f:(Pred.unscoped d_alias) ps) (strip_scope d_lhs))
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

  let flatten_select r =
    match r.node with
    | Select (ps, { node = Select (ps', r); _ }) ->
        let ctx =
          List.filter_map ps' ~f:(fun p ->
              Option.map (Pred.to_name p) ~f:(fun n -> (n, p)))
          |> Map.of_alist_exn (module Name)
        in
        let ps = List.map ps ~f:(Pred.subst ctx) in
        Some (select ps r)
    | _ -> None

  let flatten_select = of_func flatten_select ~name:"flatten-select"

  let flatten_dedup r =
    match r.node with Dedup { node = Dedup r'; _ } -> Some r' | _ -> None

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

  let simplify =
    seq_many
      [
        (* Drop constant filters if possible. *)
        for_all filter_const Path.(all >>? is_filter);
        (* Eliminate complex structures in compile time position. *)
        fix
          (at_ elim_structure
             Path.(
               all >>? is_compile_time
               >>? Infix.(is_list || is_hash_idx || is_ordered_idx)
               >>| shallowest));
        for_all elim_depjoin Path.(all >>? is_depjoin);
        for_all flatten_select Path.(all >>? is_select);
        for_all flatten_dedup Path.(all >>? is_dedup);
        for_all flatten_tuple Path.(all >>? is_tuple);
        for_all dealias_select Path.(all >>? is_select);
        for_all flatten_select Path.(all >>? is_select);
      ]
end
