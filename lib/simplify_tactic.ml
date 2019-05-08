open Core
open Castor
open Abslayout

module Config = struct
  module type S = sig
    include Ops.Config.S

    include Abslayout_db.Config.S
  end
end

module Make (C : Config.S) = struct
  module Ops = Ops.Make (C)
  open Ops
  module M = Abslayout_db.Make (C)

  let filter_const r =
    match r.node with
    | Filter (Bool true, r') -> Some r'
    | Filter (Bool false, _) -> Some empty
    | _ -> None

  let filter_const = of_func filter_const ~name:"filter-const"

  let elim_structure r =
    match r.node with
    | AHashIdx (rk, rv, {lookup; _}) ->
        let scope = scope_exn rk in
        let key_pred =
          let rk_schema = schema_exn rk in
          List.map2_exn rk_schema lookup ~f:(fun p1 p2 ->
              Binop (Eq, Name p1, p2) )
          |> Pred.conjoin
        in
        Some (dep_join (strip_scope rk) scope (filter key_pred rv))
    | AOrderedIdx (rk, rv, {lookup_low; lookup_high; _}) ->
        let scope = scope_exn rk in
        let key_pred =
          let rk_schema = schema_exn rk in
          match rk_schema with
          | [n] ->
              Pred.conjoin
                [ Binop (Lt, lookup_low, Name n)
                ; Binop (Lt, Name n, lookup_high) ]
          | _ -> failwith "Unexpected schema."
        in
        Some (dep_join (strip_scope rk) scope (filter key_pred rv))
    | AList (rk, rv) ->
        let scope = scope_exn rk in
        Some (dep_join (strip_scope rk) scope rv)
    | _ -> None

  let elim_structure = of_func elim_structure ~name:"elim-structure"

  let elim_depjoin r =
    match r.node with
    | DepJoin {d_lhs; d_alias; d_rhs= {node= AScalar p; _}} ->
        Some (select [Pred.unscoped d_alias p] (strip_scope d_lhs))
    | DepJoin {d_lhs; d_alias; d_rhs= {node= ATuple (rs, Cross); _}} ->
        let open Option.Let_syntax in
        let%bind s =
          List.map rs ~f:(fun r ->
              match r.node with AScalar p -> Some p | _ -> None )
          |> Option.all
        in
        let s = List.map ~f:(Pred.unscoped d_alias) s in
        Some (select s (strip_scope d_lhs))
    | DepJoin
        {d_lhs; d_alias; d_rhs= {node= Select (ps, {node= AScalar Null; _}); _}}
      ->
        Some
          (select (List.map ~f:(Pred.unscoped d_alias) ps) (strip_scope d_lhs))
    | _ -> None

  let elim_depjoin = of_func elim_depjoin ~name:"elim-depjoin"

  let flatten_select r =
    match r.node with
    | Select (ps, {node= Select (ps', r); _}) ->
        let ctx =
          List.filter_map ps' ~f:(fun p ->
              Option.map (Pred.to_name p) ~f:(fun n -> (n, p)) )
          |> Map.of_alist_exn (module Name)
        in
        let ps = List.map ps ~f:(Pred.subst ctx) in
        Some (select ps r)
    | _ -> None

  let flatten_select = of_func flatten_select ~name:"flatten-select"

  let simplify =
    seq_many
      [ (* Drop constant filters if possible. *)
        for_all filter_const Path.(all >>? is_filter)
      ; (* Eliminate complex structures in compile time position. *)
        fix
          (at_ elim_structure
             Path.(
               all >>? is_compile_time
               >>? Infix.(is_list || is_hash_idx || is_ordered_idx)
               >>| shallowest))
      ; for_all elim_depjoin Path.(all >>? is_depjoin)
      ; for_all flatten_select Path.(all >>? is_select) ]
end
