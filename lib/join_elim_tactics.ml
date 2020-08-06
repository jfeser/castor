open! Base
open Ast
open Schema
open Collections
module P = Pred.Infix
module A = Abslayout
open Match

module Config = struct
  module type S = sig
    val params : Set.M(Name).t

    include Ops.Config.S
  end
end

module Make (C : Config.S) = struct
  open Ops.Make (C)

  let elim_join_nest r =
    let open Option.Let_syntax in
    let%bind pred, r1, r2 = to_join r in
    let scope = Fresh.name Global.fresh "s%d" in
    let pred = Pred.scoped (schema r1) scope pred in
    let lhs =
      let scalars =
        schema r1 |> Schema.scoped scope |> Schema.to_select_list
        |> List.map ~f:A.scalar
      in
      A.tuple scalars Cross
    and rhs = A.filter pred r2 in
    return @@ A.list r1 scope @@ A.tuple [ lhs; rhs ] Cross

  let elim_join_nest = of_func elim_join_nest ~name:"elim-join-nest"

  let elim_join_hash r =
    let open Option.Let_syntax in
    let%bind pred, r1, r2 = to_join r in
    match pred with
    | Binop (Eq, kl, kr) ->
        let join_scope = Fresh.name Global.fresh "s%d"
        and hash_scope = Fresh.name Global.fresh "s%d"
        and r1_schema = schema r1
        and r2_schema = schema r2 in
        let layout =
          let slist =
            let r1_schema = Schema.scoped join_scope r1_schema in
            r1_schema @ schema r2 |> List.map ~f:P.name
          in
          A.dep_join r1 join_scope @@ A.select slist
          @@ A.hash_idx
               (A.dedup @@ A.select [ kr ] r2)
               hash_scope
               (A.filter
                  (Binop (Eq, Pred.scoped r2_schema hash_scope kr, kr))
                  r2)
               [ Pred.scoped r1_schema join_scope kl ]
        in
        Some layout
    | _ -> None

  let elim_join_hash = of_func elim_join_hash ~name:"elim-join-hash"

  let elim_join_filter r =
    let open Option.Let_syntax in
    let%map pred, r1, r2 = to_join r in
    A.filter pred (A.join (Bool true) r1 r2)

  let elim_join_filter = of_func elim_join_filter ~name:"elim-join-filter"

  let hoist_join_param_filter r =
    let open Option.Let_syntax in
    let%bind pred, r1, r2 = to_join r in
    let has_params p =
      not (Set.is_empty @@ Set.inter (Free.pred_free p) C.params)
    in
    let hoist, keep = Pred.conjuncts pred |> List.partition_tf ~f:has_params in
    if List.is_empty hoist then None
    else
      return
      @@ A.filter (Pred.conjoin hoist)
      @@ A.join (Pred.conjoin keep) r1 r2

  let hoist_join_param_filter =
    of_func hoist_join_param_filter ~name:"hoist-join-param-filter"

  let split_out path pk =
    let open A in
    let pk = A.name_of_string_exn pk in
    let eq = [%compare.equal: Name.t] in
    let fresh_name = Fresh.name Global.fresh in

    of_func ~name:"split-out" @@ fun r ->
    let open Option.Let_syntax in
    let%bind path = path r in
    let rel = Path.get_exn path r in
    let rel_schema = Schema.schema rel in

    let schema = schema r in
    if List.mem schema pk ~equal:eq then
      let scope = fresh_name "s%d" in
      let alias = Name.scoped scope @@ Name.fresh "x%d" in
      (* Partition the schema of the original layout between the two new layouts. *)
      let fst_sel_list, snd_sel_list =
        List.partition_tf schema ~f:(fun n ->
            eq n pk
            || not
                 (List.mem rel_schema n ~equal:(fun n n' ->
                      String.(Name.(name n = name n')))))
      in
      let scope2 = fresh_name "s%d" in
      Option.return
      @@ dep_join
           (select (Schema.to_select_list fst_sel_list) r)
           scope2
           (hash_idx
              (dedup (select [ As_pred (Name pk, Name.name alias) ] rel))
              scope
              (select
                 ( List.map fst_sel_list ~f:(fun n ->
                       Name (Name.scoped scope2 n))
                 @ Schema.to_select_list snd_sel_list )
                 (filter (Binop (Eq, Name pk, Name alias)) rel))
              [ Name (Name.scoped scope2 pk) ])
    else None

  (* let hoist_join_left r =
   *   let open Option.Let_syntax in
   *   let%bind p1, r1, r2 = to_join r in
   *   let%bind p2, r3, r4 = to_join r1 in
   *   return A.join p2 (A.join p1)
   *   let has_params p =
   *     not (Set.is_empty @@ Set.inter (Free.pred_free p) C.params)
   *   in
   *   let hoist, keep = Pred.conjuncts pred |> List.partition_tf ~f:has_params in
   *   if List.is_empty hoist then None
   *   else
   *     return
   *     @@ A.filter (Pred.conjoin hoist)
   *     @@ A.join (Pred.conjoin keep) r1 r2 *)
end
