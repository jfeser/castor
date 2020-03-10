open! Base
open Ast
open Schema
open Collections
module P = Pred.Infix
module A = Abslayout
open Match

module Config = struct
  module type S = sig
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
    let tuple_elems =
      ( schema r1 |> Schema.scoped scope |> Schema.to_select_list
      |> List.map ~f:A.scalar )
      @ [ A.filter pred r2 ]
    in
    Some (A.list r1 scope (A.tuple tuple_elems Cross))

  let elim_join_nest = of_func elim_join_nest ~name:"elim-join-nest"

  let elim_join_hash r =
    let open Option.Let_syntax in
    let%bind pred, r1, r2 = to_join r in
    match pred with
    | Binop (Eq, kl, kr) ->
        let join_scope = Fresh.name Global.fresh "s%d" in
        let hash_scope = Fresh.name Global.fresh "s%d" in
        let slist =
          let r1_schema = Schema.scoped join_scope (schema r1) in
          r1_schema @ schema r2 |> List.map ~f:P.name
        in
        let r1_scoped = Pred.scoped (schema r1) in
        let layout =
          A.dep_join r1 join_scope
            (A.select slist
               (A.hash_idx
                  (A.dedup (A.select [ kl ] r1))
                  hash_scope
                  (A.filter (r1_scoped hash_scope (Binop (Eq, kl, kr))) r2)
                  [ r1_scoped join_scope kl ]))
        in
        Logs.debug (fun m -> m "%a" Abslayout.pp layout);
        Some layout
    | _ -> None

  let elim_join_hash = of_func elim_join_hash ~name:"elim-join-hash"

  let elim_join_filter r =
    let open Option.Let_syntax in
    let%map pred, r1, r2 = to_join r in
    A.filter pred (A.join (Bool true) r1 r2)

  let elim_join_filter = of_func elim_join_filter ~name:"elim-join-filter"
end
