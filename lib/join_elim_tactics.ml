open! Base
open Castor
open Abslayout
open Collections

module Config = struct
  module type S = sig
    include Ops.Config.S
  end
end

module Make (C : Config.S) = struct
  module O = Ops.Make (C)
  open O

  let elim_join_nest r =
    match r.node with
    | Join {pred; r1; r2} ->
        let scope = Fresh.name Global.fresh "s%d" in
        let pred = Pred.scoped (schema_exn r1) scope pred in
        let slist =
          let r1_schema = Schema.scoped scope (schema_exn r1) in
          r1_schema @ schema_exn r2 |> List.map ~f:(fun n -> Name n)
        in
        Some (dep_join r1 scope (select slist (filter pred r2)))
    | _ -> None

  let elim_join_nest = of_func elim_join_nest ~name:"elim-join-nest"

  let elim_join_hash r =
    match r.node with
    | Join {pred= Binop (Eq, kl, kr); r1; r2} ->
        let join_scope = Fresh.name Global.fresh "s%d" in
        let hash_scope = Fresh.name Global.fresh "s%d" in
        let slist =
          let r1_schema = Schema.scoped join_scope (schema_exn r1) in
          r1_schema @ schema_exn r2 |> List.map ~f:(fun n -> Name n)
        in
        let r1_scoped = Pred.scoped (schema_exn r1) in
        let layout =
          dep_join r1 join_scope
            (select slist
               (hash_idx
                  (dedup (select [kl] r1))
                  hash_scope
                  (filter (r1_scoped hash_scope (Binop (Eq, kl, kr))) r2)
                  [r1_scoped join_scope kl]))
        in
        Logs.debug (fun m -> m "%a" pp layout) ;
        Some layout
    | _ -> None

  let elim_join_hash = of_func elim_join_hash ~name:"elim-join-hash"
end
