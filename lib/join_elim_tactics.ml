open! Base
open Castor
open Abslayout
open Collections

module Config = struct
  module type S = sig
    val fresh : Fresh.t

    include Ops.Config.S
  end
end

module Make (C : Config.S) = struct
  module O = Ops.Make (C)
  open O

  (* module F = Filter_tactics.Make (C)
   * open F *)
  open C

  let elim_join_nest r =
    match r.node with
    | Join {pred; r1; r2} ->
        let scope = Fresh.name fresh "s%d" in
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
        let join_scope = Fresh.name fresh "s%d" in
        let hash_scope = Fresh.name fresh "s%d" in
        let key = Fresh.name fresh "k%d" in
        let filter_pred = Binop (Eq, kr, Name (Name.create key)) in
        let slist =
          let r1_schema = Schema.scoped join_scope (schema_exn r1) in
          r1_schema @ schema_exn r2 |> List.map ~f:(fun n -> Name n)
        in
        Some
          (dep_join r1 join_scope
             (select slist
                (hash_idx
                   (dedup (select [kl] r1))
                   hash_scope
                   (filter
                      (Pred.scoped (schema_exn r1) hash_scope filter_pred)
                      r2)
                   { lookup= [Pred.scoped (schema_exn r1) join_scope kl]
                   ; hi_key_layout= None })))
    | _ -> None

  let elim_join_hash =
    (* seq *)
    of_func elim_join_hash ~name:"elim-join-hash"

  (* (fix (at_ push_filter Path.(all >>? is_const_filter >>| shallowest))) *)
end
