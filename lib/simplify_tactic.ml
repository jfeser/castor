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
        let key_pred =
          let rk_schema = schema_exn rk in
          List.map2_exn rk_schema lookup ~f:(fun p1 p2 ->
              Binop (Eq, Name p1, p2) )
          |> Pred.conjoin
        in
        let values = schema_exn rv |> List.map ~f:(fun n -> Name n) in
        Some (select values (tuple [filter key_pred rk; rv] Cross))
    | AOrderedIdx (rk, rv, {lookup_low; lookup_high; _}) ->
        let key_pred =
          let rk_schema = schema_exn rk in
          match rk_schema with
          | [n] ->
              Pred.conjoin
                [ Binop (Lt, lookup_low, Name n)
                ; Binop (Lt, Name n, lookup_high) ]
          | _ -> failwith "Unexpected schema."
        in
        let values = schema_exn rv |> List.map ~f:(fun n -> Name n) in
        Some (select values (tuple [filter key_pred rk; rv] Cross))
    | AList (rk, rv) ->
        let values = schema_exn rv |> List.map ~f:(fun n -> Name n) in
        Some (select values (tuple [rk; rv] Cross))
    | _ -> None

  let elim_structure = of_func elim_structure ~name:"elim-structure"

  let simplify =
    seq_many
      [ (* Drop constant filters if possible. *)
        for_all filter_const Path.(all >>? is_filter)
        (* Eliminate complex structures in compile time position. *)
        (* fix
         *   (at_ elim_structure
         *      Path.(
         *        all >>? is_compile_time
         *        >>? Infix.(is_list || is_hash_idx || is_ordered_idx)
         *        >>| shallowest)) *)
       ]
end
