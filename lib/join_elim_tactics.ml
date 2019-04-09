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
    | Join {pred; r1; r2} -> Some (tuple [r1; filter pred r2] Cross)
    | _ -> None

  let elim_join_nest = of_func elim_join_nest ~name:"elim-join-nest"

  let elim_join_hash r =
    match r.node with
    | Join {pred= Binop (Eq, kl, kr); r1; r2} ->
        let key = Fresh.name fresh "k%d" in
        let filter_pred = Binop (Eq, kr, Name (Name.create key)) in
        Some
          (tuple
             [ r1
             ; hash_idx
                 (dedup (select [As_pred (kl, key)] r1))
                 (filter filter_pred r2) [kl] ]
             Cross)
    | _ -> None

  let elim_join_hash =
    (* seq *)
    of_func elim_join_hash ~name:"elim-join-hash"

  (* (fix (at_ push_filter Path.(all >>? is_const_filter >>| shallowest))) *)
end
