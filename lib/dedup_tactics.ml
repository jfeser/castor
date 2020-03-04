open Ast
open Abslayout
module P = Pred.Infix

module Config = struct
  module type S = sig
    include Ops.Config.S

    include Tactics_util.Config.S
  end
end

module Make (C : Config.S) = struct
  module O = Ops.Make (C)
  module Tactics_util = Tactics_util.Make (C)

  let to_dedup r = match r.node with Dedup r -> Some r | _ -> None

  let elim_dedup r =
    let open Option.Let_syntax in
    let%bind r = to_dedup r in
    match Cardinality.estimate r with
    | Abs_int.Interval (l, h) when h <= 1 -> Some r
    | _ -> None

  let elim_dedup = O.of_func elim_dedup ~name:"elim-dedup"

  let push_dedup r =
    let open Option.Let_syntax in
    let%bind r = to_dedup r in
    match r.node with
    | Filter (p, r') -> Some (filter p (dedup r'))
    | Dedup r' -> Some (dedup r')
    | AScalar _ | AEmpty -> Some r
    | AHashIdx h -> Some (hash_idx' { h with hi_values = dedup h.hi_values })
    | AList (rk, rv) ->
        let scope = Abslayout.scope_exn rk in
        let rk = Abslayout.strip_scope rk in
        Some (list (dedup rk) scope (dedup rv))
    | AOrderedIdx (rk, rv, o) ->
        let scope = Abslayout.scope_exn rk in
        let rk = Abslayout.strip_scope rk in
        Some (ordered_idx (dedup rk) scope (dedup rv) o)
    | ATuple (ts, Cross) -> Some (tuple (List.map ts ~f:dedup) Cross)
    | Select _ -> None
    | _ -> None

  let push_dedup = O.of_func push_dedup ~name:"push-dedup"
end
