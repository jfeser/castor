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
    | Abs_int.Interval (_, h) when h <= 1 -> Some r
    | _ -> None

  let elim_dedup = O.of_func elim_dedup ~name:"elim-dedup"

  let lhs_visible lhs rhs =
    Set.is_subset
      (Set.of_list (module Name) (Schema.schema lhs))
      ~of_:(Set.of_list (module Name) (Schema.schema rhs))

  let push_dedup r =
    let open Option.Let_syntax in
    let%bind r = to_dedup r in
    match r.node with
    | Filter (p, r') -> Some (filter p (dedup r'))
    | Dedup r' -> Some (dedup r')
    | AScalar _ | AEmpty -> Some r
    | AHashIdx h -> return @@ hash_idx' { h with hi_values = dedup h.hi_values }
    | AList ({ l_keys = lhs; l_values = rhs; _ } as l) when lhs_visible lhs rhs
      ->
        return @@ list' { l with l_keys = dedup lhs; l_values = dedup rhs }
    | DepJoin ({ d_lhs = lhs; d_rhs = rhs; _ } as d) when lhs_visible lhs rhs ->
        return @@ dep_join' { d with d_lhs = dedup lhs; d_rhs = dedup rhs }
    | AOrderedIdx o ->
        (* TODO: This transform isn't correct unless the right hand sides are
           non-overlapping. *)
        return
        @@ ordered_idx'
             { o with oi_keys = dedup o.oi_keys; oi_values = dedup o.oi_values }
    | ATuple (ts, Cross) -> Some (tuple (List.map ts ~f:dedup) Cross)
    | Select _ -> None
    | _ -> None

  let push_dedup = O.of_func push_dedup ~name:"push-dedup"
end
