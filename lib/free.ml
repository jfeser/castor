module A = Abslayout
open Collections
open Ast
module V = Visitors

let rec pred_free_open free = function
  | `Name n -> Set.singleton (module Name) n
  | `Exists r | `First r -> free r
  | p ->
      V.Reduce.pred
        (Set.empty (module Name))
        Set.union free (pred_free_open free) p

let empty = Set.empty (module Name)
let of_list = Set.of_list (module Name)
let union_list = Set.union_list (module Name)

let select_list_free pred_free ps =
  List.map ps ~f:(fun (p, _) -> pred_free p) |> union_list

let zero = Set.map (module Name) ~f:Name.zero
let decr = Set.map (module Name) ~f:Name.decr

let free_open free =
  let pred_free = pred_free_open free in
  let schema_set = Schema.schema_set in
  let open Set.O in
  function
  | Relation _ | AEmpty -> empty
  | Range (p, p') -> pred_free p || pred_free p'
  | AScalar p -> select_list_free pred_free [ (p.s_pred, p.s_name) ]
  | Select (ps, r) -> free r || (select_list_free pred_free ps - schema_set r)
  | Filter (p, r) -> free r || (pred_free p - schema_set r)
  | Dedup r -> free r
  | Join { pred; r1; r2 } ->
      free r1 || free r2 || (pred_free pred - (schema_set r1 || schema_set r2))
  | GroupBy (ps, key, r) ->
      free r || ((select_list_free pred_free ps || of_list key) - schema_set r)
  | OrderBy { key; rel } ->
      free rel
      || (List.map key ~f:(fun (p, _) -> pred_free p) |> union_list)
         - schema_set rel
  | DepJoin { d_lhs = r; d_rhs = r' } | AList { l_keys = r; l_values = r' } ->
      free r || decr (free r' - zero (schema_set r))
  | AHashIdx h ->
      free h.hi_keys
      || decr
           ((free h.hi_values - zero (schema_set h.hi_keys))
           || List.map ~f:pred_free h.hi_lookup |> union_list)
  | AOrderedIdx { oi_keys = r'; oi_values = r''; oi_lookup; _ } ->
      let one_bound_free = function
        | Some (p, _) -> pred_free p
        | None -> empty
      in
      let bound_free (b1, b2) =
        Set.union (one_bound_free b1) (one_bound_free b2)
      in
      free r'
      || decr
           (union_list
              ([ free r'' - zero (schema_set r') ]
              @ List.map ~f:bound_free oi_lookup))
  | ATuple (rs, (Zip | Concat | Cross)) -> List.map rs ~f:free |> union_list
  | _ -> failwith "unsupported"

let rec free r = free_open free r.node
let pred_free p = pred_free_open free p

let annotate r =
  V.Annotate_obj.annot
    (fun m -> m#free)
    (fun m x ->
      object
        method meta = m
        method free = x
      end)
    free_open r
