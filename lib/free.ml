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

let free_open free r =
  let pred_free = pred_free_open free
  and empty = Set.empty (module Name)
  and of_list = Set.of_list (module Name)
  and union_list = Set.union_list (module Name) in
  let exposed ?scope r =
    of_list (Schema.schema r |> List.map ~f:(Name.copy ~scope))
  in
  let free_set =
    match r with
    | Relation _ | AEmpty -> empty
    | Range (p, p') -> Set.O.(pred_free p || pred_free p')
    | AScalar p -> select_list_free pred_free [ (p.s_pred, p.s_name) ]
    | Select (ps, r') ->
        Set.O.(free r' || (select_list_free pred_free ps - exposed r'))
    | Filter (p, r') ->
        Set.union (free r') (Set.diff (pred_free p) (exposed r'))
    | Dedup r' -> free r'
    | DepJoin { d_lhs; d_alias; d_rhs } ->
        Set.O.(free d_lhs || (free d_rhs - exposed ~scope:d_alias d_lhs))
    | Join { pred; r1; r2 } ->
        Set.O.(
          free r1 || free r2 || (pred_free pred - (exposed r1 || exposed r2)))
    | GroupBy (ps, key, r') ->
        Set.O.(
          free r'
          || ((select_list_free pred_free ps || of_list key) - exposed r'))
    | OrderBy { key; rel } ->
        Set.O.(
          free rel
          || (List.map key ~f:(fun (p, _) -> pred_free p) |> union_list)
             - exposed rel)
    | AList { l_keys = r'; l_values = r''; l_scope = scope } ->
        Set.O.((free r' - exposed r') || (free r'' - exposed ~scope r'))
    | AHashIdx h ->
        Set.O.(
          union_list
            [
              free h.hi_keys - exposed h.hi_keys;
              free h.hi_values - exposed ~scope:h.hi_scope h.hi_keys;
              List.map ~f:pred_free h.hi_lookup |> union_list;
            ])
    | AOrderedIdx
        { oi_keys = r'; oi_values = r''; oi_lookup; oi_scope = scope; _ } ->
        let one_bound_free = function
          | Some (p, _) -> pred_free p
          | None -> empty
        in
        let bound_free (b1, b2) =
          Set.union (one_bound_free b1) (one_bound_free b2)
        in
        Set.O.(
          union_list
            ([ free r' - exposed r'; free r'' - exposed ~scope r' ]
            @ List.map ~f:bound_free oi_lookup))
    | ATuple (rs, (Zip | Concat | Cross)) -> List.map rs ~f:free |> union_list
    | _ -> failwith "unsupported"
  in
  free_set

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
