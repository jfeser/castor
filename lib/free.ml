module A = Abslayout
open Collections
open Ast
module V = Visitors

let pred_free_open free p =
  let singleton = Set.singleton (module Name) in
  let visitor =
    object (self : 'a)
      inherit [_] V.reduce as super
      inherit [_] Util.set_monoid (module Name)
      method visit_subquery r = free r
      method! visit_Name () n = singleton n

      method! visit_pred () p =
        match p with
        | Exists r | First r -> self#visit_subquery r
        | _ -> super#visit_pred () p
    end
  in
  visitor#visit_pred () p

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
    | AScalar p -> pred_free p
    | Select (ps, r') ->
        Set.O.(
          free r' || ((List.map ps ~f:pred_free |> union_list) - exposed r'))
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
          || (List.map ps ~f:pred_free |> union_list || of_list key)
             - exposed r')
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
