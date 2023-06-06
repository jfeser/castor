open Core
module A = Abslayout
open Collections
open Ast
module V = Visitors

let debug = false
let empty = Set.empty (module Name)
let of_list = Set.of_list (module Name)
let union_list = Set.union_list (module Name)

let rec pred_open annot = function
  | `Name n -> Set.singleton (module Name) n
  | `Exists r | `First r -> annot r
  | p ->
      V.Reduce.pred
        (Set.empty (module Name))
        Set.union annot (pred_open annot) p

let select_list_free pred_free ps =
  List.map ps ~f:(fun (p, _) -> pred_free p) |> union_list

let zero = Set.map (module Name) ~f:Name.zero
let decr_exn = Set.map (module Name) ~f:Name.decr_exn
let schema_set r = Schema.schema r |> Set.of_list (module Name)

let query_open ~schema annot =
  let pred = pred_open annot in
  let open Set.O in
  let run = function
    | Relation _ | AEmpty -> empty
    | Range (p, p') -> pred p || pred p'
    | AScalar p -> select_list_free pred [ (p.s_pred, p.s_name) ]
    | Select (ps, r) -> annot r || (select_list_free pred ps - schema r)
    | Filter (p, r) -> annot r || (pred p - schema r)
    | Dedup r -> annot r
    | Join { pred = p; r1; r2 } ->
        annot r1 || annot r2 || (pred p - (schema r1 || schema r2))
    | GroupBy (ps, key, r) ->
        annot r || ((select_list_free pred ps || of_list key) - schema r)
    | OrderBy { key; rel } ->
        annot rel
        || ((List.map key ~f:(fun (p, _) -> pred p) |> union_list) - schema rel)
    | DepJoin { d_lhs = r; d_rhs = r' } | AList { l_keys = r; l_values = r' } ->
        annot r || decr_exn (annot r' - zero (schema r))
    | AHashIdx h ->
        let lhs_schema = zero (schema h.hi_keys) in
        let rhs_free = annot h.hi_values - lhs_schema in
        annot h.hi_keys || decr_exn rhs_free
        || List.map ~f:pred h.hi_lookup |> union_list
    | AOrderedIdx { oi_keys = r'; oi_values = r''; oi_lookup; _ } ->
        let one_bound_free = function Some (p, _) -> pred p | None -> empty in
        let bound_free (b1, b2) =
          Set.union (one_bound_free b1) (one_bound_free b2)
        in
        annot r'
        || decr_exn (annot r'' - zero (schema r'))
        || union_list (List.map ~f:bound_free oi_lookup)
    | ATuple (rs, (Zip | Concat | Cross)) -> List.map rs ~f:annot |> union_list
    | (Limit _ | Call _) as q ->
        raise_s [%message "Not implemented" (q : (_, _) query)]
  in
  fun r ->
    Util.reraise
      (fun exn ->
        Exn.create_s [%message "Free.query_open" (r : (_, _) query) (exn : exn)])
      (fun () ->
        let ret = run r in
        if debug then
          print_s
            [%message "free" (r : (_ pred, _) query) (ret : Set.M(Name).t)];
        ret)

let rec annot r = query_open ~schema:schema_set annot r.node
let pred p = pred_open annot p

let annotate r =
  V.Annotate_obj.annot
    (fun m -> m#free)
    (fun m x ->
      object
        method meta = m
        method free = x
      end)
    (query_open ~schema:schema_set)
    r
