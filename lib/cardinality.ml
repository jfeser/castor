open Core
open Ast
module V = Visitors
module A = Abslayout
module I = Abs_int

module Bool_exp = struct
  type 'a t = { v : bool; why : 'a }

  let ( || ) ({ v = v1; why = w1 } as x1) ({ v = v2; why = w2 } as x2) =
    if v1 then x1 else if v2 then x2 else { v = false; why = `And (w1, w2) }

  let ( && ) ({ v = v1; why = w1 } as x1) ({ v = v2; why = w2 } as x2) =
    if not v1 then x1
    else if not v2 then x2
    else { v = true; why = `And (w1, w2) }

  let t x = { v = true; why = x }
  let f x = { v = false; why = x }
end

let rec pred_card_matters =
  let open Bool_exp in
  function
  | `Count | `Sum _ | `Avg _ -> t `Counting_agg
  | p ->
      V.Reduce.pred (f `No_counting_agg) ( || )
        (fun _ -> f `Meta)
        pred_card_matters p

(** Determines whether the cardinality of a relation changes the results of a
    selection or groupby. *)
let select_card_matters ps =
  let open Bool_exp in
  Select_list.to_list ps
  |> List.map ~f:(fun (p, _) -> pred_card_matters p)
  |> List.fold_left ~init:(f `Empty_select) ~f:( || )

let rec explanation = function
  | `Output -> "produces output"
  | `In_index_keys -> "part of an indexes key relation"
  | `Empty_select -> "selected with an empty list"
  | `Meta -> ""
  | `In_dedup -> "deduplicated"
  | `No_counting_agg -> "aggregated"
  | `Counting_agg -> "counted"
  | `In_pred -> "part of a predicate"
  | `And (e, e') -> explanation e ^ " and " ^ explanation e'

let rec annot c r =
  let node = query c r.node in
  let meta =
    object
      method cardinality_matters = c.Bool_exp.v
      method why_card_matters = explanation c.why
      method meta = r.meta
    end
  in
  { node; meta }

and select_list ps = Select_list.map ~f:(fun p _ -> pred p) ps

and query c =
  let open Bool_exp in
  function
  | Dedup r -> Dedup (annot (f `In_dedup) r)
  | GroupBy (ps, ns, r) ->
      GroupBy (select_list ps, ns, annot (select_card_matters ps) r)
  | Select (ps, r) -> (
      match A.select_kind ps with
      | `Scalar -> Select (select_list ps, annot c r)
      | `Agg -> Select (select_list ps, annot (select_card_matters ps) r))
  | AHashIdx { hi_keys; hi_values; hi_key_layout; hi_lookup } ->
      AHashIdx
        {
          hi_keys = annot (f `In_index_keys) hi_keys;
          hi_values = annot c hi_values;
          hi_key_layout = Option.map hi_key_layout ~f:(annot (f `In_index_keys));
          hi_lookup = List.map hi_lookup ~f:pred;
        }
  | AOrderedIdx { oi_keys; oi_values; oi_key_layout; oi_lookup } ->
      AOrderedIdx
        {
          oi_keys = annot (f `In_index_keys) oi_keys;
          oi_values = annot c oi_values;
          oi_key_layout = Option.map oi_key_layout ~f:(annot (f `In_index_keys));
          oi_lookup =
            List.map oi_lookup ~f:(fun (b, b') ->
                ( Option.map ~f:(V.Map.bound pred) b,
                  Option.map ~f:(V.Map.bound pred) b' ));
        }
  | q -> V.Map.query (annot c) pred q

and pred p = V.Map.pred (annot (Bool_exp.f `In_pred)) pred p

(** Annotate a query with whether changing the cardinality (as long as the same
   set of values is emitted) of its child queries will change its output. For
   example, changing the cardinality of the input to dedup doesn't matter. *)
let annotate ?(dedup = false) r =
  (* If the query is deduplicated, then its cardinality doesn't matter. *)
  let init = (if dedup then Bool_exp.f else Bool_exp.t) `Output in
  annot init r

let extend ?dedup r =
  annotate ?dedup r
  |> V.map_meta (fun m ->
         object
           method cardinality_matters =
             m#cardinality_matters && m#meta#cardinality_matters

           method why_card_matters = m#why_card_matters
         end)

let rec annot r = query r.node

and query = function
  | AEmpty -> I.of_int 0
  | AScalar _ -> I.of_int 1
  | Select (ps, r) -> (
      match A.select_kind ps with `Scalar -> annot r | `Agg -> I.top)
  | Dedup r
  | Filter (_, r)
  | AHashIdx { hi_values = r; _ }
  | OrderBy { rel = r; _ } ->
      annot r
  | Join { r1; r2; _ }
  | AList { l_keys = r1; l_values = r2; _ }
  | DepJoin { d_lhs = r1; d_rhs = r2; _ } ->
      I.(O.(of_int 0 || (annot r1 * annot r2)))
  | ATuple (ts, Concat) -> List.sum (module I) ~f:annot ts
  | ATuple (ts, Cross) ->
      List.map ts ~f:annot |> List.fold_left ~f:I.O.( * ) ~init:(I.of_int 1)
  | ATuple (_, Zip) -> failwith "Zip tuples not supported"
  | GroupBy _ | AOrderedIdx _ | Relation _ | Range _ -> I.top
  | _ -> failwith "unsupported"

and pred _ = failwith "Predicates have no cardinality"

let estimate r = annot r
