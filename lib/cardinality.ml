open Ast
open Abslayout_visitors
module A = Abslayout

(** Determines whether the cardinality of a relation changes the results of a
   selection or groupby. *)
let rec pred_card_matters = function
  | Count | Sum _ | Avg _ -> true
  | p -> Reduce.pred false ( || ) (fun _ -> false) pred_card_matters p

let select_card_matters ps = List.exists ps ~f:pred_card_matters

let rec annot c r =
  let node = query c r.node in
  let meta =
    object
      method cardinality_matters = c

      method meta = r.meta
    end
  in
  { node; meta }

and query c = function
  | Dedup r -> Dedup (annot false r)
  | GroupBy (ps, ns, r) ->
      GroupBy (List.map ~f:pred ps, ns, annot (select_card_matters ps) r)
  | Select (ps, r) -> (
      match A.select_kind ps with
      | `Scalar -> Select (List.map ~f:pred ps, annot c r)
      | `Agg -> Select (List.map ~f:pred ps, annot (select_card_matters ps) r) )
  | AHashIdx ({ hi_keys; hi_values; hi_key_layout; hi_lookup } as h) ->
      AHashIdx
        {
          h with
          hi_keys = annot false hi_keys;
          hi_values = annot c hi_values;
          hi_key_layout = Option.map hi_key_layout ~f:(annot false);
          hi_lookup = List.map hi_lookup ~f:pred;
        }
  | AOrderedIdx (rk, rv, o) ->
      AOrderedIdx
        (annot false rk, annot c rv, map_ordered_idx (annot false) pred o)
  | q -> map_query (annot c) pred q

and pred p = map_pred (annot false) pred p

let annotate : 'a. 'a annot -> < cardinality_matters : bool ; meta : 'a > annot
    =
 fun r -> annot true r
