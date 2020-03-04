open Ast
open Abslayout_visitors
module A = Abslayout
module I = Abs_int

let rec pred_card_matters = function
  | Count | Sum _ | Avg _ -> true
  | p -> Reduce.pred false ( || ) (fun _ -> false) pred_card_matters p

(** Determines whether the cardinality of a relation changes the results of a
    selection or groupby. *)
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

(** Annotate a query with whether changing the cardinality (as long as the same
   set of values is emitted) of its child queries will change its output. For
   example, changing the cardinality of the input to dedup doesn't matter. *)
let annotate : 'a. 'a annot -> < cardinality_matters : bool ; meta : 'a > annot
    =
 fun r -> annot true r

let rec annot r = query r.node

and query = function
  | AEmpty -> I.of_int 0
  | AScalar _ -> I.of_int 1
  | Select (ps, r) -> (
      match A.select_kind ps with `Scalar -> annot r | `Agg -> I.top )
  | Dedup r
  | Filter (_, r)
  | AHashIdx { hi_values = r }
  | OrderBy { rel = r; _ }
  | As (_, r) ->
      annot r
  | Join { r1; r2 } | AList (r1, r2) | DepJoin { d_lhs = r1; d_rhs = r2; _ } ->
      I.(O.(of_int 0 || (annot r1 * annot r2)))
  | ATuple (ts, Concat) -> List.sum (module I) ~f:annot ts
  | ATuple (ts, Cross) ->
      List.map ts ~f:annot |> List.fold_left ~f:I.O.( * ) ~init:(I.of_int 1)
  | ATuple (_, Zip) -> failwith "Zip tuples not supported"
  | GroupBy _ | AOrderedIdx _ | Relation _ | Range _ -> I.top

and pred _ = failwith "Predicates have no cardinality"

let estimate r = annot r
