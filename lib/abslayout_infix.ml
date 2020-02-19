open Ast
open Abslayout_visitors

module type INFIX = sig
  type t

  val select : 'a annot pred list -> 'b annot -> t annot

  val range : 'a annot pred -> 'b annot pred -> t annot

  val dep_join : 'a annot -> scope -> 'b annot -> t annot

  val dep_join' : 'a annot depjoin -> t annot

  val join : 'a annot pred -> 'b annot -> 'a annot -> t annot

  val filter : 'a annot pred -> 'b annot -> t annot

  val group_by : 'a annot pred list -> Name.t list -> 'b annot -> t annot

  val dedup : 'a annot -> t annot

  val order_by : ('a annot pred * order) list -> 'b annot -> t annot

  val relation : Relation.t -> t annot

  val empty : t annot

  val scalar : 'a annot pred -> t annot

  val as_ : scope -> 'a annot -> t annot

  val list : 'a annot -> scope -> 'b annot -> t annot

  val list' : 'a annot * 'b annot -> t annot

  val tuple : 'a annot list -> tuple -> t annot

  val hash_idx :
    ?key_layout:'a annot ->
    'b annot ->
    scope ->
    'c annot ->
    'd annot pred list ->
    t annot

  val hash_idx' : ('a annot pred, 'a annot) hash_idx -> t annot

  val ordered_idx :
    'a annot ->
    scope ->
    'b annot ->
    ('c annot pred, 'c annot) ordered_idx ->
    t annot
end

let constructors (type t) (default : unit -> t) =
  ( module struct
    type nonrec t = t

    let scope_exn r =
      match r.node with As (n, _) -> n | _ -> failwith "Expected a scope"

    let rec strip { node; meta } =
      { node = strip_query node; meta = default () }

    and strip_query q = map_query strip strip_pred q

    and strip_pred p = map_pred strip strip_pred p

    let strips = List.map ~f:strip

    let strip_preds = List.map ~f:strip_pred

    let strip_ordered_idx o = map_ordered_idx strip strip_pred o

    let strip_order = List.map ~f:(fun (p, o) -> (strip_pred p, o))

    let wrap q = { node = strip_query q; meta = default () }

    let select a b = wrap @@ Select (strip_preds a, strip b)

    let range a b = wrap @@ Range (strip_pred a, strip_pred b)

    let dep_join a b c =
      wrap @@ DepJoin { d_lhs = strip a; d_alias = b; d_rhs = strip c }

    let dep_join' d = dep_join d.d_lhs d.d_alias d.d_rhs

    let join a b c =
      wrap @@ Join { pred = strip_pred a; r1 = strip b; r2 = strip c }

    let filter a b =
      wrap
      @@
      match Pred.kind a with
      | `Scalar -> Filter (strip_pred a, strip b)
      | `Agg | `Window ->
          Error.of_string "Aggregates not allowed in filter." |> Error.raise

    let group_by a b c = wrap @@ GroupBy (strip_preds a, b, strip c)

    let dedup a = wrap @@ Dedup (strip a)

    let order_by a b = wrap @@ OrderBy { key = strip_order a; rel = strip b }

    let relation r = wrap @@ Relation r

    let empty = wrap @@ AEmpty

    let scalar a = wrap @@ AScalar (strip_pred a)

    let as_ a b = wrap @@ As (a, strip b)

    let list a b c = wrap @@ AList (as_ b a, strip c)

    let list' (a, b) = list a (scope_exn a) b

    let tuple a b = wrap @@ ATuple (strips a, b)

    let hash_idx ?key_layout a b c d =
      wrap
      @@ AHashIdx
           {
             hi_keys = strip a;
             hi_values = strip c;
             hi_scope = b;
             hi_lookup = strip_preds d;
             hi_key_layout = Option.map ~f:strip key_layout;
           }

    let hash_idx' h =
      hash_idx ?key_layout:h.hi_key_layout h.hi_keys h.hi_scope h.hi_values
        h.hi_lookup

    let ordered_idx a b c d =
      wrap @@ AOrderedIdx (as_ b a, strip c, strip_ordered_idx d)
  end : INFIX
    with type t = t )
