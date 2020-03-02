open Ast
open Abslayout_visitors

module Query = struct
  (** Check that the names in a select list are unique. *)
  let assert_unique_names ps =
    List.filter_map ps ~f:Pred.to_name
    |> List.find_a_dup ~compare:[%compare: Name.t]
    |> Option.iter ~f:(fun dup ->
           Error.create "Select list contains duplicate names" dup
             [%sexp_of: Name.t]
           |> Error.raise)

  let select a b =
    assert_unique_names a;
    Select (a, b)

  let range a b = Range (a, b)

  let dep_join a b c = DepJoin { d_lhs = a; d_alias = b; d_rhs = c }

  let dep_join' d = dep_join d.d_lhs d.d_alias d.d_rhs

  let join a b c = Join { pred = a; r1 = b; r2 = c }

  let filter a b =
    ( match Pred.kind a with
    | `Scalar -> ()
    | `Agg | `Window -> failwith "Aggregates not allowed in filter." );
    Filter (a, b)

  let group_by a b c =
    assert_unique_names a;
    GroupBy (a, b, c)

  let dedup a = Dedup a

  let order_by a b = OrderBy { key = a; rel = b }

  let relation r = Relation r

  let empty = AEmpty

  let scalar a = AScalar a

  let as_ a b = As (a, b)

  let tuple a b = ATuple (a, b)

  let hash_idx ?key_layout a b c d =
    AHashIdx
      {
        hi_keys = a;
        hi_values = c;
        hi_scope = b;
        hi_lookup = d;
        hi_key_layout = key_layout;
      }

  let hash_idx' h =
    hash_idx ?key_layout:h.hi_key_layout h.hi_keys h.hi_scope h.hi_values
      h.hi_lookup
end

module Annot = struct
  module type S = sig
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

    val pred : 'a annot pred -> t annot pred
  end

  let with_strip_meta (type t) (default : unit -> t) =
    ( module struct
      type nonrec t = t

      let scope_exn r =
        match r.node with As (n, _) -> n | _ -> failwith "Expected a scope"

      let rec strip { node; meta } =
        { node = strip_query node; meta = default () }

      and strip_query q = map_query strip strip_pred q

      and strip_pred p = map_pred strip strip_pred p

      let pred = strip_pred

      let strips = List.map ~f:strip

      let strip_preds = List.map ~f:strip_pred

      let strip_ordered_idx o = map_ordered_idx strip strip_pred o

      let strip_order = List.map ~f:(fun (p, o) -> (strip_pred p, o))

      let wrap q = { node = strip_query q; meta = default () }

      let select a b = wrap @@ Query.select (strip_preds a) (strip b)

      let range a b = wrap @@ Query.range (strip_pred a) (strip_pred b)

      let dep_join a b c =
        wrap
        @@ Query.dep_join' { d_lhs = strip a; d_alias = b; d_rhs = strip c }

      let dep_join' d = dep_join d.d_lhs d.d_alias d.d_rhs

      let join a b c = wrap @@ Query.join (strip_pred a) (strip b) (strip c)

      let filter a b = wrap @@ Query.filter (strip_pred a) (strip b)

      let group_by a b c = wrap @@ Query.group_by (strip_preds a) b (strip c)

      let dedup a = wrap @@ Query.dedup @@ strip a

      let order_by a b = wrap @@ Query.order_by (strip_order a) (strip b)

      let relation r = wrap @@ Query.relation r

      let empty = wrap @@ Query.empty

      let scalar a = wrap @@ Query.scalar @@ strip_pred a

      let as_ a b = wrap @@ Query.as_ a (strip b)

      let list a b c = wrap @@ AList (as_ b a, strip c)

      let list' (a, b) = list a (scope_exn a) b

      let tuple a b = wrap @@ Query.tuple (strips a) b

      let hash_idx ?key_layout a b c d =
        wrap
        @@ Query.hash_idx
             ?key_layout:(Option.map ~f:strip key_layout)
             (strip a) b (strip c) (strip_preds d)

      let hash_idx' h =
        hash_idx ?key_layout:h.hi_key_layout h.hi_keys h.hi_scope h.hi_values
          h.hi_lookup

      let ordered_idx a b c d =
        wrap @@ AOrderedIdx (as_ b a, strip c, strip_ordered_idx d)
    end : S
      with type t = t )

  include (val with_strip_meta (fun () -> ()))
end
