open Core
open Ast
module V = Visitors

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
    (match Pred.kind a with
    | `Scalar -> ()
    | `Agg | `Window -> failwith "Aggregates not allowed in filter.");
    Filter (a, b)

  let group_by a b c =
    assert_unique_names a;
    GroupBy (a, b, c)

  let dedup a = Dedup a
  let order_by a b = OrderBy { key = a; rel = b }
  let relation r = Relation r
  let empty = AEmpty
  let scalar a = AScalar a
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

  let ordered_idx ?key_layout a b c d =
    AOrderedIdx
      {
        oi_keys = a;
        oi_values = c;
        oi_scope = b;
        oi_lookup = d;
        oi_key_layout = key_layout;
      }

  let ordered_idx' h =
    ordered_idx ?key_layout:h.oi_key_layout h.oi_keys h.oi_scope h.oi_values
      h.oi_lookup

  let list a b c = AList { l_keys = a; l_scope = b; l_values = c }
end

module Annot = struct
  module type S = sig
    type 'a meta
    type t

    val select : _ meta annot pred list -> _ meta annot -> t annot
    val range : _ meta annot pred -> _ meta annot pred -> t annot
    val dep_join : _ meta annot -> scope -> _ meta annot -> t annot
    val dep_join' : (_ meta annot, scope) depjoin -> t annot
    val join : _ meta annot pred -> _ meta annot -> _ meta annot -> t annot
    val filter : _ meta annot pred -> _ meta annot -> t annot

    val group_by :
      _ meta annot pred list -> Name.t list -> _ meta annot -> t annot

    val dedup : _ meta annot -> t annot
    val order_by : (_ meta annot pred * order) list -> _ meta annot -> t annot
    val relation : Relation.t -> t annot
    val empty : t annot
    val scalar : _ meta annot pred -> t annot
    val list : _ meta annot -> scope -> _ meta annot -> t annot
    val list' : (_ meta annot pred, _ meta annot, scope) list_ -> t annot
    val tuple : _ meta annot list -> tuple -> t annot

    val hash_idx :
      ?key_layout:_ meta annot ->
      _ meta annot ->
      scope ->
      _ meta annot ->
      _ meta annot pred list ->
      t annot

    val hash_idx' : (_ meta annot pred, _ meta annot, scope) hash_idx -> t annot

    val ordered_idx :
      ?key_layout:_ meta annot ->
      _ meta annot ->
      scope ->
      _ meta annot ->
      (_ meta annot pred bound option * _ meta annot pred bound option) list ->
      t annot

    val ordered_idx' :
      (_ meta annot pred, _ meta annot, scope) ordered_idx -> t annot

    val pred : _ meta annot pred -> t annot pred
  end

  module type S_strip = S with type 'a meta := 'a

  let with_strip_meta (type t) (default : unit -> t) =
    (module struct
      type nonrec t = t

      let rec strip { node; _ } = { node = strip_query node; meta = default () }
      and strip_query q = V.Map.query strip strip_pred q
      and strip_pred p = V.Map.pred strip strip_pred p

      let pred = strip_pred
      let strips = List.map ~f:strip
      let strip_preds = List.map ~f:strip_pred

      let strip_bounds =
        List.map ~f:(fun (b, b') ->
            ( Option.map ~f:(V.Map.bound strip_pred) b,
              Option.map ~f:(V.Map.bound strip_pred) b' ))

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
      let list a b c = wrap @@ Query.list (strip a) b (strip c)
      let list' l = list l.l_keys l.l_scope l.l_values
      let tuple a b = wrap @@ Query.tuple (strips a) b

      let hash_idx ?key_layout a b c d =
        wrap
        @@ Query.hash_idx
             ?key_layout:(Option.map ~f:strip key_layout)
             (strip a) b (strip c) (strip_preds d)

      let hash_idx' h =
        hash_idx ?key_layout:h.hi_key_layout h.hi_keys h.hi_scope h.hi_values
          h.hi_lookup

      let ordered_idx ?key_layout a b c d =
        wrap
        @@ Query.ordered_idx
             ?key_layout:(Option.map ~f:strip key_layout)
             (strip a) b (strip c) (strip_bounds d)

      let ordered_idx' o =
        ordered_idx ?key_layout:o.oi_key_layout o.oi_keys o.oi_scope o.oi_values
          o.oi_lookup
    end : S_strip
      with type t = t)

  module type S_default = sig
    type t

    include S with type _ meta := t and type t := t
  end

  let with_default (type t) default =
    (module struct
      type nonrec t = t

      let pred = Fun.id
      let wrap q = { node = q; meta = default }
      let select a b = wrap @@ Query.select a b
      let range a b = wrap @@ Query.range a b

      let dep_join a b c =
        wrap @@ Query.dep_join' { d_lhs = a; d_alias = b; d_rhs = c }

      let dep_join' d = dep_join d.d_lhs d.d_alias d.d_rhs
      let join a b c = wrap @@ Query.join a b c
      let filter a b = wrap @@ Query.filter a b
      let group_by a b c = wrap @@ Query.group_by a b c
      let dedup a = wrap @@ Query.dedup a
      let order_by a b = wrap @@ Query.order_by a b
      let relation r = wrap @@ Query.relation r
      let empty = wrap @@ Query.empty
      let scalar a = wrap @@ Query.scalar a
      let list a b c = wrap @@ Query.list a b c
      let list' l = list l.l_keys l.l_scope l.l_values
      let tuple a b = wrap @@ Query.tuple a b

      let hash_idx ?key_layout a b c d =
        wrap @@ Query.hash_idx ?key_layout a b c d

      let hash_idx' h =
        hash_idx ?key_layout:h.hi_key_layout h.hi_keys h.hi_scope h.hi_values
          h.hi_lookup

      let ordered_idx ?key_layout a b c d =
        wrap @@ Query.ordered_idx ?key_layout a b c d

      let ordered_idx' o =
        ordered_idx ?key_layout:o.oi_key_layout o.oi_keys o.oi_scope o.oi_values
          o.oi_lookup
    end : S_default
      with type t = t)

  include (val with_strip_meta (fun () -> ()))
end
