open Ast

module type INFIX = sig
  type t

  val select : t annot pred list -> t annot -> t annot

  val range : t annot pred -> t annot pred -> t annot

  val dep_join : t annot -> scope -> t annot -> t annot

  val dep_join' : t annot depjoin -> t annot

  val join : t annot pred -> t annot -> t annot -> t annot

  val filter : t annot pred -> t annot -> t annot

  val group_by : t annot pred list -> Name.t list -> t annot -> t annot

  val dedup : t annot -> t annot

  val order_by : (t annot pred * order) list -> t annot -> t annot

  val relation : Relation.t -> t annot

  val empty : t annot

  val scalar : t annot pred -> t annot

  val as_ : scope -> t annot -> t annot

  val list : t annot -> scope -> t annot -> t annot

  val list' : t annot * t annot -> t annot

  val tuple : t annot list -> tuple -> t annot

  val hash_idx :
    ?key_layout:t annot ->
    t annot ->
    scope ->
    t annot ->
    t annot pred list ->
    t annot

  val hash_idx' : (t annot pred, t annot) hash_idx -> t annot

  val ordered_idx :
    t annot ->
    scope ->
    t annot ->
    (t annot pred, t annot) ordered_idx ->
    t annot
end

let constructors (type t) (default : unit -> t) =
  ( module struct
    type nonrec t = t

    let scope_exn r =
      match r.node with As (n, _) -> n | _ -> failwith "Expected a scope"

    let wrap x = { node = x; meta = default () }

    let select a b = wrap @@ Select (a, b)

    let range a b = wrap @@ Range (a, b)

    let dep_join a b c = wrap @@ DepJoin { d_lhs = a; d_alias = b; d_rhs = c }

    let dep_join' d = dep_join d.d_lhs d.d_alias d.d_rhs

    let join a b c = wrap @@ Join { pred = a; r1 = b; r2 = c }

    let filter a b =
      wrap
      @@
      match Pred.kind a with
      | `Scalar -> Filter (a, b)
      | `Agg | `Window ->
          Error.of_string "Aggregates not allowed in filter." |> Error.raise

    let group_by a b c = wrap @@ GroupBy (a, b, c)

    let dedup a = wrap @@ Dedup a

    let order_by a b = wrap @@ OrderBy { key = a; rel = b }

    let relation r = wrap @@ Relation r

    let empty = wrap @@ AEmpty

    let scalar a = wrap @@ AScalar a

    let as_ a b = wrap @@ As (a, b)

    let list a b c = wrap @@ AList (as_ b a, c)

    let list' (a, b) = list a (scope_exn a) b

    let tuple a b = wrap @@ ATuple (a, b)

    let hash_idx ?key_layout a b c d =
      wrap
      @@ AHashIdx
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

    let ordered_idx a b c d = wrap @@ AOrderedIdx (as_ b a, c, d)
  end : INFIX
    with type t = t )
