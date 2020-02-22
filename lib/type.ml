open Ast
open Abslayout_fold
open Abslayout_visitors
open Collections

exception TypeError of Error.t [@@deriving sexp]

module AbsInt = struct
  type t = Bottom | Interval of int * int | Top [@@deriving compare, sexp]

  let pp fmt = function
    | Top -> Format.fprintf fmt "⊤"
    | Bottom -> Format.fprintf fmt "⊥"
    | Interval (l, h) -> Format.fprintf fmt "[%d, %d]" l h

  let top = Top

  let bot = Bottom

  let zero = Interval (0, 0)

  let inf = function
    | Top -> Ok Int.min_value
    | Bottom -> Error (Error.of_string "Bottom has no infimum.")
    | Interval (x, _) -> Ok x

  let sup = function
    | Top -> Ok Int.max_value
    | Bottom -> Error (Error.of_string "Bottom has no supremum.")
    | Interval (_, x) -> Ok x

  let lift1 f i =
    match i with Bottom -> Bottom | Top -> Top | Interval (l, h) -> f l h

  let lift2 f i1 i2 =
    match (i1, i2) with
    | Bottom, _ | _, Bottom -> Bottom
    | Top, _ | _, Top -> Top
    | Interval (l1, h1), Interval (l2, h2) -> f l1 h1 l2 h2

  let ceil_pow2 = lift1 (fun l h -> Interval (Int.ceil_pow2 l, Int.ceil_pow2 h))

  let meet i1 i2 =
    match (i1, i2) with
    | Bottom, _ | _, Bottom -> Bottom
    | Top, x | x, Top -> x
    | Interval (l1, h1), Interval (l2, h2) ->
        if h1 < l2 || h2 < l1 then Bottom
        else Interval (Int.max l1 l2, Int.min h1 h2)

  let join i1 i2 =
    match (i1, i2) with
    | Bottom, x | x, Bottom -> x
    | Top, _ | _, Top -> Top
    | Interval (l1, h1), Interval (l2, h2) ->
        Interval (Int.min l1 l2, Int.max h1 h2)

  let of_int x = Interval (x, x)

  let to_int = function
    | Interval (l, h) -> if l = h then Some l else None
    | _ -> None

  let byte_width ~nullable = function
    | Bottom -> 1
    | Top -> 8
    | Interval (l, h) ->
        let open Int in
        let maxval = Int.max (Int.abs l) (Int.abs h) in
        let maxval = if nullable then maxval + 1 else maxval in
        if maxval = 0 then 1
        else
          let bit_width = Float.((log (of_int maxval) /. log 2.0) + 1.0) in
          bit_width /. 8.0 |> Float.iround_exn ~dir:`Up

  module O = struct
    let ( + ) = lift2 (fun l1 h1 l2 h2 -> Interval (l1 + l2, h1 + h2))

    let ( - ) = lift2 (fun l1 h1 l2 h2 -> Interval (l1 - h2, l2 - h1))

    let ( * ) =
      lift2 (fun l1 h1 l2 h2 ->
          let min_many = List.reduce_exn ~f:Int.min in
          let max_many = List.reduce_exn ~f:Int.max in
          let xs = [ l1 * l2; l1 * h2; l2 * h1; h2 * h1 ] in
          Interval (min_many xs, max_many xs))

    let ( && ) = meet

    let ( || ) = join
  end

  include O
end

module Test_absint_summable : Container.Summable with type t = AbsInt.t = AbsInt

module AbsFixed = struct
  module I = AbsInt

  type t = { range : I.t; scale : int } [@@deriving compare, sexp]

  let of_fixed f = { range = I.of_int f.Fixed_point.value; scale = f.scale }

  let zero = of_fixed (Fixed_point.of_int 0)

  let bot = { range = I.bot; scale = 1 }

  let top = { range = I.top; scale = 1 }

  let rec unify dir f1 f2 =
    if f1.scale = f2.scale then { f1 with range = dir f1.range f2.range }
    else if f1.scale > f2.scale then unify dir f2 f1
    else
      let scale_factor = f2.scale / f1.scale in
      let scaled_range = I.O.(f1.range * I.of_int scale_factor) in
      { f2 with range = dir f2.range scaled_range }

  let meet = unify I.meet

  let join = unify I.join
end

module Distinct : sig
  type ('a, 'b) t

  val empty : ('a, 'b) Map.comparator -> ('a, 'b) t

  val singleton : ('a, 'b) Map.comparator -> 'a -> int -> ('a, 'b) t

  val join : ('a, 'b) t -> ('a, 'b) t -> ('a, 'b) t
end = struct
  type ('a, 'b) t = unit

  let singleton _ _ _ = ()

  let empty _m = ()

  let join _m1 _m2 = ()
end

type int_ = {
  range : AbsInt.t;
  distinct : ((int, Int.comparator_witness) Distinct.t[@sexp.opaque]);
      [@compare.ignore]
  nullable : bool; [@sexp.bool]
}
[@@deriving compare, sexp]

type date = int_ [@@deriving compare, sexp]

type bool_ = { nullable : bool [@sexp.bool] } [@@deriving compare, sexp]

type string_ = {
  nchars : AbsInt.t;
  distinct : ((string, String.comparator_witness) Distinct.t[@sexp.opaque]);
      [@compare.ignore]
  nullable : bool; [@sexp.bool]
}
[@@deriving compare, sexp]

type list_ = { count : AbsInt.t } [@@deriving compare, sexp]

type tuple = { kind : [ `Cross | `Concat ] } [@@deriving compare, sexp]

type hash_idx = { key_count : AbsInt.t } [@@deriving compare, sexp]

type ordered_idx = { key_count : AbsInt.t } [@@deriving compare, sexp]

type fixed = { value : AbsFixed.t; nullable : bool [@sexp.bool] }
[@@deriving compare, sexp]

type t =
  | NullT
  | IntT of int_
  | DateT of date
  | FixedT of fixed
  | BoolT of bool_
  | StringT of string_
  | TupleT of (t list * tuple)
  | ListT of (t * list_)
  | HashIdxT of (t * t * hash_idx)
  | OrderedIdxT of (t * t * ordered_idx)
  | FuncT of (t list * [ `Child_sum | `Width of int ])
  | EmptyT
[@@deriving compare, sexp]

let least_general_of_primtype = function
  | Prim_type.IntT { nullable } ->
      IntT
        { range = AbsInt.bot; nullable; distinct = Distinct.empty (module Int) }
  | NullT -> NullT
  | DateT { nullable } ->
      DateT
        { range = AbsInt.bot; nullable; distinct = Distinct.empty (module Int) }
  | FixedT { nullable } -> FixedT { value = AbsFixed.bot; nullable }
  | StringT { nullable; _ } ->
      StringT
        {
          nchars = AbsInt.bot;
          nullable;
          distinct = Distinct.empty (module String);
        }
  | BoolT { nullable } -> BoolT { nullable }
  | TupleT _ | VoidT -> failwith "Not a layout type."

let rec unify_exn t1 t2 =
  let module I = AbsInt in
  let fail m =
    let err =
      Error.create
        (Printf.sprintf "Unification failed: %s" m)
        (t1, t2) [%sexp_of: t * t]
    in
    raise (TypeError err)
  in
  match (t1, t2) with
  | NullT, NullT -> NullT
  | ( IntT { range = b1; nullable = n1; distinct = d1 },
      IntT { range = b2; nullable = n2; distinct = d2 } ) ->
      IntT
        {
          range = I.O.(b1 || b2);
          nullable = n1 || n2;
          distinct = Distinct.join d1 d2;
        }
  | ( DateT { range = b1; nullable = n1; distinct = d1 },
      DateT { range = b2; nullable = n2; distinct = d2 } ) ->
      DateT
        {
          range = I.O.(b1 || b2);
          nullable = n1 || n2;
          distinct = Distinct.join d1 d2;
        }
  | FixedT { value = v1; nullable = n1 }, FixedT { value = v2; nullable = n2 }
    ->
      FixedT { value = AbsFixed.join v1 v2; nullable = n1 || n2 }
  | IntT x, NullT | NullT, IntT x -> IntT { x with nullable = true }
  | DateT x, NullT | NullT, DateT x -> DateT { x with nullable = true }
  | FixedT x, NullT | NullT, FixedT x -> FixedT { x with nullable = true }
  | BoolT { nullable = n1 }, BoolT { nullable = n2 } ->
      BoolT { nullable = n1 || n2 }
  | BoolT _, NullT | NullT, BoolT _ -> BoolT { nullable = true }
  | ( StringT { nchars = b1; nullable = n1; distinct = d1 },
      StringT { nchars = b2; nullable = n2; distinct = d2 } ) ->
      StringT
        {
          nchars = I.O.(b1 || b2);
          nullable = n1 || n2;
          distinct = Distinct.join d1 d2;
        }
  | StringT x, NullT | NullT, StringT x -> StringT { x with nullable = true }
  | TupleT (e1s, { kind = k1 }), TupleT (e2s, { kind = k2 }) when Poly.(k1 = k2)
    ->
      let elem_ts =
        match List.map2 e1s e2s ~f:unify_exn with
        | Ok ts -> ts
        | Unequal_lengths -> fail "Different number of columns."
      in
      TupleT (elem_ts, { kind = k1 })
  | ListT (et1, { count = c1 }), ListT (et2, { count = c2 }) ->
      ListT (unify_exn et1 et2, { count = I.O.(c1 || c2) })
  | ( OrderedIdxT (k1, v1, { key_count = c1 }),
      OrderedIdxT (k2, v2, { key_count = c2 }) ) ->
      OrderedIdxT
        (unify_exn k1 k2, unify_exn v1 v2, { key_count = I.O.(c1 || c2) })
  | ( HashIdxT (kt1, vt1, { key_count = kc1 }),
      HashIdxT (kt2, vt2, { key_count = kc2 }) ) ->
      let kt = unify_exn kt1 kt2 in
      let vt = unify_exn vt1 vt2 in
      HashIdxT (kt, vt, { key_count = I.O.(kc1 || kc2) })
  | EmptyT, t | t, EmptyT -> t
  | FuncT (t, `Child_sum), FuncT (t', `Child_sum) ->
      FuncT (List.map2_exn ~f:unify_exn t t', `Child_sum)
  | FuncT (t, `Width w), FuncT (t', `Width w') when Int.(w = w') ->
      FuncT (List.map2_exn ~f:unify_exn t t', `Width w)
  | NullT, _
  | IntT _, _
  | DateT _, _
  | FixedT _, _
  | BoolT _, _
  | StringT _, _
  | TupleT _, _
  | ListT _, _
  | HashIdxT _, _
  | OrderedIdxT _, _
  | FuncT _, _ ->
      fail "Unexpected types."

let rec width = function
  | NullT | IntT _ | BoolT _ | StringT _ | FixedT _ | DateT _ -> 1
  | TupleT (ts, _) ->
      List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | ListT (t, _) -> width t
  | HashIdxT (kt, vt, _) | OrderedIdxT (kt, vt, _) -> width kt + width vt
  | EmptyT -> 0
  | FuncT (ts, `Child_sum) ->
      List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | FuncT (_, `Width w) -> w

let rec count = function
  | EmptyT -> AbsInt.of_int 0
  | NullT | IntT _ | BoolT _ | StringT _ | FixedT _ | DateT _ -> AbsInt.of_int 1
  | TupleT (ts, { kind = `Concat }) -> List.sum (module AbsInt) ts ~f:count
  | TupleT (ts, { kind = `Cross }) ->
      List.map ts ~f:count
      |> List.fold_left ~init:(AbsInt.of_int 1) ~f:AbsInt.( * )
  | OrderedIdxT (_, vt, { key_count }) ->
      AbsInt.(count vt || (key_count * count vt))
  | HashIdxT (_, vt, _) -> count vt
  | ListT (_, { count }) -> count
  | FuncT _ -> AbsInt.top

let hash_kind_of_key_type c = function
  | IntT { range = r; _ } | DateT { range = r; _ } -> (
      match (c, r) with
      | AbsInt.Interval (_, h_count), Interval (l_range, h_range) ->
          if h_count / (h_range - l_range) < 5 then `Direct else `Cmph
      | _ -> `Cmph )
  | _ -> `Cmph

let hash_kind_exn = function
  | HashIdxT (kt, _, m) -> hash_kind_of_key_type m.key_count kt
  | _ -> failwith "Unexpected type."

let range_exn = function
  | IntT x -> x.range
  | DateT x -> x.range
  | _ -> failwith "Has no range."

let header_len field_len =
  let module I = AbsInt in
  match I.to_int field_len with
  | Some _ -> I.of_int 0
  | None -> I.byte_width ~nullable:false field_len |> I.of_int

let rec len =
  let module I = AbsInt in
  let open AbsInt.O in
  function
  | EmptyT -> I.zero
  | IntT x -> I.byte_width ~nullable:x.nullable x.range |> I.of_int
  | DateT x -> I.byte_width ~nullable:x.nullable x.range |> I.of_int
  | FixedT x -> I.byte_width ~nullable:x.nullable x.value.range |> I.of_int
  | BoolT _ -> I.of_int 1
  | StringT x -> header_len x.nchars + x.nchars
  | TupleT (ts, _) ->
      let body_len = List.sum (module AbsInt) ts ~f:len in
      body_len + header_len body_len
  | ListT (t, x) ->
      let count_len = header_len x.count in
      let body_len = x.count * len t in
      let len_len = header_len body_len in
      count_len + len_len + body_len
  | OrderedIdxT (kt, vt, m) ->
      let values = m.key_count * len vt in
      oi_map_len kt vt m + values
  | HashIdxT (kt, vt, m) ->
      hi_hash_len kt m + hi_map_len kt vt m + (m.key_count * len vt)
  | FuncT (ts, _) -> List.sum (module AbsInt) ts ~f:len
  | NullT as t -> Error.create "Unexpected type." t [%sexp_of: t] |> Error.raise

and oi_map_len kt vt m =
  AbsInt.(m.key_count * (len kt + of_int (oi_ptr_size vt m)))

and oi_ptr_size vt m =
  AbsInt.(byte_width ~nullable:false (m.key_count * len vt))

and hi_hash_len ?(bytes_per_key = AbsInt.of_int 1) kt m =
  let module I = AbsInt in
  let open AbsInt.O in
  match hash_kind_of_key_type m.key_count kt with
  | `Universal -> I.of_int Int.(8 * 3)
  | `Direct -> I.of_int 0
  | `Cmph ->
      (* The interval represents uncertainty about the hash size, and CMPH hashes
       seem to have some fixed overhead ~100B? *)
      (m.key_count * bytes_per_key * Interval (1, 2)) + I.of_int 128

and hi_map_len kt vt m =
  let module I = AbsInt in
  let open AbsInt.O in
  match hash_kind_of_key_type m.key_count kt with
  | `Universal -> I.ceil_pow2 m.key_count
  | `Direct -> range_exn kt * I.of_int (hi_ptr_size kt vt m)
  | `Cmph -> m.key_count * Interval (1, 2) * I.of_int (hi_ptr_size kt vt m)

and hi_ptr_size kt vt m =
  AbsInt.(byte_width ~nullable:false (m.key_count * (len kt + len vt)))

(** Returns the least general type of a layout. *)
let least_general_of_layout r =
  let rec f r =
    match r.node with
    | Range _ -> FuncT ([], `Width 1)
    | Select (ps, r') | GroupBy (ps, _, r') ->
        FuncT ([ f r' ], `Width (List.length ps))
    | OrderBy { rel = r'; _ } | Filter (_, r') | Dedup r' ->
        FuncT ([ f r' ], `Child_sum)
    | Join { r1; r2; _ } -> FuncT ([ f r1; f r2 ], `Child_sum)
    | AEmpty -> EmptyT
    | AScalar p -> Pred.to_type p |> least_general_of_primtype
    | AList (_, r') -> ListT (f r', { count = Bottom })
    | DepJoin { d_lhs; d_rhs; _ } -> FuncT ([ f d_lhs; f d_rhs ], `Child_sum)
    | AHashIdx { hi_key_layout = Some kr; hi_values = vr; _ } ->
        HashIdxT (f kr, f vr, { key_count = Bottom })
    | AOrderedIdx (_, vr, { oi_key_layout = Some kr; _ }) ->
        OrderedIdxT (f kr, f vr, { key_count = Bottom })
    | ATuple (rs, k) ->
        let kind =
          match k with
          | Cross -> `Cross
          | Concat -> `Concat
          | _ -> failwith "Unsupported"
        in
        TupleT (List.map rs ~f, { kind })
    | As (_, r') -> f r'
    | AOrderedIdx (_, _, { oi_key_layout = None; _ })
    | AHashIdx { hi_key_layout = None; _ } ->
        failwith "Missing key layout."
    | Relation _ -> failwith "Layout is still abstract."
  in
  f r

(* let contexts r =
 *   let split_scope r =
 *     match r.node with As (s, r') -> (r', s) | _ -> failwith "Expected as"
 *   in
 *   let plus = ( @ ) in
 *   let zero = [] in
 *   let empty_context q = [ ([], q) ] in
 *   let add_context ctx = List.map ~f:(fun (ctxs, q) -> (ctx :: ctxs, q)) in
 *   let rec annot r = Reduce.annot zero plus query meta r
 *   and query q =
 *     match q with
 *     | AOrderedIdx (qk, qv, _) | AList (qk, qv) ->
 *         plus (empty_context q) @@ add_context (split_scope qk) (annot qv)
 *     | AHashIdx h ->
 *         plus (empty_context q)
 *         @@ add_context (h.hi_keys, h.hi_scope) (annot h.hi_values)
 *     | _ -> Reduce.query (empty_context q) plus query pred q
 *   and pred _ = zero
 *   and meta _ = zero in
 *   annot r *)

(** Returns a layout type that is general enough to hold all of the data. *)
class ['self] type_fold =
  object (_ : 'self)
    inherit [_] abslayout_fold

    method! select _ (exprs, _) t = FuncT ([ t ], `Width (List.length exprs))

    method join _ _ t1 t2 = FuncT ([ t1; t2 ], `Child_sum)

    method depjoin _ _ t1 t2 = FuncT ([ t1; t2 ], `Child_sum)

    method! filter _ _ t = FuncT ([ t ], `Child_sum)

    method! order_by _ _ t = FuncT ([ t ], `Child_sum)

    method! dedup _ t = FuncT ([ t ], `Child_sum)

    method! group_by _ (exprs, _, _) t =
      FuncT ([ t ], `Width (List.length exprs))

    method empty _ = EmptyT

    method scalar _ _ =
      function
      | Value.Date x ->
          let x = Date.to_int x in
          DateT
            {
              range = AbsInt.of_int x;
              nullable = false;
              distinct = Distinct.singleton (module Int) x 1;
            }
      | Int x ->
          IntT
            {
              range = AbsInt.of_int x;
              nullable = false;
              distinct = Distinct.singleton (module Int) x 1;
            }
      | Bool _ -> BoolT { nullable = false }
      | String x ->
          StringT
            {
              nchars = AbsInt.of_int (String.length x);
              nullable = false;
              distinct = Distinct.singleton (module String) x 1;
            }
      | Null -> NullT
      | Fixed x -> FixedT { value = AbsFixed.of_fixed x; nullable = false }

    method list _ (_, elem_l) =
      let init = (least_general_of_layout elem_l, 0) in
      let fold (t, c) (_, t') = (unify_exn t t', c + 1) in
      let extract (elem_type, num_elems) =
        ListT (elem_type, { count = AbsInt.of_int num_elems })
      in
      Fold { init; fold; extract }

    method tuple _ (_, kind) =
      let kind =
        match kind with
        | Cross -> `Cross
        | Zip -> failwith ""
        | Concat -> `Concat
      in
      let init = RevList.empty in
      let fold = RevList.( ++ ) in
      let extract ts = TupleT (RevList.to_list ts, { kind }) in
      Fold { init; fold; extract }

    method hash_idx _ h =
      let init =
        ( 0,
          least_general_of_layout (Option.value_exn h.hi_key_layout),
          least_general_of_layout h.hi_values )
      in
      let fold (kct, kt, vt) (_, kt', vt') =
        (kct + 1, unify_exn kt kt', unify_exn vt vt')
      in
      let extract (kct, kt, vt) =
        HashIdxT (kt, vt, { key_count = AbsInt.of_int kct })
      in
      Fold { init; fold; extract }

    method ordered_idx _ (_, value_l, { oi_key_layout; _ }) =
      let key_l = Option.value_exn oi_key_layout in
      let init =
        (least_general_of_layout key_l, least_general_of_layout value_l, 0)
      in
      let fold (kt, vt, ct) (_, kt', vt') =
        (unify_exn kt kt', unify_exn vt vt', ct + 1)
      in
      let extract (kt, vt, ct) =
        OrderedIdxT (kt, vt, { key_count = AbsInt.of_int ct })
      in
      Fold { init; fold; extract }
  end

let type_of ?timeout conn r =
  Log.info (fun m -> m "Computing type of abstract layout.");
  let type_ = (new type_fold)#run ?timeout conn r in
  Log.info (fun m ->
      m "The type is: %s" (Sexp.to_string_hum ([%sexp_of: t] type_)));
  type_

let annotate conn r =
  let type_ = Univ_map.Key.create ~name:"type" [%sexp_of: t] in
  let rec annot r t =
    Meta.(set_m r type_ t);
    match (r.node, t) with
    | ( (AScalar _ | Range _),
        (IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _ | NullT) ) ->
        ()
    | AList (_, r'), ListT (t', _)
    | (Filter (_, r') | Select (_, r')), FuncT ([ t' ], _) ->
        annot r' t'
    | AHashIdx h, HashIdxT (kt, vt, _) ->
        Option.iter h.hi_key_layout ~f:(fun kr -> annot kr kt);
        annot h.hi_values vt
    | AOrderedIdx (_, vr, m), OrderedIdxT (kt, vt, _) ->
        Option.iter m.oi_key_layout ~f:(fun kr -> annot kr kt);
        annot vr vt
    | ATuple (rs, _), TupleT (ts, _) -> (
        match List.iter2 rs ts ~f:annot with
        | Ok () -> ()
        | Unequal_lengths ->
            Error.create "Mismatched tuple type." (r, t)
              [%sexp_of: _ Ast.annot * t]
            |> Error.raise )
    | DepJoin { d_lhs; d_rhs; _ }, FuncT ([ t1; t2 ], _) ->
        annot d_lhs t1;
        annot d_rhs t2
    | As (_, r), _ -> annot r t
    | ( ( Select _ | Filter _ | DepJoin _ | Join _ | GroupBy _ | OrderBy _
        | Dedup _ | Relation _ | AEmpty | AScalar _ | AList _ | ATuple _
        | AHashIdx _ | AOrderedIdx _ | Range _ ),
        ( NullT | IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _ | TupleT _
        | ListT _ | HashIdxT _ | OrderedIdxT _ | FuncT _ | EmptyT ) ) ->
        Error.create "Unexpected type." (r, t) [%sexp_of: _ annot * t]
        |> Error.raise
  in
  let r = map_meta (fun _ -> Meta.empty ()) r in
  annot r (type_of conn r);
  let visitor =
    object
      inherit Abslayout_visitors.runtime_subquery_visitor

      method visit_Subquery r = annot r (type_of conn r)
    end
  in
  visitor#visit_t () r;
  map_meta
    (fun m ->
      object
        method type_ = Univ_map.find_exn !m type_
      end)
    r
