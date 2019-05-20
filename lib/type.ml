open! Core
open Collections
include Type0

exception TypeError of Error.t [@@deriving sexp]

(** Range abstraction for integers. *)
module AbsInt = struct
  type t = Bottom | Interval of int * int | Top [@@deriving compare, sexp]

  let pp fmt = function
    | Top -> Format.fprintf fmt "⊤"
    | Bottom -> Format.fprintf fmt "⊥"
    | Interval (l, h) -> Format.fprintf fmt "[%d, %d]" l h

  let top = Top

  let bot = Bottom

  let zero = Interval (0, 0)

  let lift f i1 i2 =
    match (i1, i2) with
    | Bottom, _ | _, Bottom -> Bottom
    | Top, _ | _, Top -> Top
    | Interval (l1, h1), Interval (l2, h2) -> f (l1, h1) (l2, h2)

  let ( + ) = lift (fun (l1, h1) (l2, h2) -> Interval (l1 + l2, h1 + h2))

  let ( - ) = lift (fun (l1, h1) (l2, h2) -> Interval (l1 - h2, l2 - h1))

  let ( * ) =
    lift (fun (l1, h1) (l2, h2) ->
        let min_many = List.fold_left1_exn ~f:Int.min in
        let max_many = List.fold_left1_exn ~f:Int.max in
        let xs = [l1 * l2; l1 * h2; l2 * h1; h2 * h1] in
        Interval (min_many xs, max_many xs) )

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
    | Interval (l1, h1), Interval (l2, h2) -> Interval (Int.min l1 l2, Int.max h1 h2)

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
end

module AbsFixed = struct
  type t = {range: AbsInt.t; scale: int} [@@deriving compare, sexp]

  let of_fixed f = {range= AbsInt.of_int f.Fixed_point.value; scale= f.scale}

  let zero = of_fixed (Fixed_point.of_int 0)

  let bot = {range= AbsInt.bot; scale= 1}

  let top = {range= AbsInt.top; scale= 1}

  let rec unify dir f1 f2 =
    if f1.scale = f2.scale then {f1 with range= dir f1.range f2.range}
    else if f1.scale > f2.scale then unify dir f2 f1
    else
      let scale_factor = f2.scale / f1.scale in
      let scaled_range = AbsInt.(f1.range * of_int scale_factor) in
      {f2 with range= dir f2.range scaled_range}

  let meet = unify AbsInt.meet

  let join = unify AbsInt.join
end

module T = struct
  type int_ = {range: AbsInt.t; nullable: bool} [@@deriving compare, sexp]

  type date = {range: AbsInt.t; nullable: bool} [@@deriving compare, sexp]

  type bool_ = {nullable: bool} [@@deriving compare, sexp]

  type string_ = {nchars: AbsInt.t; nullable: bool} [@@deriving compare, sexp]

  type list_ = {count: AbsInt.t} [@@deriving compare, sexp]

  type tuple = {count: AbsInt.t} [@@deriving compare, sexp]

  type hash_idx = {key_count: AbsInt.t; value_count: AbsInt.t}
  [@@deriving compare, sexp]

  type ordered_idx = {count: AbsInt.t} [@@deriving compare, sexp]

  type fixed = {value: AbsFixed.t; nullable: bool} [@@deriving compare, sexp]

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
    | FuncT of (t list * [`Child_sum | `Width of int])
    | EmptyT
  [@@deriving compare, sexp]
end

include T
include Comparable.Make (T)

let bind2 : f:('a -> 'b -> 'c option) -> 'a option -> 'b option -> 'c option =
 fun ~f x y -> match (x, y) with Some a, Some b -> f a b | _ -> None

let least_general_of_primtype = function
  | PrimType.IntT {nullable} -> IntT {range= AbsInt.bot; nullable}
  | NullT -> NullT
  | DateT {nullable} -> DateT {range= AbsInt.bot; nullable}
  | FixedT {nullable} -> FixedT {value= AbsFixed.bot; nullable}
  | StringT {nullable; _} -> StringT {nchars= AbsInt.bot; nullable}
  | BoolT {nullable} -> BoolT {nullable}
  | TupleT _ | VoidT -> failwith "Not a layout type."

let rec unify_exn t1 t2 =
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
  | IntT {range= b1; nullable= n1}, IntT {range= b2; nullable= n2} ->
      IntT {range= AbsInt.join b1 b2; nullable= n1 || n2}
  | DateT {range= b1; nullable= n1}, DateT {range= b2; nullable= n2} ->
      DateT {range= AbsInt.join b1 b2; nullable= n1 || n2}
  | FixedT {value= v1; nullable= n1}, FixedT {value= v2; nullable= n2} ->
      FixedT {value= AbsFixed.join v1 v2; nullable= n1 || n2}
  | IntT x, NullT | NullT, IntT x -> IntT {x with nullable= true}
  | DateT x, NullT | NullT, DateT x -> DateT {x with nullable= true}
  | FixedT x, NullT | NullT, FixedT x -> FixedT {x with nullable= true}
  | BoolT {nullable= n1}, BoolT {nullable= n2} -> BoolT {nullable= n1 || n2}
  | BoolT _, NullT | NullT, BoolT _ -> BoolT {nullable= true}
  | StringT {nchars= b1; nullable= n1}, StringT {nchars= b2; nullable= n2} ->
      StringT {nchars= AbsInt.join b1 b2; nullable= n1 || n2}
  | StringT x, NullT | NullT, StringT x -> StringT {x with nullable= true}
  | TupleT (e1s, {count= c1}), TupleT (e2s, {count= c2}) ->
      let elem_ts =
        match List.map2 e1s e2s ~f:unify_exn with
        | Ok ts -> ts
        | Unequal_lengths -> fail "Different number of columns."
      in
      TupleT (elem_ts, {count= AbsInt.join c1 c2})
  | ListT (et1, {count= c1}), ListT (et2, {count= c2}) ->
      ListT (unify_exn et1 et2, {count= AbsInt.join c1 c2})
  | OrderedIdxT (k1, v1, {count= c1}), OrderedIdxT (k2, v2, {count= c2}) ->
      OrderedIdxT (unify_exn k1 k2, unify_exn v1 v2, {count= AbsInt.join c1 c2})
  | ( HashIdxT (kt1, vt1, {key_count= kc1; value_count= vc1})
    , HashIdxT (kt2, vt2, {key_count= kc2; value_count= vc2}) ) ->
      let kt = unify_exn kt1 kt2 in
      let vt = unify_exn vt1 vt2 in
      HashIdxT
        (kt, vt, {key_count= AbsInt.join kc1 kc2; value_count= AbsInt.join vc1 vc2})
  | EmptyT, t | t, EmptyT -> t
  | FuncT (t, `Child_sum), FuncT (t', `Child_sum) ->
      FuncT (List.map2_exn ~f:unify_exn t t', `Child_sum)
  | FuncT (t, `Width w), FuncT (t', `Width w') when Int.(w = w') ->
      FuncT (List.map2_exn ~f:unify_exn t t', `Width w)
  | NullT, _
   |IntT _, _
   |DateT _, _
   |FixedT _, _
   |BoolT _, _
   |StringT _, _
   |TupleT _, _
   |ListT _, _
   |HashIdxT _, _
   |OrderedIdxT _, _
   |FuncT _, _ ->
      fail "Unexpected types."

(** Returns the width of the tuples produced by reading a layout with this type.
   *)
let rec width = function
  | NullT | IntT _ | BoolT _ | StringT _ | FixedT _ | DateT _ -> 1
  | TupleT (ts, _) -> List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | ListT (t, _) -> width t
  | HashIdxT (kt, vt, _) | OrderedIdxT (kt, vt, _) -> width kt + width vt
  | EmptyT -> 0
  | FuncT (ts, `Child_sum) ->
      List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | FuncT (_, `Width w) -> w

let count = function
  | EmptyT -> AbsInt.of_int 0
  | NullT | IntT _ | BoolT _ | StringT _ | FixedT _ | DateT _ -> AbsInt.of_int 1
  | TupleT (_, {count}) | OrderedIdxT (_, _, {count; _}) -> count
  | HashIdxT (_, _, {value_count; _}) -> value_count
  | ListT (_, {count}) -> count
  | FuncT _ -> AbsInt.top

let rec len =
  let open AbsInt in
  let header_len field_len =
    match to_int field_len with
    | Some _ -> of_int 0
    | None -> byte_width ~nullable:false field_len |> of_int
  in
  function
  | EmptyT -> zero
  | IntT x -> byte_width ~nullable:x.nullable x.range |> of_int
  | DateT x -> byte_width ~nullable:x.nullable x.range |> of_int
  | FixedT x -> byte_width ~nullable:x.nullable x.value.range |> of_int
  | BoolT _ -> of_int 1
  | StringT x -> header_len x.nchars + x.nchars
  | TupleT (ts, _) ->
      let body_len = List.sum (module AbsInt) ts ~f:len in
      body_len + header_len body_len
  | ListT (t, x) ->
      let count_len = header_len x.count in
      let body_len = x.count * len t in
      let len_len = header_len body_len in
      count_len + len_len + body_len
  | T.OrderedIdxT (kt, vt, m) ->
      let values = m.count * len vt in
      oi_map_len kt vt m + values
  | T.HashIdxT _ ->
      (* TODO: Fix this nasty hack. *)
      Interval (0, 100000000)
  | FuncT (ts, _) -> List.sum (module AbsInt) ts ~f:len
  | NullT as t -> Error.(create "Unexpected type." t [%sexp_of: T.t] |> raise)

(** Range of ordered index map lengths. *)
and oi_map_len kt vt m = AbsInt.(m.count * (len kt + of_int (oi_ptr_size vt m)))

(** Size of pointers (in bytes) in ordered indexes. *)
and oi_ptr_size vt m = AbsInt.(byte_width ~nullable:false AbsInt.(m.count * len vt))

(** Range of hash index map lengths. *)
and hi_map_len kt vt m =
  AbsInt.(m.key_count * of_int 16 * of_int (hi_ptr_size kt vt m))

(** Size of pointers (in bytes) in hash indexes. *)
and hi_ptr_size kt vt m =
  AbsInt.(byte_width ~nullable:false AbsInt.(m.key_count * (len kt + len vt)))

(** Use the type of a hash index to decide what hash method to use. *)
let hash_kind_exn = function
  | HashIdxT (kt, _, _) -> (
    match kt with IntT _ | DateT _ -> `Direct | _ -> `Cmph )
  | _ -> failwith "Unexpected type."
