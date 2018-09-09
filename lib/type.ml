open Base
open Collections
include Type0

exception TypeError of Error.t [@@deriving sexp]

module AbsInt = struct
  type t = int * int [@@deriving compare, sexp]

  let zero : t = (0, 0)

  let ( + ) : t -> t -> t = fun (l1, h1) (l2, h2) -> (l1 + l2, h1 + h2)

  let ( - ) (l1, h1) (l2, h2) = (l1 - h2, l2 - h1)

  let ( * ) : t -> t -> t =
   fun (l1, h1) (l2, h2) ->
    let min_many = List.fold_left1_exn ~f:Int.min in
    let max_many = List.fold_left1_exn ~f:Int.min in
    let xs = [l1 * l2; l1 * h2; l2 * h1; h2 * h1] in
    (min_many xs, max_many xs)

  let unify : t -> t -> t = fun (l1, h1) (l2, h2) -> (Int.min l1 l2, Int.max h1 h2)

  let abstract : int -> t = fun x -> (x, x)

  let concretize : t -> int option = fun (l, h) -> if l = h then Some l else None

  let byte_width ~nullable (_, h) =
    let open Int in
    let h = if nullable then h + 1 else h in
    if h = 0 then 1 else Int.max (Int.ceil_log2 h / 8) 1
end

module AbsCount = struct
  type t = AbsInt.t option [@@deriving compare, sexp]

  let zero : t = Some AbsInt.zero

  let top = None

  let ( + ) : t -> t -> t =
   fun a b -> match (a, b) with Some x, Some y -> Some AbsInt.(x + y) | _ -> None

  let ( * ) : t -> t -> t =
   fun a b -> match (a, b) with Some x, Some y -> Some AbsInt.(x * y) | _ -> None

  let abstract : int -> t = fun x -> Some (AbsInt.abstract x)

  let concretize : t -> int option = Option.bind ~f:AbsInt.concretize

  let kind : t -> [`Count of int | `Countable | `Unknown] = function
    | Some x -> (
      match AbsInt.concretize x with Some x -> `Count x | None -> `Countable )
    | None -> `Unknown

  let unify : t -> t -> t =
   fun x y ->
    let module Let_syntax = Option in
    let%bind x = x in
    let%map y = y in
    AbsInt.unify x y
end

module T = struct
  type int_ = {range: AbsInt.t; nullable: bool} [@@deriving compare, sexp]

  type bool_ = {nullable: bool} [@@deriving compare, sexp]

  type string_ = {nchars: AbsInt.t; nullable: bool} [@@deriving compare, sexp]

  type list_ = {count: AbsInt.t} [@@deriving compare, sexp]

  type tuple = {count: AbsCount.t} [@@deriving compare, sexp]

  type hash_idx = {count: AbsCount.t} [@@deriving compare, sexp_of]

  type ordered_idx = {count: AbsCount.t} [@@deriving compare, sexp_of]

  type t =
    | NullT
    | IntT of int_
    | BoolT of bool_
    | StringT of string_
    | TupleT of (t list * tuple)
    | ListT of (t * list_)
    | HashIdxT of (t * t * hash_idx)
    | OrderedIdxT of (t * t * ordered_idx)
    | FuncT of t list * [`Child_sum | `Width of int]
    | EmptyT
  [@@deriving compare, sexp_of]
end

include T
include Comparable.Make (T)

let bind2 : f:('a -> 'b -> 'c option) -> 'a option -> 'b option -> 'c option =
 fun ~f x y -> match (x, y) with Some a, Some b -> f a b | _ -> None

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
      IntT {range= AbsInt.unify b1 b2; nullable= n1 || n2}
  | IntT x, NullT | NullT, IntT x -> IntT {x with nullable= true}
  | BoolT {nullable= n1}, BoolT {nullable= n2} -> BoolT {nullable= n1 || n2}
  | BoolT _, NullT | NullT, BoolT _ -> BoolT {nullable= true}
  | StringT {nchars= b1; nullable= n1}, StringT {nchars= b2; nullable= n2} ->
      StringT {nchars= AbsInt.unify b1 b2; nullable= n1 || n2}
  | StringT x, NullT | NullT, StringT x -> StringT {x with nullable= true}
  | TupleT (e1s, {count= c1}), TupleT (e2s, {count= c2}) ->
      let elem_ts =
        match List.map2 e1s e2s ~f:unify_exn with
        | Ok ts -> ts
        | Unequal_lengths -> fail "Different number of columns."
      in
      TupleT (elem_ts, {count= AbsCount.unify c1 c2})
  | ListT (et1, {count= c1}), ListT (et2, {count= c2}) ->
      ListT (unify_exn et1 et2, {count= AbsInt.unify c1 c2})
  | OrderedIdxT (k1, v1, {count= c1}), OrderedIdxT (k2, v2, {count= c2}) ->
      OrderedIdxT (unify_exn k1 k2, unify_exn v1 v2, {count= AbsCount.unify c1 c2})
  | HashIdxT (kt1, vt1, {count= c1}), HashIdxT (kt2, vt2, {count= c2}) ->
      let kt = unify_exn kt1 kt2 in
      let vt = unify_exn vt1 vt2 in
      HashIdxT (kt, vt, {count= AbsCount.unify c1 c2})
  | EmptyT, t | t, EmptyT -> t
  | FuncT (t, `Child_sum), FuncT (t', `Child_sum) ->
      FuncT (List.map2_exn ~f:unify_exn t t', `Child_sum)
  | FuncT (t, `Width w), FuncT (t', `Width w') when Int.(w = w') ->
      FuncT (List.map2_exn ~f:unify_exn t t', `Width w)
  | _ -> fail "Unexpected types."

(** Returns the width of the tuples produced by reading a layout with this type.
   *)
let rec width = function
  | NullT | IntT _ | BoolT _ | StringT _ -> 1
  | TupleT (ts, _) -> List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | ListT (t, _) -> width t
  | HashIdxT (kt, vt, _) | OrderedIdxT (kt, vt, _) -> width kt + width vt
  | EmptyT -> 0
  | FuncT (ts, `Child_sum) ->
      List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | FuncT (_, `Width w) -> w

let count : t -> AbsCount.t = function
  | EmptyT -> AbsCount.abstract 0
  | NullT | IntT _ | BoolT _ | StringT _ -> AbsCount.abstract 1
  | TupleT (_, {count})
   |OrderedIdxT (_, _, {count; _})
   |HashIdxT (_, _, {count; _}) ->
      count
  | ListT (_, {count}) -> Some count
  | FuncT _ -> AbsCount.top

let rec len =
  let open AbsInt in
  function
  | EmptyT -> zero
  | NullT -> failwith "Unexpected type."
  | IntT x -> byte_width ~nullable:x.nullable x.range |> abstract
  | BoolT _ -> abstract 1
  | StringT x ->
      let header_len = byte_width ~nullable:false x.nchars |> abstract in
      header_len + x.nchars
  | TupleT (ts, _) ->
      let body_len = List.sum (module AbsInt) ts ~f:len in
      let header_len = byte_width ~nullable:false body_len |> abstract in
      body_len + header_len
  | ListT (t, x) ->
      let count_len = byte_width ~nullable:false x.count |> abstract in
      let body_len = x.count * len t in
      let len_len = byte_width ~nullable:false body_len |> abstract in
      count_len + len_len + body_len
  | T.HashIdxT _ | T.OrderedIdxT (_, _, _) -> (0, 100000)
  | FuncT (ts, _) -> List.sum (module AbsInt) ts ~f:len

(* let rec to_schema : t -> Db.Schema.t = function
 *   | EmptyT -> []
 *   | NullT m -> [m.field]
 *   | IntT m -> [m.field]
 *   | BoolT m -> [m.field]
 *   | StringT m -> [m.field]
 *   | CrossTupleT (ts, _) | ZipTupleT (ts, _) -> List.concat_map ~f:to_schema ts
 *   | OrderedListT (t, _) | UnorderedListT (t, _) -> to_schema t
 *   | TableT (_, t, m) -> m.field :: to_schema t
 *   | GroupingT (_, _, m) -> Layout.grouping_to_schema m.output *)
