open Base
open Collections
include Type0

exception TypeError of Error.t [@@deriving sexp]

module AbsInt = struct
  type t = int * int [@@deriving compare, sexp]

  let zero : t = (0, 0)

  let ( + ) : t -> t -> t = fun (l1, h1) (l2, h2) -> (l1 + l2, h1 + h2)

  let ( * ) : t -> t -> t =
   fun (l1, h1) (l2, h2) ->
    let min_many = List.fold_left1_exn ~f:Int.min in
    let max_many = List.fold_left1_exn ~f:Int.min in
    let xs = [l1 * l2; l1 * h2; l2 * h1; h2 * h1] in
    (min_many xs, max_many xs)

  let unify : t -> t -> t = fun (l1, h1) (l2, h2) -> (Int.min l1 l2, Int.max h1 h2)

  let abstract : int -> t = fun x -> (x, x)

  let concretize : t -> int option = fun (l, h) -> if l = h then Some l else None

  let bitwidth : nullable:bool -> t -> int =
   fun ~nullable (_, h) ->
    let open Int in
    (* Ensures we can store null values. *)
    let h = if nullable then h + 1 else h in
    Int.max (Int.ceil_log2 h |> Int.round_up ~to_multiple_of:8) 8

  let bytewidth : nullable:bool -> t -> int =
   fun ~nullable x ->
    let open Int in
    let bw = bitwidth ~nullable x in
    if bw % 8 = 0 then bw / 8 else (bw / 8) + 1
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
  type null = {field: Db.Field.t} [@@deriving compare, sexp]

  type int_ = {range: AbsInt.t; nullable: bool; field: Db.Field.t}
  [@@deriving compare, sexp]

  type bool_ = {nullable: bool; field: Db.Field.t} [@@deriving compare, sexp]

  type string_ = {nchars: AbsInt.t; nullable: bool; field: Db.Field.t}
  [@@deriving compare, sexp]

  type ziptuple = {count: AbsCount.t} [@@deriving compare, sexp]

  type crosstuple = {count: AbsCount.t} [@@deriving compare, sexp]

  type ordered_list = {count: AbsCount.t} [@@deriving compare, sexp_of]

  type unordered_list = {count: AbsCount.t} [@@deriving compare, sexp]

  type table = {count: AbsCount.t} [@@deriving compare, sexp_of]

  type grouping =
    {count: AbsCount.t; key: Db.Field.t list; output: Db.Field.t Ralgebra0.agg list}
  [@@deriving compare, sexp_of]

  type t =
    | NullT of null
    | IntT of int_
    | BoolT of bool_
    | StringT of string_
    | CrossTupleT of (t list * crosstuple)
    | ZipTupleT of (t list * ziptuple)
    | UnorderedListT of (t * unordered_list)
    | TableT of (t * t * table)
    | OrderedIdxT of (t * t * ordered_list)
    | GroupingT of (t * t * grouping)
    | FuncT of t list * [`Child_sum | `Width of int]
    | EmptyT
  [@@deriving compare, sexp_of]
end

include T
include Comparable.Make (T)

let bind2 : f:('a -> 'b -> 'c option) -> 'a option -> 'b option -> 'c option =
 fun ~f x y -> match (x, y) with Some a, Some b -> f a b | _ -> None

let rec unify_exn : t -> t -> t =
 fun t1 t2 ->
  let fail m =
    let err =
      Error.create
        (Printf.sprintf "Unification failed: %s" m)
        (t1, t2) [%sexp_of : t * t]
    in
    raise (TypeError err)
  in
  match (t1, t2) with
  | NullT {field= f1}, NullT {field= f2} when Db.Field.equal f1 f2 ->
      NullT {field= f1}
  | ( IntT {range= b1; field= f1; nullable= n1}
    , IntT {range= b2; field= f2; nullable= n2} )
    when Db.Field.equal f1 f2 ->
      IntT {range= AbsInt.unify b1 b2; field= f1; nullable= n1 || n2}
  | IntT x, NullT _ | NullT _, IntT x -> IntT {x with nullable= true}
  | BoolT {field= f1; nullable= n1}, BoolT {field= f2; nullable= n2}
    when Db.Field.equal f1 f2 ->
      BoolT {field= f1; nullable= n1 || n2}
  | BoolT x, NullT _ | NullT _, BoolT x -> BoolT {x with nullable= true}
  | ( StringT {nchars= b1; nullable= n1; field= f1}
    , StringT {nchars= b2; nullable= n2; field= f2} )
    when Db.Field.equal f1 f2 ->
      StringT {nchars= AbsInt.unify b1 b2; field= f1; nullable= n1 || n2}
  | StringT x, NullT _ | NullT _, StringT x -> StringT {x with nullable= true}
  | CrossTupleT (e1s, {count= c1}), CrossTupleT (e2s, {count= c2}) ->
      let elem_ts =
        match List.map2 e1s e2s ~f:unify_exn with
        | Ok ts -> ts
        | Unequal_lengths -> fail "Different number of columns."
      in
      CrossTupleT (elem_ts, {count= AbsCount.unify c1 c2})
  | ZipTupleT (e1s, {count= c1}), ZipTupleT (e2s, {count= c2}) ->
      let elem_ts =
        match List.map2 e1s e2s ~f:unify_exn with
        | Ok ts -> ts
        | Unequal_lengths -> fail "Different number of columns."
      in
      ZipTupleT (elem_ts, {count= AbsCount.unify c1 c2})
  | UnorderedListT (et1, {count= c1}), UnorderedListT (et2, {count= c2}) ->
      UnorderedListT (unify_exn et1 et2, {count= AbsCount.unify c1 c2})
  | OrderedIdxT (k1, v1, {count= c1}), OrderedIdxT (k2, v2, {count= c2}) ->
      OrderedIdxT (unify_exn k1 k2, unify_exn v1 v2, {count= AbsCount.unify c1 c2})
  | TableT (kt1, vt1, {count= c1}), TableT (kt2, vt2, {count= c2}) ->
      let kt = unify_exn kt1 kt2 in
      let vt = unify_exn vt1 vt2 in
      TableT (kt, vt, {count= AbsCount.unify c1 c2})
  | EmptyT, t | t, EmptyT -> t
  | GroupingT (kt1, vt1, m1), GroupingT (kt2, vt2, m2)
    when Polymorphic_compare.(m1 = m2) ->
      GroupingT (unify_exn kt1 kt2, unify_exn vt1 vt2, m1)
  | FuncT (t, `Child_sum), FuncT (t', `Child_sum) ->
      FuncT (List.map2_exn ~f:unify_exn t t', `Child_sum)
  | FuncT (t, `Width w), FuncT (t', `Width w') when Int.(w = w') ->
      FuncT (List.map2_exn ~f:unify_exn t t', `Width w)
  | _ -> fail "Unexpected types."

let rec width : t -> int = function
  | NullT _ | IntT _ | BoolT _ | StringT _ -> 1
  | CrossTupleT (ts, _) | ZipTupleT (ts, _) ->
      List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | UnorderedListT (t, _) -> width t
  | TableT (kt, vt, _) | OrderedIdxT (kt, vt, _) -> width kt + width vt
  | GroupingT (_, _, {output; _}) -> List.length output
  | EmptyT -> 0
  | FuncT (ts, `Child_sum) ->
      List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | FuncT (_, `Width w) -> w

let count : t -> AbsCount.t = function
  | EmptyT -> AbsCount.abstract 0
  | NullT _ | IntT _ | BoolT _ | StringT _ -> AbsCount.abstract 1
  | CrossTupleT (_, {count})
   |ZipTupleT (_, {count})
   |OrderedIdxT (_, _, {count; _})
   |UnorderedListT (_, {count})
   |TableT (_, _, {count; _}) ->
      count
  | GroupingT (_, _, m) -> m.count
  | FuncT _ -> AbsCount.top

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
