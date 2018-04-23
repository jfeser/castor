open Base
open Collections
open Db

include Type0

exception TypeError of Error.t [@@deriving sexp]

module AbsInt = struct
  type t = (int * int) [@@deriving compare, sexp]

  let unify : t -> t -> t = fun (l1, h1) (l2, h2) ->
    (Int.min l1 l2, Int.max h1 h2)

  let abstract : int -> t = fun x -> (x, x)

  let concretize : t -> int option = fun (l, h) ->
    if l = h then Some l else None
end

module AbsCount = struct
  type t = AbsInt.t option [@@deriving compare, sexp]

  let abstract : int -> t = fun x -> Some (AbsInt.abstract x)

  let concretize : t -> int option =
    Option.bind ~f:AbsInt.concretize

  let kind : t -> [`Count of int | `Countable | `Unknown] = function
    | Some x -> begin match AbsInt.concretize x with
        | Some x -> `Count x
        | None -> `Countable
      end
    | None -> `Unknown

  let unify : t -> t -> t = fun x y ->
    let module Let_syntax = Option in
    let%bind x = x in
    let%map y = y in
    AbsInt.unify x y
end

module T = struct
  type null = { field : Db.Field.t } [@@deriving compare, sexp]
  type int_ = { range : AbsInt.t; nullable : bool; field : Db.Field.t } [@@deriving compare, sexp]
  type bool_ = { nullable : bool; field : Db.Field.t } [@@deriving compare, sexp]
  type string_ = { nchars : AbsInt.t; nullable : bool; field : Db.Field.t } [@@deriving compare, sexp]
  type ziptuple = { count : AbsCount.t } [@@deriving compare, sexp]
  type crosstuple = { count : AbsCount.t } [@@deriving compare, sexp]
  type ordered_list = {
    field : Db.Field.t; order : [`Asc | `Desc]; lookup : Layout.Range.t;
    count : AbsCount.t;
  } [@@deriving compare, sexp]
  type unordered_list = { count : AbsCount.t } [@@deriving compare, sexp]
  type table = {
    count : AbsCount.t;
    field : Db.Field.t;
    lookup : Layout.PredCtx.Key.t;
  } [@@deriving compare, sexp]
  type grouping = {
    count : AbsCount.t;
    key : Db.Field.t list;
    output : Db.Field.t Ralgebra0.agg list;
  } [@@deriving compare, sexp]
  type t =
    | NullT of null
    | IntT of int_
    | BoolT of bool_
    | StringT of string_
    | CrossTupleT of t list * crosstuple
    | ZipTupleT of t list * ziptuple
    | OrderedListT of t * ordered_list
    | UnorderedListT of t * unordered_list
    | TableT of t * t * table
    | GroupingT of t * t * grouping
    | EmptyT
  [@@deriving compare, sexp]
end
include T
include Comparable.Make(T)

let bind2 : f:('a -> 'b -> 'c option) -> 'a option -> 'b option -> 'c option =
  fun ~f x y -> match x, y with
    | Some a, Some b -> f a b
    | _ -> None

let rec unify_exn : t -> t -> t = fun t1 t2 ->
  let fail m =
    let err = Error.create
        (Printf.sprintf "Unification failed: %s" m) (t1, t2) [%sexp_of:t*t]
    in
    raise (TypeError err)
  in
  match t1, t2 with
  | NullT { field = f1 }, NullT { field = f2 } when Db.Field.equal f1 f2 ->
    NullT { field = f1 }
  | (IntT { range = b1; field = f1; nullable = n1 },
     IntT { range = b2; field = f2; nullable = n2 })
    when Db.Field.equal f1 f2 ->
    IntT { range = AbsInt.unify b1 b2; field = f1; nullable = n1 || n2 }
  | (IntT x), NullT _ | NullT _, (IntT x) -> IntT ({x with nullable = true})
  | (BoolT { field = f1; nullable = n1 },
     BoolT { field = f2; nullable = n2 }) when Db.Field.equal f1 f2 ->
    BoolT { field = f1; nullable = n1 || n2 }
  | (BoolT x), NullT _ | NullT _, (BoolT x) -> BoolT ({x with nullable = true})
  | (StringT { nchars = b1; nullable = n1; field = f1 },
     StringT { nchars = b2; nullable = n2; field = f2 })
    when Db.Field.equal f1 f2 ->
    StringT { nchars = AbsInt.unify b1 b2; field = f1; nullable = n1 || n2 }
  | (StringT x), NullT _ | NullT _, (StringT x) -> StringT ({x with nullable = true})
  | CrossTupleT (e1s, { count = c1 }), CrossTupleT (e2s, { count = c2 }) ->
      let elem_ts = match List.map2 e1s e2s ~f:unify_exn with
        | Ok ts -> ts
        | Unequal_lengths -> fail "Different number of columns."
      in
      CrossTupleT (elem_ts, { count = AbsCount.unify c1 c2 })
  | ZipTupleT (e1s, { count = c1 }), ZipTupleT (e2s, { count = c2 }) ->
      let elem_ts = match List.map2 e1s e2s ~f:unify_exn with
        | Ok ts -> ts
        | Unequal_lengths -> fail "Different number of columns."
      in
      ZipTupleT (elem_ts, { count = AbsCount.unify c1 c2 })
  | UnorderedListT (et1, { count = c1 }), UnorderedListT (et2, { count = c2 }) ->
    UnorderedListT (unify_exn et1 et2, { count = AbsCount.unify c1 c2 })
  | OrderedListT (e1, { field = f1; order = o1; lookup = l1; count = c1 }),
    OrderedListT (e2, { field = f2; order = o2; lookup = l2; count = c2 })
    when Db.Field.(f1 = f2) && Polymorphic_compare.(o1 = o2) &&
         Layout.Range.(l1 = l2) ->
    OrderedListT (unify_exn e1 e2, { field = f1; order = o1; lookup = l1;
                                     count = AbsCount.unify c1 c2 })
  | (TableT (kt1, vt1, { count = c1; field = f1; lookup = l1 }),
     TableT (kt2, vt2, { count = c2; field = f2; lookup = l2 }))
    when Db.Field.(f1 = f2) && Layout.PredCtx.Key.(l1 = l2) ->
    let kt = unify_exn kt1 kt2 in
    let vt = unify_exn vt1 vt2 in
    TableT (kt, vt, { field = f1; lookup = l1; count = AbsCount.unify c1 c2 })
  | EmptyT, t | t, EmptyT -> t
  | GroupingT (kt1, vt1, m1), GroupingT (kt2, vt2, m2)
    when Polymorphic_compare.(m1 = m2) ->
    GroupingT (unify_exn kt1 kt2, unify_exn vt1 vt2, m1)
  | _ -> fail "Unexpected types."

let rec of_layout_exn : Layout.t -> t =
  let fail m =
    let err = Error.of_string (Printf.sprintf "Type inference failed: %s" m) in
    raise (TypeError err)
  in
  let of_many ls = List.map ls ~f:of_layout_exn in
  let unify_many ls = of_many ls |> List.fold_left ~f:unify_exn ~init:EmptyT in
  fun l ->
    let count = match Layout.ntuples l with
      | Ok c -> AbsCount.abstract c
      | _ -> None
    in
    match l.node with
    | Int (x, {node = { field }}) ->
      IntT { range = AbsInt.abstract x; nullable = false; field }
    | Bool (x, {node = { field }}) -> BoolT { nullable = false; field }
    | String (x, {node = { field }}) ->
      StringT { nchars = AbsInt.abstract (String.length x); nullable = false; field }
    | Null { node = { field } } -> NullT { field }
    | CrossTuple ls -> CrossTupleT (of_many ls, { count })
    | ZipTuple ls ->
      begin match List.map ls ~f:Layout.ntuples |> List.all_equal with
        | Some len -> ZipTupleT (of_many ls, { count })
        | None -> fail "Columns have different lengths."
      end
    | UnorderedList ls -> UnorderedListT (unify_many ls, { count })
    | OrderedList (ls, { field; order; lookup }) ->
      OrderedListT (unify_many ls, { field; order; lookup; count })
    | Table (elems, { field; lookup }) ->
      let kt = Map.keys elems |> List.map ~f:Layout.of_value |> unify_many in
      let vt = Map.data elems |> unify_many in
      TableT (kt, vt, { field; lookup; count })
    | Empty -> EmptyT
    | Grouping (elems, { key; output }) ->
      let keys, values = List.unzip elems in
      GroupingT (unify_many keys, unify_many values, { key; output; count })

let rec params : t -> Set.M(TypedName).t =
  let params_of_key_option = function
    | Some k -> Layout.PredCtx.Key.params k
    | None -> Set.empty (module TypedName)
  in
  let union_list = Set.union_list (module TypedName) in
  function
  | EmptyT | NullT _ | IntT _ | BoolT _ | StringT _ -> Set.empty (module TypedName)
  | CrossTupleT (ts, _) | ZipTupleT (ts, _) -> List.map ts ~f:params |> union_list
  | UnorderedListT (t, _) -> params t
  | OrderedListT (t, { field; lookup = (v1, v2) }) ->
    union_list [params_of_key_option v1; params_of_key_option v2; params t]
  | TableT (_, t, { field; lookup = k }) ->
    Set.union (Layout.PredCtx.Key.params k) (params t)
  | GroupingT (kt, vt, _) -> Set.union (params kt) (params vt)

let rec width : t -> int = function
  | NullT _ | IntT _ | BoolT _ | StringT _ -> 1
  | CrossTupleT (ts, _) | ZipTupleT (ts, _) ->
    List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | OrderedListT (t, _) | UnorderedListT (t, _) -> width t
  | TableT (_, t, _) -> width t + 1
  | GroupingT (_, _, { output }) -> List.length output
  | EmptyT -> 0

let count : t -> AbsCount.t = function
  | EmptyT -> AbsCount.abstract 0
  | NullT _ | IntT _ | BoolT _ | StringT _ -> AbsCount.abstract 1
  | CrossTupleT (_, { count }) | ZipTupleT (_, { count })
  | OrderedListT (_, { count }) | UnorderedListT (_, { count })
  | TableT (_, _, { count }) -> count
  | GroupingT (_, _, m) -> m.count

let rec to_schema : t -> Db.Schema.t = function
  | EmptyT -> []
  | NullT m -> [m.field]
  | IntT m -> [m.field]
  | BoolT m -> [m.field]
  | StringT m -> [m.field]
  | CrossTupleT (ts, _) | ZipTupleT (ts, _) -> List.concat_map ~f:to_schema ts
  | OrderedListT (t, _) | UnorderedListT (t, _) -> to_schema t
  | TableT (_, t, m) -> m.field :: to_schema t
  | GroupingT (_, _, m) -> Layout.grouping_to_schema m.output
