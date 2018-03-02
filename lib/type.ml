open Base
open Collections

include Type0

module AbsCount = struct
  type t =
    | Countable
    | Count of int
    | Unknown
  [@@deriving compare, sexp]

  let unify : t -> t -> t = fun x y -> match x, y with
    | Unknown, _ | _, Unknown -> Unknown
    | Count a, Count b -> if a = b then Count a else Countable
    | Countable, _ | _, Countable -> Countable
end

module AbsLen = struct
  type t =
    | Len of int
    | Variable
  [@@deriving compare, sexp]

  let unify : t -> t -> t = fun x y -> match x, y with
    | Len x, Len y when x = y -> Len x
    | Len _, Len _ | Variable, _ | _, Variable -> Variable
end

module T = struct
  type int_ = { bitwidth : int; field : Db.Field.t } [@@deriving compare, sexp]
  type bool_ = { field : Db.Field.t } [@@deriving compare, sexp]
  type string_ = { nchars : AbsLen.t; field : Db.Field.t } [@@deriving compare, sexp]
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
  type t =
    | IntT of int_
    | BoolT of bool_
    | StringT of string_
    | CrossTupleT of t list * crosstuple
    | ZipTupleT of t list * ziptuple
    | OrderedListT of t * ordered_list
    | UnorderedListT of t * unordered_list
    | TableT of t * t * table
    | EmptyT
  [@@deriving compare, sexp]
end
include T
include Comparable.Make(T)

exception TypeError of Error.t [@@deriving sexp]

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
  | (IntT { bitwidth = b1; field = f1 }, IntT { bitwidth = b2; field = f2 })
    when Db.Field.equal f1 f2 ->
    IntT { bitwidth = Int.max b1 b2; field = f1 }
  | (BoolT { field = f1 }, BoolT { field = f2 }) when Db.Field.equal f1 f2 ->
    BoolT { field = f1 }
  | (StringT { nchars = b1; field = f1 }, StringT { nchars = b2; field = f2 })
    when Db.Field.equal f1 f2 ->
    StringT { nchars = AbsLen.unify b1 b2; field = f1 }
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
  | _ -> fail "Unexpected types."

let rec of_layout_exn : Layout.t -> t =
  let fail m =
    let err = Error.of_string (Printf.sprintf "Type inference failed: %s" m) in
    raise (TypeError err)
  in
  fun l ->
    let count = match Layout.ntuples l with
      | Ok c -> AbsCount.Count c
      | _ -> Unknown
    in
    match l.node with
    | Int (x, { field }) when Int.(x = 0) -> IntT { bitwidth = 1; field }
    | Int (x, { field }) when Int.(x < 0) ->
      IntT { bitwidth = Int.floor_log2 (-x) + 1; field }
    | Int (x, { field }) -> IntT { bitwidth = Int.floor_log2 x + 1; field }
    | Bool (x, { field }) -> BoolT { field }
    | String (x, {field}) -> StringT { nchars = Len (String.length x); field }
    | Null _ -> EmptyT
    | CrossTuple ls ->
      let ts = List.map ls ~f:of_layout_exn in
      CrossTupleT (ts, { count })
    | ZipTuple ls ->
      let elems = List.map ls ~f:of_layout_exn in
      begin match List.map ls ~f:Layout.ntuples |> List.all_equal with
        | Some len -> ZipTupleT (elems, { count })
        | None -> fail "Columns have different lengths."
      end
    | UnorderedList ls ->
      let elem_t =
        List.map ls ~f:of_layout_exn |> List.fold_left ~f:unify_exn ~init:EmptyT
      in
      UnorderedListT (elem_t, { count })
    | OrderedList (ls, { field; order; lookup }) ->
      let elem_t =
        List.map ls ~f:of_layout_exn |> List.fold_left ~f:unify_exn ~init:EmptyT
      in
      OrderedListT (elem_t, { field; order; lookup; count })
    | Table (elems, { field; lookup }) ->
      let kt = Map.keys elems
               |> List.map ~f:(fun v -> Layout.of_value v |> of_layout_exn)
               |> List.fold_left ~f:unify_exn ~init:EmptyT
      in
      let vt = Map.data elems |> List.map ~f:of_layout_exn
               |> List.fold_left ~f:unify_exn ~init:EmptyT
      in
      TableT (kt, vt, { field; lookup; count })
    | Empty -> EmptyT

let rec params : t -> Set.M(TypedName).t =
  let params_of_key_option = function
    | Some k -> Layout.PredCtx.Key.params k
    | None -> Set.empty (module TypedName)
  in
  let union_list = Set.union_list (module TypedName) in
  function
  | EmptyT | IntT _ | BoolT _ | StringT _ -> Set.empty (module TypedName)
  | CrossTupleT (ts, _) | ZipTupleT (ts, _) -> List.map ts ~f:params |> union_list
  | UnorderedListT (t, _) -> params t
  | OrderedListT (t, { field; lookup = (v1, v2) }) ->
    union_list [params_of_key_option v1; params_of_key_option v2; params t]
  | TableT (_, t, { field; lookup = k }) ->
    Set.union (Layout.PredCtx.Key.params k) (params t)

let rec width : t -> int = function
  | IntT _ | BoolT _ | StringT _ -> 1
  | CrossTupleT (ts, _) | ZipTupleT (ts, _) ->
    List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | OrderedListT (t, _) | UnorderedListT (t, _) -> width t
  | TableT (_, t, _) -> width t + 1
  | EmptyT -> 0

let count : t -> AbsCount.t = function
  | EmptyT -> Count 0
  | IntT _ | BoolT _ | StringT _ -> Count 1
  | CrossTupleT (_, { count }) | ZipTupleT (_, { count })
  | OrderedListT (_, { count }) | UnorderedListT (_, { count })
  | TableT (_, _, { count }) -> count

let rec to_schema : t -> Db.Schema.t = function
  | EmptyT -> []
  | IntT m -> [m.field]
  | BoolT m -> [m.field]
  | StringT m -> [m.field]
  | CrossTupleT (ts, _) | ZipTupleT (ts, _) -> List.concat_map ~f:to_schema ts
  | OrderedListT (t, _) | UnorderedListT (t, _) -> to_schema t
  | TableT (_, t, m) -> m.field :: to_schema t
