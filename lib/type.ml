open Base
open Collections

include Type0

module T = struct
  type ziptuple = { len : int } [@@deriving compare, sexp]
  type scalar = { field : Db.Field.t } [@@deriving compare, sexp]
  type int_ = { bitwidth : int; field : Db.Field.t } [@@deriving compare, sexp]
  type bool_ = { field : Db.Field.t } [@@deriving compare, sexp]
  type string_ = { length : int; field : Db.Field.t } [@@deriving compare, sexp]
  type t =
    | IntT of int_
    | BoolT of bool_
    | StringT of string_
    | CrossTupleT of (t * int) list
    | ZipTupleT of t list * ziptuple
    | OrderedListT of t * Layout.ordered_list
    | UnorderedListT of t
    | TableT of t * t * Layout.table
    | EmptyT
  [@@deriving compare, sexp]
end
include T
include Comparable.Make(T)

exception TypeError of Error.t [@@deriving sexp]

let rec unify_exn : t -> t -> t = fun t1 t2 ->
  let fail m =
    let err = Error.create
        (Printf.sprintf "Unification failed: %s" m) (t1, t2) [%sexp_of:t*t]
    in
    raise (TypeError err)
  in
  match t1, t2 with
  | (IntT { bitwidth = b1; field = f1 }, IntT { bitwidth = b2; field = f2 })
    when Db.Field.equal f1 f2 -> IntT { bitwidth = Int.max b1 b2; field = f1 }
  | (BoolT { field = f1 }, BoolT { field = f2 })
    when Db.Field.equal f1 f2 -> BoolT { field = f1 }
  | (StringT { length = b1; field = f1 }, StringT { length = b2; field = f2 })
    when Db.Field.equal f1 f2 -> StringT { length = Int.max b1 b2; field = f1 }
  | (CrossTupleT e1s, CrossTupleT e2s) -> begin
      let m_es = List.map2 e1s e2s ~f:(fun (e1, l1) (e2, l2) ->
          if Int.(l1 <> l2) then fail "Columns have different lengths." else
            let e = unify_exn e1 e2 in
            (e, l1))
      in
      match m_es with
      | Ok ts -> CrossTupleT ts
      | Unequal_lengths -> fail "Different number of columns."
    end
  | ZipTupleT (e1, z1), ZipTupleT (e2, z2) when
      [%compare.equal:ziptuple] z1 z2 ->
    begin
      match List.map2 e1 e2 ~f:unify_exn with
      | Ok ts -> ZipTupleT (ts, z1)
      | Unequal_lengths -> fail "Different number of columns."
    end
  | UnorderedListT et1, UnorderedListT et2 -> UnorderedListT (unify_exn et1 et2)
  | OrderedListT (e1, l1), OrderedListT (e2, l2)
    when [%compare.equal:Layout.ordered_list] l1 l2 ->
    OrderedListT (unify_exn e1 e2, l1)
  | (TableT (kt1, vt1, t1), TableT (kt2, vt2, t2))
    when [%compare.equal:Layout.table] t1 t2 ->
    let kt = unify_exn kt1 kt2 in
    let vt = unify_exn vt1 vt2 in
    TableT (kt, vt, t1)
  | EmptyT, t | t, EmptyT -> t
  | _ -> fail "Unexpected types."

let rec of_layout_exn : Layout.t -> t =
  let fail m =
    let err = Error.of_string (Printf.sprintf "Type inference failed: %s" m) in
    raise (TypeError err)
  in
  let of_primvalue_exn : Db.Field.t -> Db.primvalue -> t = fun field -> function
    | `Unknown s | `String s -> StringT { length = String.length s; field }
    | `Bool _ -> BoolT { field }
    | `Int x when Int.(x = 0) -> IntT { bitwidth = 1; field }
    | `Int x when Int.(x < 0) -> IntT { bitwidth = Int.floor_log2 (-x) + 1; field }
    | `Int x -> IntT { bitwidth = Int.floor_log2 x + 1; field }
  in
  function
  | Scalar { field; value } -> of_primvalue_exn field value
  | CrossTuple ls ->
    let ts = List.map ls ~f:(fun l -> (of_layout_exn l, Layout.ntuples l)) in
    CrossTupleT ts
  | ZipTuple ls ->
    let elems = List.map ls ~f:of_layout_exn in
    begin match List.map ls ~f:Layout.ntuples |> List.all_equal with
      | Some len -> ZipTupleT (elems, { len; })
      | None -> fail "Columns have different lengths."
    end
  | UnorderedList ls ->
    let elem_t = List.map ls ~f:of_layout_exn
                 |> List.fold_left ~f:unify_exn ~init:EmptyT
    in
    UnorderedListT elem_t
  | OrderedList (elems, l) ->
    let elem_t = List.map elems ~f:of_layout_exn
                 |> List.fold_left ~f:unify_exn ~init:EmptyT
    in
    OrderedListT (elem_t, l)
  | Table (elems, tbl) ->
    let kt = Map.keys elems
             |> List.map ~f:(fun Db.Value.({ field; value }) ->
                 of_primvalue_exn field value)
             |> List.fold_left ~f:unify_exn ~init:EmptyT
    in
    let vt = Map.data elems |> List.map ~f:of_layout_exn
             |> List.fold_left ~f:unify_exn ~init:EmptyT
    in
    TableT (kt, vt, tbl)
  | Empty -> EmptyT

let rec params : t -> Set.M(TypedName).t =
  let params_of_key_option = function
    | Some k -> Layout.PredCtx.Key.params k
    | None -> Set.empty (module TypedName)
  in
  let union_list = Set.union_list (module TypedName) in
  function
  | EmptyT | IntT _ | BoolT _ | StringT _ -> Set.empty (module TypedName)
  | CrossTupleT ts -> List.map ts ~f:(fun (t, _) -> params t) |> union_list
  | ZipTupleT (ts, _) -> List.map ts ~f:params |> union_list
  | UnorderedListT t -> params t
  | OrderedListT (t, { field; lookup = (v1, v2) }) ->
    union_list [params_of_key_option v1; params_of_key_option v2; params t]
  | TableT (_, t, { field; lookup = k }) ->
    Set.union (Layout.PredCtx.Key.params k) (params t)

let rec width : t -> int = function
  | IntT _ | BoolT _ | StringT _ -> 1
  | CrossTupleT ts ->
    List.map ts ~f:(fun (t, _) -> width t)
    |> List.sum (module Int) ~f:(fun x -> x)
  | ZipTupleT (ts, _) ->
    List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | OrderedListT (t, _) | UnorderedListT t -> width t
  | TableT (_, t, _) -> width t + 1
  | EmptyT -> 0

let rec to_schema : t -> Db.Schema.t = function
  | EmptyT -> []
  | IntT m -> [m.field]
  | BoolT m -> [m.field]
  | StringT m -> [m.field]
  | CrossTupleT ts -> List.concat_map ~f:(fun (t, _) -> to_schema t) ts
  | ZipTupleT (ts, _) -> List.concat_map ~f:to_schema ts
  | OrderedListT (t, _) | UnorderedListT t -> to_schema t
  | TableT (_, t, m) -> m.field :: to_schema t
