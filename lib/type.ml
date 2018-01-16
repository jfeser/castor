open Base
open Collections

include Type0

type ziptuple = { len : int } [@@deriving compare, sexp]
type scalar = { field : Db.Field.t } [@@deriving compare, sexp]
type t =
  | ScalarT of PrimType.t * scalar
  | CrossTupleT of (t * int) list
  | ZipTupleT of t list * ziptuple
  | OrderedListT of t * Layout.ordered_list
  | UnorderedListT of t
  | TableT of PrimType.t * t * Layout.table
  | EmptyT
[@@deriving compare, sexp]

exception TypeError of Error.t [@@deriving sexp]

let rec unify_exn : t -> t -> t = fun t1 t2 ->
  let fail m =
    let err = Error.create
        (Printf.sprintf "Unification failed: %s" m) (t1, t2) [%sexp_of:t*t]
    in
    raise (TypeError err)
  in
  match t1, t2 with
  | (ScalarT (pt1, m1), ScalarT (pt2, m2))
    when [%compare.equal:PrimType.t * scalar] (pt1, m1) (pt2, m2) -> t1
  | (CrossTupleT e1s, CrossTupleT e2s) -> begin
      let m_es = List.map2 e1s e2s ~f:(fun (e1, l1) (e2, l2) ->
          if l1 <> l2 then fail "Columns have different lengths." else
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
  | (TableT (pt1, et1, t1), TableT (pt2, et2, t2))
    when [%compare.equal:PrimType.t] pt1 pt2
      && Layout.compare_table t1 t2 = 0 ->
    TableT (pt1, unify_exn et1 et2, t1)
  | EmptyT, t | t, EmptyT -> t
  | _ -> fail "Unexpected types."

let rec of_layout_exn : Layout.t -> t =
  let fail m =
    let err = Error.of_string (Printf.sprintf "Type inference failed: %s" m) in
    raise (TypeError err)
  in
  function
  | Scalar { rel; field; idx; value } ->
    ScalarT (PrimType.of_primvalue value, { field })
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
  | Table (elems, ({ field = { dtype }; } as t')) ->
    let pt =
      let open PrimType in
      match dtype with
      | DInt _ -> IntT
      | DBool _ -> BoolT
      | DString _ -> StringT
      | _ -> fail "Unexpected data type."
    in
    let t = Map.data elems |> List.map ~f:of_layout_exn
            |> List.fold_left ~f:unify_exn ~init:EmptyT
    in
    TableT (pt, t, t')
  | Empty -> EmptyT

let rec params : t -> Set.M(TypedName).t =
  let params_of_key_option = function
    | Some k -> Layout.PredCtx.Key.params k
    | None -> Set.empty (module TypedName)
  in
  let union_list = Set.union_list (module TypedName) in
  function
  | ScalarT _ | EmptyT -> Set.empty (module TypedName)
  | CrossTupleT ts -> List.map ts ~f:(fun (t, _) -> params t) |> union_list
  | ZipTupleT (ts, _) -> List.map ts ~f:params |> union_list
  | UnorderedListT t -> params t
  | OrderedListT (t, { field; lookup = (v1, v2) }) ->
    union_list [params_of_key_option v1; params_of_key_option v2; params t]
  | TableT (_, t, { field; lookup = k }) ->
    Set.union (Layout.PredCtx.Key.params k) (params t)

let rec width : t -> int = function
  | ScalarT _ -> 1
  | CrossTupleT ts ->
    List.map ts ~f:(fun (t, _) -> width t)
    |> List.sum (module Int) ~f:(fun x -> x)
  | ZipTupleT (ts, _) ->
    List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | OrderedListT (t, _) | UnorderedListT t -> width t
  | TableT (_, t, _) -> width t + 1
  | EmptyT -> 0

let rec to_schema : t -> Layout.Schema.t = function
  | EmptyT -> []
  | ScalarT (_, m) -> [m.field]
  | CrossTupleT ts -> List.concat_map ~f:(fun (t, _) -> to_schema t) ts
  | ZipTupleT (ts, _) -> List.concat_map ~f:to_schema ts
  | OrderedListT (t, _) | UnorderedListT t -> to_schema t
  | TableT (_, t, m) -> m.field :: to_schema t
