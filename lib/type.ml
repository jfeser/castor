open Base

open Collections

type p = BoolT | IntT | StringT [@@deriving compare, sexp]
type ziptuple = { len : int } [@@deriving compare, sexp]
type t =
  | ScalarT of p
  | CrossTupleT of (t * int) list
  | ZipTupleT of t list * ziptuple
  | OrderedListT of t * Layout.ordered_list
  | UnorderedListT of t
  | TableT of p * t * Layout.table
  | EmptyT
[@@deriving compare, sexp]

exception UnifyError

let rec unify_exn : t -> t -> t = fun t1 t2 ->
  match t1, t2 with
  | (ScalarT pt1, ScalarT pt2) when compare_p pt1 pt2 = 0 -> t1
  | (CrossTupleT e1s, CrossTupleT e2s) -> begin
      let m_es = List.map2 e1s e2s ~f:(fun (e1, l1) (e2, l2) ->
          if l1 <> l2 then raise UnifyError else
            let e = unify_exn e1 e2 in
            (e, l1))
      in
      match m_es with
      | Ok ts -> CrossTupleT ts
      | Unequal_lengths -> raise UnifyError
    end
  | ZipTupleT (e1, z1), ZipTupleT (e2, z2) when compare_ziptuple z1 z2 = 0 ->
    begin
      match List.map2 e1 e2 ~f:unify_exn with
      | Ok ts -> ZipTupleT (ts, z1)
      | Unequal_lengths -> raise UnifyError
    end
  | UnorderedListT et1, UnorderedListT et2 -> UnorderedListT (unify_exn et1 et2)
  | OrderedListT (e1, l1), OrderedListT (e2, l2)
    when Layout.compare_ordered_list l1 l2 = 0 ->
    OrderedListT (unify_exn e1 e2, l1)
  | (TableT (pt1, et1, t1), TableT (pt2, et2, t2))
    when compare_p pt1 pt2 = 0 && Layout.compare_table t1 t2 = 0 ->
    TableT (pt1, unify_exn et1 et2, t1)
  | EmptyT, EmptyT -> EmptyT
  | _ -> raise UnifyError

let rec of_layout_exn : Layout.t -> t = function
  | Scalar { rel; field; idx; value } ->
    let pt = match value with
      | `Bool _ -> BoolT
      | `Int _ -> IntT
      | `String _ -> StringT
      | `Unknown _ -> StringT
    in
    ScalarT pt
  | CrossTuple ls ->
    let ts = List.map ls ~f:(fun l -> (of_layout_exn l, Layout.ntuples l)) in
    CrossTupleT ts
  | ZipTuple ls ->
    let elems = List.map ls ~f:of_layout_exn in
    begin match List.map ls ~f:Layout.ntuples |> List.all_equal with
      | Some len -> ZipTupleT (elems, { len; })
      | None -> raise UnifyError
    end
  | UnorderedList ls ->
    let elem_t =
      List.map ls ~f:of_layout_exn
      |> List.fold_left ~f:unify_exn ~init:EmptyT
    in
    UnorderedListT elem_t
  | OrderedList (elems, l) ->
    let elem_t =
      List.map elems ~f:of_layout_exn
      |> List.fold_left ~f:unify_exn ~init:EmptyT
    in
    OrderedListT (elem_t, l)
  | Table (elems, ({ field = { dtype }; } as t')) ->
    let pt = match dtype with
      | DInt _ -> IntT
      | DBool _ -> BoolT
      | DString _ -> StringT
      | _ -> failwith "Unexpected type."
    in
    let t =
      Map.data elems
      |> List.map ~f:of_layout_exn
      |> List.fold_left ~f:unify_exn ~init:EmptyT
    in
    TableT (pt, t, t')
  | Empty -> EmptyT

(* let rec params : t -> Set.M(String).t = function
 *   | ScalarT _ | EmptyT -> Set.empty (module String)
 *   | CrossTupleT ts ->
 *     List.map ts ~f:(fun (t, _) -> params t) |> Set.union_list (module String)
 *   | ZipTupleT { elems } ->
 *     List.map elems ~f:params |> Set.union_list (module String)
 *   | UnorderedListT t -> params t
 *   | OrderedListT { field; order; elems; lookup = (v1, v2) } ->
 *     let s1 = match v1 with
 *       | Some k -> Layout.PredCtx.Key.params k
 *       | None -> Set.empty (module String)
 *     in
 *     let s2 = match v2 with
 *       | Some k -> Layout.PredCtx.Key.params k
 *       | None -> Set.empty (module String)
 *     in
 *     Set.union s1 s2
 *   | TableT (_, _) *)
