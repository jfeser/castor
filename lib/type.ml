open Base

open Collections

type p = BoolT | IntT | StringT [@@deriving compare, sexp]
type t =
  | ScalarT of p
  | CrossTupleT of (t * int) list
  | ZipTupleT of ziptuple
  | OrderedListT of ordered_list
  | UnorderedListT of t
  | TableT of p * t
  | EmptyT
and ziptuple = { len : int; elems : t list }
and ordered_list = {
  field : Db.Field.t;
  order : [`Asc | `Desc];
  elems : t;
  lookup : Layout.range;
} [@@deriving compare, sexp]

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
  | (ZipTupleT { len = l1; elems = e1 },
     ZipTupleT { len = l2; elems = e2 }) when l1 = l2 -> begin
      match List.map2 e1 e2 ~f:unify_exn with
      | Ok ts -> ZipTupleT { len = l1; elems = ts }
      | Unequal_lengths -> raise UnifyError
    end
  | UnorderedListT et1, UnorderedListT et2 ->
    UnorderedListT (unify_exn et1 et2)
  | OrderedListT ({ field = f1; order = o1; lookup = r1; elems = e1 } as l1),
    OrderedListT ({ field = f2; order = o2; lookup = r2; elems = e2 } as l2)
    when Db.Field.(f1 = f2) &&
         Polymorphic_compare.(o1 = o2) &&
         Layout.compare_range r1 r2 = 0 ->
    OrderedListT { l1 with elems = unify_exn e1 e2 }
  | (TableT (pt1, et1), TableT (pt2, et2)) when compare_p pt1 pt2 = 0 ->
    TableT (pt1, unify_exn et1 et2)
  | (EmptyT, t) | (t, EmptyT) -> t
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
      | Some len -> ZipTupleT { len; elems }
      | None -> raise UnifyError
    end
  | UnorderedList ls ->
    let elem_t =
      List.map ls ~f:of_layout_exn
      |> List.fold_left ~f:unify_exn ~init:EmptyT
    in
    UnorderedListT elem_t
  | OrderedList { field; order; elems; lookup } ->
    let elem_t =
      List.map elems ~f:of_layout_exn
      |> List.fold_left ~f:unify_exn ~init:EmptyT
    in
    OrderedListT { field; order; elems = elem_t; lookup }
  | Table { field = { dtype }; elems } ->
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
    TableT (pt, t)
  | Empty -> EmptyT
