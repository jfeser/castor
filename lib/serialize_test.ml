open Core
open Base
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

module Eval = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (Eval)

module S =
  Serialize.Make (struct
      let layout_map = false
    end)
    (Eval)

[@@@warning "-8"]

let _, [f; _] =
  Test_util.create rels "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

[@@@warning "+8"]

let%expect_test "scalar-int" =
  let layout =
    of_string_exn "AScalar(1)" |> M.resolve |> M.annotate_schema
    |> M.annotate_key_layouts
  in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : Type.t * int * string] (type_, len, buf_str) |> print_s ;
  [%expect {|
    ((IntT ((range (1 1)) (nullable false))) 1 "\\001") |}]

let%expect_test "scalar-bool" =
  let layout =
    of_string_exn "AScalar(true)"
    |> M.resolve |> M.annotate_schema |> M.annotate_key_layouts
  in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : Type.t * int * string] (type_, len, buf_str) |> print_s ;
  [%expect {| ((BoolT ((nullable false))) 1 "\\001") |}]

let%expect_test "scalar-string" =
  let layout =
    of_string_exn "AScalar(\"test\")"
    |> M.resolve |> M.annotate_schema |> M.annotate_key_layouts
  in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : Type.t * int * string] (type_, len, buf_str) |> print_s ;
  [%expect
    {|
    ((StringT ((nchars (4 4)) (nullable false))) 8 "test\\000\\000\\000\\000") |}]

let%expect_test "tuple" =
  let layout =
    of_string_exn "ATuple([AScalar(1), AScalar(\"test\")], Cross)"
    |> M.resolve |> M.annotate_schema |> M.annotate_key_layouts
  in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : Type.t * int * string] (type_, len, buf_str) |> print_s ;
  [%expect
    {|
    ((TupleT
      (((IntT ((range (1 1)) (nullable false)))
        (StringT ((nchars (4 4)) (nullable false))))
       ((count ((1 1))))))
     17 "\\017\\000\\000\\000\\000\\000\\000\\000\\001test\\000\\000\\000\\000") |}]

let%expect_test "hash-idx" =
  let layout =
    of_string_exn "AHashIdx(Dedup(Select([r1.f], r1)) as k, AScalar(k.f), null)"
    |> M.resolve |> M.annotate_schema |> M.annotate_key_layouts
  in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : Type.t * int * string] (type_, len, buf_str) |> print_s ;
  [%expect
    {|
    ((HashIdxT
      ((IntT ((range (1 3)) (nullable false)))
       (IntT ((range (1 3)) (nullable false))) ((count ()))))
     150
     "\\150\\000\\000\\000\\000\\000\\000\\000h\\000\\000\\000\\000\\000\\000\\000\\b\\000\\000\\000$\\000\\000\\000\\n\\000\\000\\000\\b\\000\\000\\000\\001\\000\\000\\000\\016\\000\\000\\000\\005\\000\\000\\000\\b\\000\\000\\000T\\t\\000\\000\\002\\000\\000\\000^\\000\\000\\0008\\000\\000\\000\\007\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\011\\000\\000\\000\\001\\000\\000\\000\\001\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\016\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\146\\000\\000\\000\\000\\000\\000\\000\\144\\000\\000\\000\\000\\000\\000\\000\\148\\000\\000\\000\\000\\000\\000\\000\\003\\003\\001\\001\\002\\002") |}]

let%expect_test "ordered-idx" =
  let layout =
    of_string_exn
      "AOrderedIdx(OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc) as k, \
       AScalar(k.f), null, null)"
    |> M.resolve |> M.annotate_schema |> M.annotate_key_layouts
  in
  let type_ = M.to_type layout in
  [%sexp_of : Type.t] type_ |> print_s ;
  [%expect
    {|
    (OrderedIdxT
     ((IntT ((range (1 3)) (nullable false)))
      (IntT ((range (1 3)) (nullable false))) ((count ())))) |}] ;
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : int * string] (len, buf_str) |> print_s ;
  [%expect
    {|
    (43
     "+\\000\\000\\000\\000\\000\\000\\000#\\000\\000\\000\\000\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\000\\002\\001\\000\\000\\000\\000\\000\\000\\000\\003\\002\\000\\000\\000\\000\\000\\000\\000") |}]
