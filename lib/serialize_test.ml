open Core
open Base
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

let create name fs xs =
  let rel = Db.Relation.of_name name in
  let data =
    List.map xs ~f:(fun data ->
        List.map2_exn fs data ~f:(fun fname value -> (fname, `Int value)) )
  in
  Hashtbl.set rels ~key:rel ~data ;
  ( name
  , List.map fs ~f:(fun f ->
        Name.{name= f; relation= Some name; type_= Some Type0.PrimType.IntT} ) )

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

let _, [f; _] = create "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

[@@@warning "+8"]

let%expect_test "scalar-int" =
  let layout = of_string_exn "AScalar(1)" in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : Type.t * int * string] (type_, len, buf_str) |> print_s

let%expect_test "scalar-bool" =
  let layout = of_string_exn "AScalar(true)" in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : Type.t * int * string] (type_, len, buf_str) |> print_s

let%expect_test "scalar-string" =
  let layout = of_string_exn "AScalar(\"test\")" in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : Type.t * int * string] (type_, len, buf_str) |> print_s

let%expect_test "tuple" =
  let layout = of_string_exn "ATuple([AScalar(1), AScalar(\"test\")], Cross)" in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : Type.t * int * string] (type_, len, buf_str) |> print_s

let%expect_test "hash-idx" =
  let layout =
    of_string_exn "AHashIdx(Dedup(Select([r1.f], r1)) as k, AScalar(k.f), null)"
  in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : Type.t * int * string] (type_, len, buf_str) |> print_s

let%expect_test "ordered-idx" =
  let layout =
    of_string_exn
      "AOrderedIdx(OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc) as k, \
       AScalar(k.f), null, null)"
    |> M.resolve |> M.annotate_schema
  in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  [%sexp_of : Type.t * int * string] (type_, len, buf_str) |> print_s
