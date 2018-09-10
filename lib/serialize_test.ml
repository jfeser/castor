open Core
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

module Eval = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (Eval)

let make_modules layout_file =
  let module S =
    Serialize.Make (struct
        let layout_map_channel = Some (Out_channel.create layout_file)
      end)
      (Eval)
  in
  (module S : Serialize.S)

[@@@warning "-8"]

let _, [f; _] =
  Test_util.create rels "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

[@@@warning "+8"]

let run_test layout_str =
  let layout_file = Filename.temp_file "layout" "txt" in
  let (module S) = make_modules layout_file in
  let layout =
    of_string_exn layout_str |> M.resolve |> M.annotate_schema
    |> M.annotate_key_layouts
  in
  let type_ = M.to_type layout in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) type_ layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  In_channel.input_all (In_channel.create layout_file) |> Stdio.print_endline ;
  [%sexp_of: Type.t * int * string] (type_, len, buf_str) |> print_s

let%expect_test "scalar-int" =
  run_test "AScalar(1)" ;
  [%expect
    {|
    0:1 Scalar (=(Int 1))

    ((IntT ((range (1 1)) (nullable false))) 1 "\\001") |}]

let%expect_test "scalar-bool" =
  run_test "AScalar(true)" ;
  [%expect
    {|
    0:1 Scalar (=(Bool true))

    ((BoolT ((nullable false))) 1 "\\001") |}]

let%expect_test "scalar-string" =
  run_test "AScalar(\"test\")" ;
  [%expect
    {|
    0:8 Scalar (=(String test))

    ((StringT ((nchars (4 4)) (nullable false))) 8 "test\\000\\000\\000\\000") |}]

let%expect_test "tuple" =
  run_test "ATuple([AScalar(1), AScalar(\"test\")], Cross)" ;
  [%expect
    {|
    0:0 Tuple len
    0:1 Scalar (=(Int 1))
    0:9 Tuple body
    1:8 Scalar (=(String test))

    ((TupleT
      (((IntT ((range (1 1)) (nullable false)))
        (StringT ((nchars (4 4)) (nullable false))))
       ((count ((1 1))))))
     9 "\\001test\\000\\000\\000\\000") |}]

let%expect_test "hash-idx" =
  run_test "AHashIdx(Dedup(Select([r1.f], r1)) as k, AScalar(k.f), null)" ;
  [%expect
    {|
    0:2 Table len
    2:8 Table hash len
    10:104 Table hash
    114:24 Table key map
    138:1 Scalar (=(Int 3))
    138:6 Table values
    139:1 Scalar (=(Int 3))
    140:1 Scalar (=(Int 1))
    141:1 Scalar (=(Int 1))
    142:1 Scalar (=(Int 2))
    143:1 Scalar (=(Int 2))

    ((HashIdxT
      ((IntT ((range (1 3)) (nullable false)))
       (IntT ((range (1 3)) (nullable false))) ((count ()))))
     144
     "\\144\\000h\\000\\000\\000\\000\\000\\000\\000\\b\\000\\000\\000$\\000\\000\\000\\t\\000\\000\\000\\b\\000\\000\\000\\001\\000\\000\\000\\016\\000\\000\\000\\004\\000\\000\\000\\b\\000\\000\\000J\\002\\000\\000\\001\\000\\000\\000\\169\\000\\000\\0008\\000\\000\\000\\007\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\011\\000\\000\\000\\001\\000\\000\\000\\001\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\016\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\142\\000\\000\\000\\000\\000\\000\\000\\138\\000\\000\\000\\000\\000\\000\\000\\140\\000\\000\\000\\000\\000\\000\\000\\003\\003\\001\\001\\002\\002") |}]

let%expect_test "ordered-idx" =
  run_test
    "AOrderedIdx(OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc) as k, \
     AScalar(k.f), null, null)" ;
  [%expect
    {|
    0:2 Ordered idx len (=40)
    2:8 Ordered idx index len (=27)
    10:1 Scalar (=(Int 1))
    10:1 Ordered idx key
    10:1 Scalar (=(Int 1))
    10:1 Ordered idx key
    11:8 Ordered idx value ptr (=37)
    19:1 Scalar (=(Int 2))
    19:1 Ordered idx key
    19:1 Scalar (=(Int 2))
    19:1 Ordered idx key
    20:8 Ordered idx value ptr (=38)
    28:1 Scalar (=(Int 3))
    28:1 Ordered idx key
    28:1 Scalar (=(Int 3))
    28:1 Ordered idx key
    29:8 Ordered idx value ptr (=39)
    37:1 Scalar (=(Int 1))
    38:1 Scalar (=(Int 2))
    39:1 Scalar (=(Int 3))

    ((OrderedIdxT
      ((IntT ((range (1 3)) (nullable false)))
       (IntT ((range (1 3)) (nullable false))) ((count ()))))
     40
     "(\\000\\027\\000\\000\\000\\000\\000\\000\\000\\001%\\000\\000\\000\\000\\000\\000\\000\\002&\\000\\000\\000\\000\\000\\000\\000\\003'\\000\\000\\000\\000\\000\\000\\000\\001\\002\\003") |}]

(* let tests =
 *   let open OUnit2 in
 *   "serialize"
 *   >::: [ ( "to-byte"
 *          >:: fun ctxt ->
 *          let x = 0xABCDEF01 in
 *          assert_equal ~ctxt x (bytes_of_int ~width:64 x |> int_of_bytes_exn) )
 *        ; ( "from-byte"
 *          >:: fun ctxt ->
 *          let b = Bytes.of_string "\031\012\000\000" in
 *          let x = 3103 in
 *          assert_equal ~ctxt ~printer:Caml.string_of_int x (int_of_bytes_exn b) )
 *        ; ( "align"
 *          >:: fun ctxt ->
 *          let b = Bytes.of_string "\001\002\003" in
 *          let b' = align 8 b in
 *          assert_equal ~ctxt ~printer:Caml.string_of_int 8 (String.length b') ;
 *          assert_equal ~ctxt "\001\002\003\000\000\000\000\000" b' ) ] *)
