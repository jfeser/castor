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
  annotate_foreach layout ;
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
    0:4 String body
    0:4 Scalar (=(String test))
    0:0 String length (=4)

    ((StringT ((nchars (4 4)) (nullable false))) 4 test) |}]

let%expect_test "tuple" =
  run_test "ATuple([AScalar(1), AScalar(\"test\")], Cross)" ;
  [%expect
    {|
    0:5 Tuple body
    0:1 Scalar (=(Int 1))
    0:0 Tuple len (=5)
    1:4 String body
    1:4 Scalar (=(String test))
    1:0 String length (=4)

    ((TupleT
      (((IntT ((range (1 1)) (nullable false)))
        (StringT ((nchars (4 4)) (nullable false))))
       ((count ((1 1))))))
     5 "\\001test") |}]

let%expect_test "hash-idx" =
  run_test "AHashIdx(Dedup(Select([r1.f], r1)) as k, AScalar(k.f), null)" ;
  [%expect
    {|
    0:4 Table len
    4:8 Table hash len
    12:104 Table hash
    116:8 Table map len
    124:24 Table key map
    124:8 Map entry (0 => 150)
    132:8 Map entry (1 => 152)
    140:8 Map entry (2 => 148)
    148:6 Table values
    148:1 Scalar (=(Int 1))
    149:1 Scalar (=(Int 1))
    150:1 Scalar (=(Int 2))
    151:1 Scalar (=(Int 2))
    152:1 Scalar (=(Int 3))
    153:1 Scalar (=(Int 3))

    ((HashIdxT
      ((IntT ((range (1 3)) (nullable false)))
       (IntT ((range (1 3)) (nullable false))) ((count ()))))
     154
     "\\154\\000\\000\\000h\\000\\000\\000\\000\\000\\000\\000\\b\\000\\000\\000$\\000\\000\\000\\t\\000\\000\\000\\b\\000\\000\\000\\001\\000\\000\\000\\016\\000\\000\\000\\004\\000\\000\\000\\b\\000\\000\\000J\\002\\000\\000\\001\\000\\000\\000\\169\\000\\000\\0008\\000\\000\\000\\007\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\011\\000\\000\\000\\001\\000\\000\\000\\001\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\016\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\024\\000\\000\\000\\000\\000\\000\\000\\150\\000\\000\\000\\000\\000\\000\\000\\152\\000\\000\\000\\000\\000\\000\\000\\148\\000\\000\\000\\000\\000\\000\\000\\001\\001\\002\\002\\003\\003") |}]

let%expect_test "ordered-idx" =
  run_test
    "AOrderedIdx(OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc) as k, \
     AScalar(k.f), null, null)" ;
  [%expect
    {|
    0:4 Ordered idx len (=42)
    4:8 Ordered idx index len (=27)
    12:1 Scalar (=(Int 1))
    12:1 Scalar (=(Int 1))
    12:1 Ordered idx key
    13:8 Ordered idx value ptr (=39)
    21:1 Scalar (=(Int 2))
    21:1 Scalar (=(Int 2))
    21:1 Ordered idx key
    22:8 Ordered idx value ptr (=40)
    30:1 Scalar (=(Int 3))
    30:1 Scalar (=(Int 3))
    30:1 Ordered idx key
    31:8 Ordered idx value ptr (=41)
    39:1 Scalar (=(Int 1))
    40:1 Scalar (=(Int 2))
    41:1 Scalar (=(Int 3))

    ((OrderedIdxT
      ((IntT ((range (1 3)) (nullable false)))
       (IntT ((range (1 3)) (nullable false))) ((count ()))))
     42
     "*\\000\\000\\000\\027\\000\\000\\000\\000\\000\\000\\000\\001'\\000\\000\\000\\000\\000\\000\\000\\002(\\000\\000\\000\\000\\000\\000\\000\\003)\\000\\000\\000\\000\\000\\000\\000\\001\\002\\003") |}]

let%expect_test "list-list" = run_test "AList(r1, AList(r1, AScalar(r1.f)))";
  [%expect {|
    0:25 List body
    0:5 List body
    0:1 Scalar (=(Int 1))
    0:0 List count (=5)
    0:0 List len (=5)
    0:0 List count (=5)
    0:0 List len (=25)
    1:1 Scalar (=(Int 1))
    2:1 Scalar (=(Int 2))
    3:1 Scalar (=(Int 2))
    4:1 Scalar (=(Int 3))
    5:5 List body
    5:1 Scalar (=(Int 1))
    5:0 List count (=5)
    5:0 List len (=5)
    6:1 Scalar (=(Int 1))
    7:1 Scalar (=(Int 2))
    8:1 Scalar (=(Int 2))
    9:1 Scalar (=(Int 3))
    10:5 List body
    10:1 Scalar (=(Int 1))
    10:0 List count (=5)
    10:0 List len (=5)
    11:1 Scalar (=(Int 1))
    12:1 Scalar (=(Int 2))
    13:1 Scalar (=(Int 2))
    14:1 Scalar (=(Int 3))
    15:5 List body
    15:1 Scalar (=(Int 1))
    15:0 List count (=5)
    15:0 List len (=5)
    16:1 Scalar (=(Int 1))
    17:1 Scalar (=(Int 2))
    18:1 Scalar (=(Int 2))
    19:1 Scalar (=(Int 3))
    20:5 List body
    20:1 Scalar (=(Int 1))
    20:0 List count (=5)
    20:0 List len (=5)
    21:1 Scalar (=(Int 1))
    22:1 Scalar (=(Int 2))
    23:1 Scalar (=(Int 2))
    24:1 Scalar (=(Int 3))

    ((ListT
      ((ListT ((IntT ((range (1 3)) (nullable false))) ((count (5 5)))))
       ((count (5 5)))))
     25
     "\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003") |}]

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
