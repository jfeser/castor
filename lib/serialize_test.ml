open Core
open Abslayout
open Test_util

(** Remove nondeterministic parts of the layout log. Returns the new log and
   true if the log was modified, false otherwise. *)
let process_layout_log log =
  let map_entry_regex = Str.regexp {|Map entry (\([0-9]+\) => [0-9]+)|} in
  let log' = Str.global_replace map_entry_regex {|Map entry (\1 => XXX)|} log in
  (log', not String.(log = log'))

let run_test layout_str =
  let layout_file = Filename.temp_file "layout" "txt" in
  let (module M), (module S), _, _ = make_modules ~layout_file () in
  let layout =
    of_string_exn layout_str |> M.annotate_relations |> M.resolve
    |> annotate_key_layouts
  in
  M.annotate_type layout ;
  let type_ = Meta.(find_exn layout type_) in
  let buf = Buffer.create 1024 in
  let _, len = S.serialize (Bitstring.Writer.with_buffer buf) layout in
  let buf_str = Buffer.contents buf |> String.escaped in
  let layout_log, did_modify =
    In_channel.input_all (In_channel.create layout_file) |> process_layout_log
  in
  Stdio.print_endline layout_log ;
  if did_modify then [%sexp_of: Type.t * int] (type_, len) |> print_s
  else [%sexp_of: Type.t * int * string] (type_, len, buf_str) |> print_s

let%expect_test "scalar-int" =
  run_test "AScalar(1)" ;
  [%expect
    {|
    0:1 Scalar (=(Int 1))

    ((IntT ((range (Interval 1 1)) (nullable false))) 1 "\\001") |}]

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

    ((StringT ((nchars (Interval 4 4)) (nullable false))) 4 test) |}]

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
      (((IntT ((range (Interval 1 1)) (nullable false)))
        (StringT ((nchars (Interval 4 4)) (nullable false))))
       ((count (Interval 1 1)))))
     5 "\\001test") |}]

let%expect_test "hash-idx" =
  run_test "AHashIdx(Dedup(Select([f], r1)) as k, AScalar(k.f as v), null)" ;
  [%expect
    {|
    0:4 Table len
    4:8 Table map len
    4:0 Table hash len
    4:0 Table hash
    12:32 Table key map
    12:8 Map entry (0 => XXX)
    20:8 Map entry (1 => XXX)
    28:8 Map entry (2 => XXX)
    36:8 Map entry (3 => XXX)
    44:6 Table values
    44:1 Scalar (=(Int 1))
    45:1 Scalar (=(Int 1))
    46:1 Scalar (=(Int 2))
    47:1 Scalar (=(Int 2))
    48:1 Scalar (=(Int 3))
    49:1 Scalar (=(Int 3))

    ((HashIdxT
      ((IntT ((range (Interval 1 3)) (nullable false)))
       (IntT ((range (Interval 1 3)) (nullable false)))
       ((key_count (Interval 3 3)) (value_count (Interval 1 1)))))
     50) |}]

let%expect_test "ordered-idx" =
  run_test
    "AOrderedIdx(OrderBy([f desc], Dedup(Select([f], r1))) as k, AScalar(k.f as \
     v), null, null)" ;
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
      ((IntT ((range (Interval 1 3)) (nullable false)))
       (IntT ((range (Interval 1 3)) (nullable false))) ((count Top))))
     42
     "*\\000\\000\\000\\027\\000\\000\\000\\000\\000\\000\\000\\001'\\000\\000\\000\\000\\000\\000\\000\\002(\\000\\000\\000\\000\\000\\000\\000\\003)\\000\\000\\000\\000\\000\\000\\000\\001\\002\\003") |}]

let%expect_test "ordered-idx-dates" =
  run_test
    "AOrderedIdx(OrderBy([f desc], Dedup(Select([f], r_date))) as k, AScalar(k.f \
     as v), null, null)" ;
  [%expect
    {|
    0:4 Ordered idx len (=72)
    4:8 Ordered idx index len (=50)
    12:2 Scalar (=(Date 2016-12-01))
    12:2 Scalar (=(Date 2016-12-01))
    12:2 Ordered idx key
    14:8 Ordered idx value ptr (=62)
    22:2 Scalar (=(Date 2017-10-05))
    22:2 Scalar (=(Date 2017-10-05))
    22:2 Ordered idx key
    24:8 Ordered idx value ptr (=64)
    32:2 Scalar (=(Date 2018-01-01))
    32:2 Scalar (=(Date 2018-01-01))
    32:2 Ordered idx key
    34:8 Ordered idx value ptr (=66)
    42:2 Scalar (=(Date 2018-01-23))
    42:2 Scalar (=(Date 2018-01-23))
    42:2 Ordered idx key
    44:8 Ordered idx value ptr (=68)
    52:2 Scalar (=(Date 2018-09-01))
    52:2 Scalar (=(Date 2018-09-01))
    52:2 Ordered idx key
    54:8 Ordered idx value ptr (=70)
    62:2 Scalar (=(Date 2016-12-01))
    64:2 Scalar (=(Date 2017-10-05))
    66:2 Scalar (=(Date 2018-01-01))
    68:2 Scalar (=(Date 2018-01-23))
    70:2 Scalar (=(Date 2018-09-01))

    ((OrderedIdxT
      ((DateT ((range (Interval 17136 17775)) (nullable false)))
       (DateT ((range (Interval 17136 17775)) (nullable false))) ((count Top))))
     72
     "H\\000\\000\\0002\\000\\000\\000\\000\\000\\000\\000\\240B>\\000\\000\\000\\000\\000\\000\\000$D@\\000\\000\\000\\000\\000\\000\\000|DB\\000\\000\\000\\000\\000\\000\\000\\146DD\\000\\000\\000\\000\\000\\000\\000oEF\\000\\000\\000\\000\\000\\000\\000\\240B$D|D\\146DoE") |}]

let%expect_test "list-list" =
  run_test "AList(r1 as k1, AList(r1 as k2, AScalar(k2.f)))" ;
  [%expect
    {|
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
      ((ListT
        ((IntT ((range (Interval 1 3)) (nullable false)))
         ((count (Interval 5 5)))))
       ((count (Interval 5 5)))))
     25
     "\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003") |}]
