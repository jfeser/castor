open Test_util

(** Remove nondeterministic parts of the layout log. Returns the new log and
   true if the log was modified, false otherwise. *)
let process_layout_log log =
  let map_entry_regex = Str.regexp {|Map entry (\([0-9]+\) => [0-9]+)|} in
  let log' = Str.global_replace map_entry_regex {|Map entry (\1 => XXX)|} log in
  (log', not String.(log = log'))

let run_test ?(params = []) layout_str =
  let open Abslayout_load in
  let conn = Lazy.force test_db_conn in

  let layout_file = Filename_unix.temp_file "layout" "bin" in
  let layout_log_file = Filename_unix.temp_file "layout" "txt" in
  let layout =
    let params =
      List.map params ~f:(fun (n, t, _) -> Name.{ n with type_ = Some t })
      |> Set.of_list (module Name)
    in
    load_string_exn ~params conn layout_str
    |> Equiv.annotate
    |> Abslayout_fold.Data.annotate conn
    |> Type.annotate
  in
  let type_ = layout.meta#type_ in
  let _, len =
    Serialize.serialize ~layout_file:layout_log_file layout_file layout
  in
  let buf_str = In_channel.read_all layout_file |> String.escaped in
  let layout_log, did_modify =
    In_channel.input_all (In_channel.create layout_log_file)
    |> process_layout_log
  in
  print_endline layout_log;
  if did_modify then [%sexp_of: Type.t * int] (type_, len) |> print_s
  else [%sexp_of: Type.t * int * string] (type_, len, buf_str) |> print_s

let%expect_test "scalar-int" =
  run_test "AScalar(1 as x)";
  [%expect
    {|
       0:1 Scalar (=(Int 1))

       ((IntT ((range (Interval 1 1)))) 1 "\\001") |}]

let%expect_test "scalar-bool" =
  run_test "AScalar(true as x)";
  [%expect
    {|
       0:1 Scalar (=(Bool true))

       ((BoolT ()) 1 "\\001") |}]

let%expect_test "scalar-string" =
  run_test "AScalar(\"test\" as x)";
  [%expect
    {|
       0:4 Scalar (=(String test))
       0:4 String body
       0:0 String length (=4)

       ((StringT ((nchars (Interval 4 4)))) 4 test) |}]

let%expect_test "tuple" =
  run_test "ATuple([AScalar(1 as x), AScalar(\"test\" as y)], Cross)";
  [%expect
    {|
       0:5 Tuple body
       0:1 Scalar (=(Int 1))
       0:0 Tuple len (=5)
       1:4 Scalar (=(String test))
       1:4 String body
       1:0 String length (=4)

       ((TupleT
         (((IntT ((range (Interval 1 1)))) (StringT ((nchars (Interval 4 4)))))
          ((kind Cross))))
        5 "\\001test") |}]

let%expect_test "hash-idx" =
  run_test "AHashIdx(Dedup(Select([f], r1)), AScalar(0.f as v), null)";
  [%expect
    {|
       0:1 Table len (=12)
       1:1 Table map len (=4)
       1:0 Table hash
       1:0 Table hash len (=0)
       2:4 Table key map
       2:1 Map entry (0 => XXX)
       3:1 Map entry (1 => XXX)
       4:1 Map entry (2 => XXX)
       5:1 Map entry (3 => XXX)
       6:6 Table values
       6:1 Scalar (=(Int 1))
       7:1 Scalar (=(Int 1))
       8:1 Scalar (=(Int 2))
       9:1 Scalar (=(Int 2))
       10:1 Scalar (=(Int 3))
       11:1 Scalar (=(Int 3))

       ((HashIdxT
         ((IntT ((range (Interval 1 3)))) (IntT ((range (Interval 1 3))))
          ((key_count (Interval 3 3)))))
        12) |}]

let%expect_test "ordered-idx" =
  run_test
    "AOrderedIdx(OrderBy([f desc], Dedup(Select([f], r1))), AScalar(0.f as v), \
     null, null)";
  [%expect
    {|
       0:6 Ordered idx map
       0:1 Ordered idx key
       0:1 Scalar (=(Int 3))
       0:0 Ordered idx index len (=6)
       0:0 Ordered idx len (=9)
       1:1 Ordered idx ptr (=0)
       2:1 Ordered idx key
       2:1 Scalar (=(Int 2))
       3:1 Ordered idx ptr (=1)
       4:1 Ordered idx key
       4:1 Scalar (=(Int 1))
       5:1 Ordered idx ptr (=2)
       6:3 Ordered idx body
       6:1 Scalar (=(Int 3))
       7:1 Scalar (=(Int 2))
       8:1 Scalar (=(Int 1))

       ((OrderedIdxT
         ((IntT ((range (Interval 1 3)))) (IntT ((range (Interval 1 3))))
          ((key_count (Interval 3 3)))))
        9 "\\003\\000\\002\\001\\001\\002\\003\\002\\001") |}]

let%expect_test "ordered-idx-dates" =
  run_test
    "AOrderedIdx(OrderBy([f desc], Dedup(Select([f], r_date))), AScalar(0.f as \
     v), null, null)";
  [%expect
    {|
       0:15 Ordered idx map
       0:2 Ordered idx key
       0:2 Scalar (=(Date 2018-09-01))
       0:0 Ordered idx index len (=15)
       0:0 Ordered idx len (=25)
       2:1 Ordered idx ptr (=0)
       3:2 Ordered idx key
       3:2 Scalar (=(Date 2018-01-23))
       5:1 Ordered idx ptr (=2)
       6:2 Ordered idx key
       6:2 Scalar (=(Date 2018-01-01))
       8:1 Ordered idx ptr (=4)
       9:2 Ordered idx key
       9:2 Scalar (=(Date 2017-10-05))
       11:1 Ordered idx ptr (=6)
       12:2 Ordered idx key
       12:2 Scalar (=(Date 2016-12-01))
       14:1 Ordered idx ptr (=8)
       15:10 Ordered idx body
       15:2 Scalar (=(Date 2018-09-01))
       17:2 Scalar (=(Date 2018-01-23))
       19:2 Scalar (=(Date 2018-01-01))
       21:2 Scalar (=(Date 2017-10-05))
       23:2 Scalar (=(Date 2016-12-01))

       ((OrderedIdxT
         ((DateT ((range (Interval 17136 17775))))
          (DateT ((range (Interval 17136 17775)))) ((key_count (Interval 5 5)))))
        25 "oE\\000\\146D\\002|D\\004$D\\006\\240B\\boE\\146D|D$D\\240B") |}]

let%expect_test "list-list" =
  run_test "AList(r1, AList(r1, AScalar(0.f)))";
  [%expect
    {|
       0:25 List body
       0:5 List body
       0:1 Scalar (=(Int 1))
       0:0 List len (=25)
       0:0 List count (=5)
       0:0 List len (=5)
       0:0 List count (=5)
       1:1 Scalar (=(Int 1))
       2:1 Scalar (=(Int 2))
       3:1 Scalar (=(Int 2))
       4:1 Scalar (=(Int 3))
       5:5 List body
       5:1 Scalar (=(Int 1))
       5:0 List len (=5)
       5:0 List count (=5)
       6:1 Scalar (=(Int 1))
       7:1 Scalar (=(Int 2))
       8:1 Scalar (=(Int 2))
       9:1 Scalar (=(Int 3))
       10:5 List body
       10:1 Scalar (=(Int 1))
       10:0 List len (=5)
       10:0 List count (=5)
       11:1 Scalar (=(Int 1))
       12:1 Scalar (=(Int 2))
       13:1 Scalar (=(Int 2))
       14:1 Scalar (=(Int 3))
       15:5 List body
       15:1 Scalar (=(Int 1))
       15:0 List len (=5)
       15:0 List count (=5)
       16:1 Scalar (=(Int 1))
       17:1 Scalar (=(Int 2))
       18:1 Scalar (=(Int 2))
       19:1 Scalar (=(Int 3))
       20:5 List body
       20:1 Scalar (=(Int 1))
       20:0 List len (=5)
       20:0 List count (=5)
       21:1 Scalar (=(Int 1))
       22:1 Scalar (=(Int 2))
       23:1 Scalar (=(Int 2))
       24:1 Scalar (=(Int 3))

       ((ListT
         ((ListT ((IntT ((range (Interval 1 3)))) ((count (Interval 5 5)))))
          ((count (Interval 5 5)))))
        25
        "\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003\\001\\001\\002\\002\\003") |}]

let%expect_test "depjoin" =
  run_test "depjoin(alist(r1, ascalar(0.f)), ascalar(5 as x))";
  [%expect
    {|
       0:6 Tuple body
       0:5 List body
       0:1 Scalar (=(Int 1))
       0:0 Tuple len (=6)
       0:0 List len (=5)
       0:0 List count (=5)
       1:1 Scalar (=(Int 1))
       2:1 Scalar (=(Int 2))
       3:1 Scalar (=(Int 2))
       4:1 Scalar (=(Int 3))
       5:1 Scalar (=(Int 5))

       ((FuncT
         (((ListT ((IntT ((range (Interval 1 3)))) ((count (Interval 5 5)))))
           (IntT ((range (Interval 5 5)))))
          Child_sum))
        6 "\\001\\001\\002\\002\\003\\005") |}]

let%expect_test "depjoin" =
  run_test
    {|
   atuple([
       alist(r, ascalar(0.f)),
       alist(r2, ascalar(0.a))
   ], cross)
   |};
  [%expect
    {|
       0:17 Tuple body
       0:7 List body
       0:1 Scalar (=(Int 0))
       0:0 Tuple len (=17)
       0:0 List len (=7)
       0:0 List count (=7)
       1:1 Scalar (=(Int 1))
       2:1 Scalar (=(Int 1))
       3:1 Scalar (=(Int 2))
       4:1 Scalar (=(Int 2))
       5:1 Scalar (=(Int 3))
       6:1 Scalar (=(Int 4))
       7:10 List body
       7:2 Scalar (=(Fixed ((value -42) (scale 100))))
       7:0 List len (=10)
       7:0 List count (=5)
       9:2 Scalar (=(Fixed ((value 1) (scale 100))))
       11:2 Scalar (=(Fixed ((value 88) (scale 100))))
       13:2 Scalar (=(Fixed ((value 5) (scale 1))))
       15:2 Scalar (=(Fixed ((value 3442) (scale 100))))

       ((TupleT
         (((ListT ((IntT ((range (Interval 0 4)))) ((count (Interval 7 7)))))
           (ListT
            ((FixedT ((value ((range (Interval -42 3442)) (scale 100)))))
             ((count (Interval 5 5))))))
          ((kind Cross))))
        17
        "\\000\\001\\001\\002\\002\\003\\004\\214\\255\\001\\000X\\000\\244\\001r\\r") |}]
