open Type
open Collections

let%expect_test "byte-width-1" =
  [%sexp_of: int] (AbsInt.byte_width ~nullable:false (Interval (0, 149)))
  |> print_s;
  [%expect {| 2 |}]

let%expect_test "byte-width-2" =
  [%sexp_of: int]
    (AbsInt.byte_width ~nullable:false (Interval (1159859652, 1839958092)))
  |> print_s;
  [%expect {| 4 |}]

let%expect_test "absfixed-unify" =
  List.reduce_exn
    [
      AbsFixed.(of_fixed { value = 3; scale = 1 });
      AbsFixed.(of_fixed { value = 34; scale = 100 });
      AbsFixed.(of_fixed { value = 7; scale = 1 });
      AbsFixed.(of_fixed { value = 7999; scale = 10000 });
    ]
    ~f:AbsFixed.join
  |> [%sexp_of: AbsFixed.t] |> print_s;
  [%expect {| ((range (Interval 3400 70000)) (scale 10000)) |}]

let%expect_test "mult" =
  AbsInt.(Interval (0, 100) * Interval (0, 1000))
  |> [%sexp_of: AbsInt.t] |> print_s;
  [%expect {| (Interval 0 100000) |}]

let%expect_test "len-1" =
  ListT
    ( IntT { range = Interval (1, 1000); nullable = false },
      { count = Interval (0, 100) } )
  |> len |> [%sexp_of: AbsInt.t] |> print_s;
  [%expect {| (Interval 3 203) |}]

let type_test conn q =
  let open Lwt in
  let p =
    let%lwt type_ = Type.Parallel.type_of conn q in
    [%sexp_of: Type.t] type_ |> print_s;
    return_unit
  in
  Lwt_main.run p

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in
  let q =
    "alist(select([f], r1) as k, ascalar(k.f))"
    |> Abslayout_load.load_string conn
  in
  type_test conn q;
  [%expect
    {|
    (ListT ((IntT ((range (Interval 1 3)))) ((count (Interval 5 5))))) |}]

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in
  let q =
    "ahashidx(select([f], r1) as k, ascalar(k.f), 0)"
    |> Abslayout_load.load_string conn
  in
  type_test conn q;
  [%expect
    {|
    (HashIdxT
     ((IntT ((range (Interval 1 3)))) (IntT ((range (Interval 1 3))))
      ((key_count (Interval 5 5))))) |}]

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in
  let q =
    "ahashidx(select([f], r1) as k, alist(select([g], filter(k.f = f, r1)) as \
     k1, ascalar(k1.g)), 0)"
    |> Abslayout_load.load_string conn
  in
  type_test conn q;
  [%expect
    {|
    (HashIdxT
     ((IntT ((range (Interval 1 3))))
      (ListT ((IntT ((range (Interval 1 4)))) ((count (Interval 1 1)))))
      ((key_count (Interval 5 5))))) |}]
