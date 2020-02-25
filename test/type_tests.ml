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

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in
  let q =
    "alist(select([f], r1) as k, ascalar(k.f))"
    |> Abslayout_load.load_string conn
  in
  let open Lwt in
  let p =
    let%lwt type_ = Type.agg_type_of conn q in
    [%sexp_of: Type.t] type_ |> print_s;
    return_unit
  in
  Lwt_main.run p;
  [%expect {|
    groupby([min((groupby([count() as c], [], select([f], r1)))) as x0,
             max((groupby([count() as c], [], select([f], r1)))) as x1],
      [],
      ascalar(0))
    select  min((select  count(*) as "c_0" from  (select  r1_10."f" as "f_1" from  "r1" as "r1_10") as "t29")) as "x0_3", max((select  count(*) as "c_1" from  (select  r1_11."f" as "f_3" from  "r1" as "r1_11") as "t30")) as "x1_3" from  (select  0 as "a0_0") as "t28"


    groupby([min(k_f) as x2, max(k_f) as x3],
      [],
      depjoin(select([f], r1) as k, select([k.f as k_f], ascalar(0))))
    select  min("k_f_0_0") as "x2_3", max("k_f_0_0") as "x3_3" from  (select  "k_f_0" as "k_f_0_0" from  (select  r1_12."f" as "f_5" from  "r1" as "r1_12") as "t32", lateral (select  "f_5" as "k_f_0") as "t31") as "t33"


    (ListT ((IntT ((range (Interval 1 3)))) ((count (Interval 5 5))))) |}]
