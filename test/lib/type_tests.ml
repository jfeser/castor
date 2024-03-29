open Type
open Collections
open Test_util

let%expect_test "byte-width-1" =
  [%sexp_of: int] (Abs_int.byte_width ~nullable:false (Interval (0, 149)))
  |> print_s;
  [%expect {| 2 |}]

let%expect_test "byte-width-2" =
  [%sexp_of: int]
    (Abs_int.byte_width ~nullable:false (Interval (1159859652, 1839958092)))
  |> print_s;
  [%expect {| 4 |}]

let%expect_test "absfixed-unify" =
  List.reduce_exn
    [
      Abs_fixed.(of_fixed { value = 3; scale = 1 });
      Abs_fixed.(of_fixed { value = 34; scale = 100 });
      Abs_fixed.(of_fixed { value = 7; scale = 1 });
      Abs_fixed.(of_fixed { value = 7999; scale = 10000 });
    ]
    ~f:Abs_fixed.join
  |> [%sexp_of: Abs_fixed.t] |> print_s;
  [%expect {| ((range (Interval 3400 70000)) (scale 10000)) |}]

let%expect_test "mult" =
  Abs_int.(Interval (0, 100) * Interval (0, 1000))
  |> [%sexp_of: Abs_int.t] |> print_s;
  [%expect {| (Interval 0 100000) |}]

let%expect_test "len-1" =
  ListT
    ( IntT { range = Interval (1, 1000); nullable = false },
      { count = Interval (0, 100) } )
  |> len |> [%sexp_of: Abs_int.t] |> print_s;
  [%expect {| (Interval 3 203) |}]
