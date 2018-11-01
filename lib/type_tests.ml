open Core
open Type
open Collections

let%expect_test "byte-width-1" =
  [%sexp_of: int] (AbsInt.byte_width ~nullable:false (0, 149)) |> print_s ;
  [%expect {| 2 |}]

let%expect_test "byte-width-2" =
  [%sexp_of: int] (AbsInt.byte_width ~nullable:false (1159859652, 1839958092))
  |> print_s ;
  [%expect {| 4 |}]

let%expect_test "absfixed-unify" =
  List.fold_left1
    [ AbsFixed.(of_fixed {value= 3; scale= 1})
    ; AbsFixed.(of_fixed {value= 34; scale= 100})
    ; AbsFixed.(of_fixed {value= 7; scale= 1})
    ; AbsFixed.(of_fixed {value= 7999; scale= 10000}) ]
    ~f:AbsFixed.unify
  |> [%sexp_of: AbsFixed.t] |> print_s;
  [%expect {| ((range (3400 70000)) (scale 10000)) |}]

let%expect_test "mult" =
  AbsInt.((0, 100) * (0, 1000)) |> [%sexp_of: AbsInt.t] |> print_s ;
  [%expect {| (0 100000) |}]

let%expect_test "len-1" =
  ListT (IntT {range= (1, 1000); nullable= false}, {count= (0, 100)})
  |> len |> [%sexp_of: AbsInt.t] |> print_s ;
  [%expect {| (3 203) |}]
