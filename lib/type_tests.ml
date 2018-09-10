open Core
open Type

let%expect_test "byte-width-1" =
  [%sexp_of: int] (AbsInt.byte_width ~nullable:false (0, 149)) |> print_s;
  [%expect {| 2 |}]

let%expect_test "mult" =
  AbsInt.((0, 100) * (0, 1000)) |> [%sexp_of: AbsInt.t] |> print_s;
  [%expect {| (0 100000) |}]

let%expect_test "len-1" =
  ListT (IntT {range= (1, 1000); nullable= false}, {count= (0, 100)})
  |> len |> [%sexp_of: AbsInt.t] |> print_s;
  [%expect {| (3 203) |}]
