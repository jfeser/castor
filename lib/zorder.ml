open! Core

let less_msb x y = x < y && x < x lxor y

let compare lhs rhs =
  let msd = ref 0 in
  for i = 1 to Array.length lhs - 1 do
    if less_msb (lhs.(!msd) lxor rhs.(!msd)) (lhs.(i) lxor rhs.(i)) then msd := i
  done ;
  Int.compare lhs.(!msd) rhs.(!msd)

let sort tups = List.sort tups ~compare

let%expect_test "" =
  sort (List.init 7 ~f:(fun i -> List.init 7 ~f:(fun j -> [|i; j|])) |> List.concat)
  |> [%sexp_of: int array list] |> print_s ;
  [%expect
    {|
    ((0 0) (0 1) (1 0) (1 1) (0 2) (0 3) (1 2) (1 3) (2 0) (2 1) (3 0) (3 1)
     (2 2) (2 3) (3 2) (3 3) (0 4) (0 5) (1 4) (1 5) (0 6) (1 6) (2 4) (2 5)
     (3 4) (3 5) (2 6) (3 6) (4 0) (4 1) (5 0) (5 1) (4 2) (4 3) (5 2) (5 3)
     (6 0) (6 1) (6 2) (6 3) (4 4) (4 5) (5 4) (5 5) (4 6) (5 6) (6 4) (6 5)
     (6 6)) |}]

let%expect_test "" =
  sort (List.init 7 ~f:(fun i -> List.init 7 ~f:(fun _ -> [|i; i|])) |> List.concat)
  |> [%sexp_of: int array list] |> print_s ;
  [%expect
    {|
    ((0 0) (0 0) (0 0) (0 0) (0 0) (0 0) (0 0) (1 1) (1 1) (1 1) (1 1) (1 1)
     (1 1) (1 1) (2 2) (2 2) (2 2) (2 2) (2 2) (2 2) (2 2) (3 3) (3 3) (3 3)
     (3 3) (3 3) (3 3) (3 3) (4 4) (4 4) (4 4) (4 4) (4 4) (4 4) (4 4) (5 5)
     (5 5) (5 5) (5 5) (5 5) (5 5) (5 5) (6 6) (6 6) (6 6) (6 6) (6 6) (6 6)
     (6 6)) |}]
