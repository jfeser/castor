open Abslayout
open Sql
open Test_util
module M = Abslayout_db.Make (Test_db)

let make_module_db () =
  let module A = Abslayout_db.Make (struct
    let conn = create_db "postgresql://localhost:5433/demomatch"
  end) in
  (module A : Abslayout_db.S)

let of_string_exn s =
  let r = of_string_exn s in
  M.annotate_schema r ; r

let%expect_test "select-agg" =
  let r = of_string_exn "select([(0.2 * avg(r.r)) as test], r)" in
  print_endline (of_ralgebra r |> to_sql) ;
  [%expect {| select (0.2) * (avg(r."r")) as test from r |}]

let%expect_test "project" =
  let r = of_string_exn "Select([r.r], r)" in
  print_endline (of_ralgebra r |> to_sql) ;
  [%expect {| select r."r" from r |}]

let%expect_test "filter" =
  let r = of_string_exn "Filter(r.f = r.g, r)" in
  print_endline (of_ralgebra r |> to_sql) ;
  [%expect {| select * from r where (r."f") = (r."g") |}]

let%expect_test "eqjoin" =
  let r = of_string_exn "Join(r.f = s.g, r as r, s as s)" in
  print_endline (of_ralgebra r |> to_sql) ;
  [%expect {| select * from r as t3, s as t4 where (t3."f") = (t4."g") |}]
