open Collections
open Abslayout
open Sql
open Test_util
module M = Abslayout_db.Make (Test_db)

let make_module_db () =
  let module A = Abslayout_db.Make (struct
    let conn = create_db "postgresql://localhost:5433/demomatch"
  end) in
  (module A : Abslayout_db.S)

let run_test s =
  let r = of_string_exn s |> M.resolve in
  M.annotate_schema r ;
  print_endline (of_ralgebra ~fresh:(Fresh.create ()) r |> to_sql)

let%expect_test "select-agg" =
  run_test "select([(0.2 * avg(r.f)) as test], r)" ;
  [%expect {| select  (0.2) * (avg(r."f")) as "test_3" from  r |}]

let%expect_test "project" =
  run_test "Select([r.f], r)" ; [%expect {| select  r."f" as "r_f_3" from  r |}]

let%expect_test "filter" =
  run_test "Filter(r.f = r.g, r)" ;
  [%expect
    {| select  r."f" as "r_f_1", r."g" as "r_g_2" from  r where (r."f") = (r."g") |}]

let%expect_test "eqjoin" =
  run_test "Join(r.f = s.g, r as r, s as s)" ;
  [%expect
    {| select  "r_f_3" , "r_g_4" , "s_f_1" , "s_g_2"  from  (select  r."f" as "r_f_3", r."g" as "r_g_4" from  r) as "t4",  (select  s."f" as "s_f_1", s."g" as "s_g_2" from  s) as "t5" where ("r_f_3") = ("s_g_2") |}]

let%expect_test "order-by" =
  run_test "OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc)" ;
  [%expect
    {| select distinct r1."f" as "r1_f_3" from  r1   order by "r1_f_3" desc |}]

let%expect_test "dedup" =
  run_test "Dedup(Select([r1.f], r1))" ;
  [%expect {| select distinct r1."f" as "r1_f_3" from  r1 |}]

let%expect_test "select" =
  run_test "Select([r1.f], r1)" ;
  [%expect {| select  r1."f" as "r1_f_3" from  r1 |}]

let%expect_test "scan" =
  run_test "r1" ;
  [%expect {| select  r1."f" as "r1_f_1", r1."g" as "r1_g_2" from  r1 |}]

let%expect_test "join" =
  run_test
    "join(lp.counter < lc.counter &&\n\
    \             lc.counter < lp.succ, \n\
    \          log as lp,\n\
    \          log as lc)";
  [%expect {| select  "log_counter_4" , "log_succ_5" , "log_id_6" , "log_counter_1" , "log_succ_2" , "log_id_3"  from  (select  log."counter" as "log_counter_4", log."succ" as "log_succ_5", log."id" as "log_id_6" from  log) as "t6",  (select  log."counter" as "log_counter_1", log."succ" as "log_succ_2", log."id" as "log_id_3" from  log) as "t7" where (("log_counter_4") < ("log_counter_1")) and (("log_counter_1") < ("log_succ_5")) |}]
