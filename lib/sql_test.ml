open Base
open Stdio
open Abslayout
open Sql

let rels = Hashtbl.create (module Db.Relation)

let _ =
  Test_util.create rels "r" ["f"; "g"]
    [[0; 5]; [1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]; [4; 6]]

let _ =
  Test_util.create rels "s" ["f"; "g"]
    [[0; 5]; [1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]; [4; 6]]

let _ =
  Test_util.create rels "log" ["counter"; "succ"; "id"]
    [[1; 4; 1]; [2; 3; 2]; [3; 4; 3]; [4; 6; 1]; [5; 6; 3]]

module E = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (E)

let make_module_db () =
  let module E = Eval.Make (struct
    let conn = new Postgresql.connection ~dbname:"demomatch" ~port:"5433" ()
  end) in
  let module A = Abslayout_db.Make (E) in
  ((module E : Eval.S), (module A : Abslayout_db.S))

let%expect_test "project" =
  let r = of_string_exn "Select([r.r], r)" |> M.annotate_schema in
  print_endline (ralgebra_to_sql r) ;
  [%expect {| select r."r" from r |}]

let%expect_test "filter" =
  let r = of_string_exn "Filter(r.f = r.g, r)" |> M.annotate_schema in
  print_endline (ralgebra_to_sql r) ;
  [%expect {| select * from r where (r."f") = (r."g") |}]

let%expect_test "eqjoin" =
  let r = of_string_exn "Join(r.f = s.g, r as r, s as s)" |> M.annotate_schema in
  print_endline (ralgebra_to_sql r) ;
  [%expect {| select * from r as t0, s as t1 where (t0."f") = (t1."g") |}]

let%expect_test "foreach-1" =
  let r1 = of_string_exn "select([r.f as k], r)" |> M.annotate_schema in
  let r2 = of_string_exn "select([r.g], filter(r.f = k, r))" |> M.annotate_schema in
  print_endline (ralgebra_foreach r1 r2) ;
  [%expect
    {| select t1."k", t4."g" from (select r."f" as k from r) as t1, lateral (select t3."g" from (select * from r where (r."f") = ("k")) as t3) as t4 order by (t1."k") |}]

let%expect_test "example2" =
  let (module E), (module A) = make_module_db () in
  let q1 =
    {|dedup(select([lp_id as lp_k, lc_id as lc_k], 
    join(true,
      select([id as lp_id], log_bench),
             select([id as lc_id], log_bench))))|}
    |> of_string_exn |> A.annotate_schema
  in
  let q2 =
    {|select([lp_counter, lc_counter], 
    join(lp_counter < lc_counter && 
         lc_counter < lp_succ, 
      select([counter as lp_counter, succ as lp_succ],
        filter(log_bench.id = lp_k, log_bench)), 
      select([counter as lc_counter],
             filter(log_bench.id = lc_k, log_bench))))|}
    |> of_string_exn |> A.annotate_schema
  in
  print_endline (ralgebra_foreach q1 q2) ;
  [%expect
    {| select t6."lp_k", t6."lc_k", t14."lp_counter", t14."lc_counter" from (select distinct * from (select t4."lp_id" as lp_k, t4."lc_id" as lc_k from (select * from (select "id" as lp_id from log_bench) as t2, (select "id" as lc_id from log_bench) as t3 where true) as t4) as t5) as t6, lateral (select t13."lp_counter", t13."lc_counter" from (select * from (select "counter" as lp_counter, "succ" as lp_succ from (select * from log_bench where (log_bench."id") = ("lp_k")) as t8) as t11, (select "counter" as lc_counter from (select * from log_bench where (log_bench."id") = ("lc_k")) as t10) as t12 where ((t11."lp_counter") < (t12."lc_counter")) and ((t12."lc_counter") < (t11."lp_succ"))) as t13) as t14 order by (t6."lp_k", t6."lc_k") |}]
