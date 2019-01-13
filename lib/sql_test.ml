open Base
open Stdio
open Abslayout
open Sql
open Test_util

let rels = Hashtbl.create (module Db.Relation)

let _ =
  create rels "r" ["f"; "g"] [[0; 5]; [1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]; [4; 6]]

let _ =
  create rels "s" ["f"; "g"] [[0; 5]; [1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]; [4; 6]]

let _ =
  create rels "log" ["counter"; "succ"; "id"]
    [[1; 4; 1]; [2; 3; 2]; [3; 4; 3]; [4; 6; 1]; [5; 6; 3]]

module E = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (E)

let make_module_db () =
  let module E = Eval.Make (struct
    let conn = create_db "postgresql://localhost:5433/demomatch"
  end) in
  let module A = Abslayout_db.Make (E) in
  ((module E : Eval.S), (module A : Abslayout_db.S))

let of_string_exn s =
  let r = of_string_exn s in
  M.annotate_schema r ; r

let%expect_test "select-agg" =
  let r = of_string_exn "select([(0.2 * avg(r.r)) as test], r)" in
  print_endline (ralgebra_to_sql r) ;
  [%expect {| select (0.2) * (avg(r."r")) as test from r |}]

let%expect_test "project" =
  let r = of_string_exn "Select([r.r], r)" in
  print_endline (ralgebra_to_sql r) ;
  [%expect {| select r."r" from r |}]

let%expect_test "filter" =
  let r = of_string_exn "Filter(r.f = r.g, r)" in
  print_endline (ralgebra_to_sql r) ;
  [%expect {| select * from r where (r."f") = (r."g") |}]

let%expect_test "eqjoin" =
  let r = of_string_exn "Join(r.f = s.g, r as r, s as s)" in
  print_endline (ralgebra_to_sql r) ;
  [%expect {| select * from r as t3, s as t4 where (t3."f") = (t4."g") |}]

let%expect_test "foreach-1" =
  let r1 = of_string_exn "select([r.f as k], r)" in
  let r2 = of_string_exn "select([r.g], filter(r.f = k, r))" in
  print_endline (ralgebra_foreach r1 r2) ;
  [%expect
    {| select t6."k", t9."g" from (select r."f" as k from r) as t6, lateral (select t8."g" from (select * from r where (r."f") = ("k")) as t8) as t9 order by (t6."k") |}]

let%expect_test "example2" =
  let (module E), (module A) = make_module_db () in
  let q1 =
    {|dedup(select([lp_id as lp_k, lc_id as lc_k], 
    join(true,
      select([id as lp_id], log_bench),
             select([id as lc_id], log_bench))))|}
    |> Abslayout.of_string_exn
  in
  A.annotate_schema q1 ;
  let q2 =
    {|select([lp_counter, lc_counter], 
    join(lp_counter < lc_counter && 
         lc_counter < lp_succ, 
      select([counter as lp_counter, succ as lp_succ],
        filter(log_bench.id = lp_k, log_bench)), 
      select([counter as lc_counter],
             filter(log_bench.id = lc_k, log_bench))))|}
    |> Abslayout.of_string_exn
  in
  A.annotate_schema q2 ;
  print_endline (ralgebra_foreach q1 q2) ;
  [%expect
    {| select t16."lp_k", t16."lc_k", t24."lp_counter", t24."lc_counter" from (select distinct * from (select t14."lp_id" as lp_k, t14."lc_id" as lc_k from (select * from (select "id" as lp_id from log_bench) as t12, (select "id" as lc_id from log_bench) as t13 where true) as t14) as t15) as t16, lateral (select t23."lp_counter", t23."lc_counter" from (select * from (select "counter" as lp_counter, "succ" as lp_succ from (select * from log_bench where (log_bench."id") = ("lp_k")) as t18) as t21, (select "counter" as lc_counter from (select * from log_bench where (log_bench."id") = ("lc_k")) as t20) as t22 where ((t21."lp_counter") < (t22."lc_counter")) and ((t22."lc_counter") < (t21."lp_succ"))) as t23) as t24 order by (t16."lp_k", t16."lc_k") |}]
