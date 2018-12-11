open Base
open Stdio
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

let run_test_tpch ?params s =
  let conn = Db.create "tpch" ~port:"5432" in
  let module M = Abslayout_db.Make (struct
    let conn = conn
  end) in
  let params = Option.map params ~f:(Set.of_list (module Name.Compare_no_type)) in
  let r = of_string_exn s |> M.resolve ?params in
  M.annotate_schema r ;
  let sql = of_ralgebra ~fresh:(Fresh.create ()) r |> to_sql in
  print_endline sql ;
  match Db.check conn sql with
  | Ok () -> ()
  | Error e -> print_endline (Error.to_string_hum e)

let%expect_test "select-agg" =
  run_test "select([(0.2 * avg(r.f)) as test], r)" ;
  [%expect {| select  (0.2) * (avg(r."f")) as "x2" from  r |}]

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
    \          log as lc)" ;
  [%expect
    {| select  "log_counter_4" , "log_succ_5" , "log_id_6" , "log_counter_1" , "log_succ_2" , "log_id_3"  from  (select  log."counter" as "log_counter_4", log."succ" as "log_succ_5", log."id" as "log_id_6" from  log) as "t6",  (select  log."counter" as "log_counter_1", log."succ" as "log_succ_2", log."id" as "log_id_3" from  log) as "t7" where (("log_counter_4") < ("log_counter_1")) and (("log_counter_1") < ("log_succ_5")) |}]

let%expect_test "tpch-1" =
  run_test_tpch
    {|orderby([l_returnflag, l_linestatus],
  groupby([l_returnflag,
           l_linestatus,
           sum(l_quantity) as sum_qty,
           sum(l_extendedprice) as sum_base_price,
           sum((l_extendedprice) * ((1) - (l_discount))) as sum_disc_price,
           sum(((l_extendedprice) * ((1) - (l_discount))) * ((1) + (l_tax))) as sum_charge,
           avg(l_quantity) as avg_qty,
           avg(l_extendedprice) as avg_price,
           avg(l_discount) as avg_disc,
           count() as count_order],
    [l_returnflag, l_linestatus],
    filter((l_shipdate) <= ((date("1998-12-01")) - (day(1))), lineitem as l))
    ,asc)|} ;
  [%expect
    {| select  "lineitem_l_returnflag_9" as "lineitem_l_returnflag_9_17", "lineitem_l_linestatus_10" as "lineitem_l_linestatus_10_18", sum("lineitem_l_quantity_5") as "x18", sum("lineitem_l_extendedprice_6") as "x19", sum(("lineitem_l_extendedprice_6") * ((1) - ("lineitem_l_discount_7"))) as "x20", sum((("lineitem_l_extendedprice_6") * ((1) - ("lineitem_l_discount_7"))) * ((1) + ("lineitem_l_tax_8"))) as "x21", avg("lineitem_l_quantity_5") as "x22", avg("lineitem_l_extendedprice_6") as "x23", avg("lineitem_l_discount_7") as "x24", count(*) as "x25" from  (select  lineitem."l_orderkey" as "lineitem_l_orderkey_1", lineitem."l_partkey" as "lineitem_l_partkey_2", lineitem."l_suppkey" as "lineitem_l_suppkey_3", lineitem."l_linenumber" as "lineitem_l_linenumber_4", lineitem."l_quantity" as "lineitem_l_quantity_5", lineitem."l_extendedprice" as "lineitem_l_extendedprice_6", lineitem."l_discount" as "lineitem_l_discount_7", lineitem."l_tax" as "lineitem_l_tax_8", lineitem."l_returnflag" as "lineitem_l_returnflag_9", lineitem."l_linestatus" as "lineitem_l_linestatus_10", lineitem."l_shipdate" as "lineitem_l_shipdate_11", lineitem."l_commitdate" as "lineitem_l_commitdate_12", lineitem."l_receiptdate" as "lineitem_l_receiptdate_13", lineitem."l_shipinstruct" as "lineitem_l_shipinstruct_14", lineitem."l_shipmode" as "lineitem_l_shipmode_15", lineitem."l_comment" as "lineitem_l_comment_16" from  lineitem where (lineitem."l_shipdate") <= ((date('1998-12-01')) - (interval '(1) day'))) as "t26"  group by ("lineitem_l_returnflag_9", "lineitem_l_linestatus_10") order by "lineitem_l_returnflag_9_17" , "lineitem_l_linestatus_10_18" |}]

let%expect_test "tpch-1" =
  run_test_tpch
    {|select([sum(l.l_discount) as agg8,
                    count() as agg7,
                    sum(((l.l_extendedprice * (1 - l.l_discount)) *
                        (1 + l.l_tax))) as agg6,
                    sum((l.l_extendedprice * (1 - l.l_discount))) as agg5,
                    sum(l.l_extendedprice) as agg4,
                    sum(l.l_quantity) as agg3,
                    l.l_returnflag,
                    l.l_linestatus],
                  lineitem as l)|};
  [%expect {| select  sum(lineitem."l_discount") as "x16", count(*) as "x17", sum(((lineitem."l_extendedprice") * ((1) - (lineitem."l_discount"))) * ((1) + (lineitem."l_tax"))) as "x18", sum((lineitem."l_extendedprice") * ((1) - (lineitem."l_discount"))) as "x19", sum(lineitem."l_extendedprice") as "x20", sum(lineitem."l_quantity") as "x21", min(lineitem."l_returnflag") as "lineitem_l_returnflag_23", min(lineitem."l_linestatus") as "lineitem_l_linestatus_24" from  lineitem |}]
