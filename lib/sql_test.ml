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
  let ctx = Sql.create_ctx ~fresh:(Fresh.create ()) () in
  print_endline (of_ralgebra ctx r |> to_string_hum ctx)

let run_test_tpch ?params s =
  let conn = create_db "postgresql://localhost:5432/tpch" in
  let module M = Abslayout_db.Make (struct
    let conn = conn
  end) in
  let params = Option.map params ~f:(Set.of_list (module Name.Compare_no_type)) in
  let r = of_string_exn s |> M.resolve ?params in
  M.annotate_schema r ;
  let ctx = Sql.create_ctx ~fresh:(Fresh.create ()) () in
  let sql = of_ralgebra ctx r |> to_string_hum ctx in
  print_endline sql ;
  match Db.check conn sql with
  | Ok () -> ()
  | Error e -> print_endline (Error.to_string_hum e)

let%expect_test "select-agg" =
  run_test "select([(0.2 * avg(r.f)) as test], r)" ;
  [%expect {|
    SELECT
        (0.2) * (avg(r. "f")) AS "x2"
    FROM
        r |}]

let%expect_test "project" =
  run_test "Select([r.f], r)" ; [%expect {|
    SELECT
        r. "f" AS "r_f_3"
    FROM
        r |}]

let%expect_test "filter" =
  run_test "Filter(r.f = r.g, r)" ;
  [%expect
    {|
      SELECT
          r. "f" AS "r_f_1",
          r. "g" AS "r_g_2"
      FROM
          r
      WHERE (r. "f") = (r. "g") |}]

let%expect_test "eqjoin" =
  run_test "Join(r.f = s.g, r as r, s as s)" ;
  [%expect
    {|
      SELECT
          "r_f_3",
          "r_g_4",
          "s_f_1",
          "s_g_2"
      FROM (
          SELECT
              r. "f" AS "r_f_3",
              r. "g" AS "r_g_4"
          FROM
              r) AS "t4",
          (
              SELECT
                  s. "f" AS "s_f_1",
                  s. "g" AS "s_g_2"
              FROM
                  s) AS "t5"
      WHERE ("r_f_3") = ("s_g_2") |}]

let%expect_test "order-by" =
  run_test "OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc)" ;
  [%expect {|
    SELECT DISTINCT
        r1. "f" AS "r1_f_3"
    FROM
        r1
    ORDER BY
        r1. "f" DESC |}]

let%expect_test "dedup" =
  run_test "Dedup(Select([r1.f], r1))" ;
  [%expect {|
    SELECT DISTINCT
        r1. "f" AS "r1_f_3"
    FROM
        r1 |}]

let%expect_test "select" =
  run_test "Select([r1.f], r1)" ;
  [%expect {|
    SELECT
        r1. "f" AS "r1_f_3"
    FROM
        r1 |}]

let%expect_test "scan" =
  run_test "r1" ;
  [%expect {|
    SELECT
        r1. "f" AS "r1_f_1",
        r1. "g" AS "r1_g_2"
    FROM
        r1 |}]

let%expect_test "join" =
  run_test
    "join(lp.counter < lc.counter &&\n\
    \             lc.counter < lp.succ, \n\
    \          log as lp,\n\
    \          log as lc)" ;
  [%expect
    {|
      SELECT
          "log_counter_4",
          "log_succ_5",
          "log_id_6",
          "log_counter_1",
          "log_succ_2",
          "log_id_3"
      FROM (
          SELECT
              log. "counter" AS "log_counter_4",
              log. "succ" AS "log_succ_5",
              log. "id" AS "log_id_6"
          FROM
              log) AS "t6",
          (
              SELECT
                  log. "counter" AS "log_counter_1",
                  log. "succ" AS "log_succ_2",
                  log. "id" AS "log_id_3"
              FROM
                  log) AS "t7"
      WHERE (("log_counter_4") < ("log_counter_1"))
      AND (("log_counter_1") < ("log_succ_5")) |}]

let%expect_test "select-groupby" =
  run_test "select([max(x)], groupby([r1.f, sum((r1.f * r1.g)) as x], [r1.f], r1))" ;
  [%expect
    {|
      SELECT
          max("x3") AS "x6"
      FROM (
          SELECT
              "r1_f_1" AS "r1_f_1_3",
              sum(("r1_f_1") * ("r1_g_2")) AS "x3"
          FROM (
              SELECT
                  r1. "f" AS "r1_f_1",
                  r1. "g" AS "r1_g_2"
              FROM
                  r1) AS "t4"
          GROUP BY
              ("r1_f_1")) AS "t5" |}]

let%expect_test "select-fusion-1" =
  run_test "select([max(x)], select([min(r1.f) as x], r1))" ;
  [%expect
    {|
      SELECT
          max("x2") AS "x4"
      FROM (
          SELECT
              min(r1. "f") AS "x2"
          FROM
              r1) AS "t3" |}]

let%expect_test "select-fusion-2" =
  run_test "select([max(x)], select([r1.f as x], r1))" ;
  [%expect {|
    SELECT
        max(r1. "f") AS "x3"
    FROM
        r1 |}]

let%expect_test "filter-fusion" =
  run_test "filter((x = 0), groupby([sum(r1.f) as x], [r1.g], r1))" ;
  [%expect
    {|
      SELECT
          "x2"
      FROM (
          SELECT
              sum("r1_f_1") AS "x2"
          FROM (
              SELECT
                  r1. "f" AS "r1_f_1",
                  r1. "g" AS "r1_g_2"
              FROM
                  r1) AS "t3"
          GROUP BY
              ("r1_g_2")) AS "t4"
      WHERE ("x2") = (0) |}]

(* let%expect_test "tpch-1" =
 *   run_test_tpch
 *     {|orderby([l_returnflag, l_linestatus],
 *   groupby([l_returnflag,
 *            l_linestatus,
 *            sum(l_quantity) as sum_qty,
 *            sum(l_extendedprice) as sum_base_price,
 *            sum((l_extendedprice) * ((1) - (l_discount))) as sum_disc_price,
 *            sum(((l_extendedprice) * ((1) - (l_discount))) * ((1) + (l_tax))) as sum_charge,
 *            avg(l_quantity) as avg_qty,
 *            avg(l_extendedprice) as avg_price,
 *            avg(l_discount) as avg_disc,
 *            count() as count_order],
 *     [l_returnflag, l_linestatus],
 *     filter((l_shipdate) <= ((date("1998-12-01")) - (day(1))), lineitem as l))
 *     ,asc)|} ;
 *   [%expect
 *     {|
 *       SELECT
 *           "lineitem_l_returnflag_9" AS "lineitem_l_returnflag_9_17",
 *           "lineitem_l_linestatus_10" AS "lineitem_l_linestatus_10_18",
 *           sum("lineitem_l_quantity_5") AS "x18",
 *           sum("lineitem_l_extendedprice_6") AS "x19",
 *           sum(("lineitem_l_extendedprice_6") * ((1) - ("lineitem_l_discount_7"))) AS "x20",
 *           sum((("lineitem_l_extendedprice_6") * ((1) - ("lineitem_l_discount_7"))) * ((1) + ("lineitem_l_tax_8"))) AS "x21",
 *           avg("lineitem_l_quantity_5") AS "x22",
 *           avg("lineitem_l_extendedprice_6") AS "x23",
 *           avg("lineitem_l_discount_7") AS "x24",
 *           count(\*\) AS "x25"
 *       FROM (
 *           SELECT
 *               lineitem. "l_orderkey" AS "lineitem_l_orderkey_1",
 *               lineitem. "l_partkey" AS "lineitem_l_partkey_2",
 *               lineitem. "l_suppkey" AS "lineitem_l_suppkey_3",
 *               lineitem. "l_linenumber" AS "lineitem_l_linenumber_4",
 *               lineitem. "l_quantity" AS "lineitem_l_quantity_5",
 *               lineitem. "l_extendedprice" AS "lineitem_l_extendedprice_6",
 *               lineitem. "l_discount" AS "lineitem_l_discount_7",
 *               lineitem. "l_tax" AS "lineitem_l_tax_8",
 *               lineitem. "l_returnflag" AS "lineitem_l_returnflag_9",
 *               lineitem. "l_linestatus" AS "lineitem_l_linestatus_10",
 *               lineitem. "l_shipdate" AS "lineitem_l_shipdate_11",
 *               lineitem. "l_commitdate" AS "lineitem_l_commitdate_12",
 *               lineitem. "l_receiptdate" AS "lineitem_l_receiptdate_13",
 *               lineitem. "l_shipinstruct" AS "lineitem_l_shipinstruct_14",
 *               lineitem. "l_shipmode" AS "lineitem_l_shipmode_15",
 *               lineitem. "l_comment" AS "lineitem_l_comment_16"
 *           FROM
 *               lineitem
 *           WHERE (lineitem. "l_shipdate") <= ((date('1998-12-01')) - (interval '(1) day'))) AS "t26"
 *       GROUP BY
 *           ("lineitem_l_returnflag_9",
 *               "lineitem_l_linestatus_10")
 *       ORDER BY
 *           "lineitem_l_returnflag_9",
 *           "lineitem_l_linestatus_10" |}]
 * 
 * let%expect_test "tpch-1" =
 *   run_test_tpch
 *     {|select([sum(l.l_discount) as agg8,
 *                     count() as agg7,
 *                     sum(((l.l_extendedprice * (1 - l.l_discount)) *
 *                         (1 + l.l_tax))) as agg6,
 *                     sum((l.l_extendedprice * (1 - l.l_discount))) as agg5,
 *                     sum(l.l_extendedprice) as agg4,
 *                     sum(l.l_quantity) as agg3,
 *                     l.l_returnflag,
 *                     l.l_linestatus],
 *                   lineitem as l)|} ;
 *   [%expect
 *     {|
 *       SELECT
 *           sum(lineitem. "l_discount") AS "x16",
 *           count(\*\) AS "x17",
 *           sum(((lineitem. "l_extendedprice") * ((1) - (lineitem. "l_discount"))) * ((1) + (lineitem. "l_tax"))) AS "x18",
 *           sum((lineitem. "l_extendedprice") * ((1) - (lineitem. "l_discount"))) AS "x19",
 *           sum(lineitem. "l_extendedprice") AS "x20",
 *           sum(lineitem. "l_quantity") AS "x21",
 *           min(lineitem. "l_returnflag") AS "lineitem_l_returnflag_23",
 *           min(lineitem. "l_linestatus") AS "lineitem_l_linestatus_24"
 *       FROM
 *           lineitem |}] *)
