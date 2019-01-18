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
  [%expect
    {|
    SELECT
        (0.2) * (avg(r_1. "f")) AS "test_4"
    FROM
        "r" AS "r_1" |}]

let%expect_test "project" =
  run_test "Select([r.f], r)" ;
  [%expect
    {|
    SELECT
        r_1. "f" AS "r_1_f_4"
    FROM
        "r" AS "r_1" |}]

let%expect_test "filter" =
  run_test "Filter(r.f = r.g, r)" ;
  [%expect
    {|
      SELECT
          r_1. "f" AS "r_1_f_2",
          r_1. "g" AS "r_1_g_3"
      FROM
          "r" AS "r_1"
      WHERE ((r_1. "f") = (r_1. "g")) |}]

let%expect_test "eqjoin" =
  run_test "Join(r.f = s.g, r as r, s as s)" ;
  [%expect
    {|
      SELECT
          r_4. "f" AS "r_4_f_5",
          r_4. "g" AS "r_4_g_6",
          s_1. "f" AS "s_1_f_2",
          s_1. "g" AS "s_1_g_3"
      FROM
          "r" AS "r_4",
          "s" AS "s_1"
      WHERE ((r_4. "f") = (s_1. "g")) |}]

let%expect_test "order-by" =
  run_test "OrderBy([r1.f desc], Dedup(Select([r1.f], r1)))" ;
  [%expect
    {|
    SELECT DISTINCT
        r1_1. "f" AS "r1_1_f_4"
    FROM
        "r1" AS "r1_1"
    ORDER BY
        r1_1. "f" DESC |}]

let%expect_test "dedup" =
  run_test "Dedup(Select([r1.f], r1))" ;
  [%expect
    {|
    SELECT DISTINCT
        r1_1. "f" AS "r1_1_f_4"
    FROM
        "r1" AS "r1_1" |}]

let%expect_test "select" =
  run_test "Select([r1.f], r1)" ;
  [%expect
    {|
    SELECT
        r1_1. "f" AS "r1_1_f_4"
    FROM
        "r1" AS "r1_1" |}]

let%expect_test "scan" =
  run_test "r1" ;
  [%expect
    {|
    SELECT
        r1_1. "f" AS "r1_1_f_2",
        r1_1. "g" AS "r1_1_g_3"
    FROM
        "r1" AS "r1_1" |}]

let%expect_test "join" =
  run_test
    "join(lp.counter < lc.counter &&\n\
    \             lc.counter < lp.succ, \n\
    \          log as lp,\n\
    \          log as lc)" ;
  [%expect
    {|
      SELECT
          log_5. "counter" AS "log_5_counter_6",
          log_5. "succ" AS "log_5_succ_7",
          log_5. "id" AS "log_5_id_8",
          log_1. "counter" AS "log_1_counter_2",
          log_1. "succ" AS "log_1_succ_3",
          log_1. "id" AS "log_1_id_4"
      FROM
          "log" AS "log_5",
          "log" AS "log_1"
      WHERE (((log_5. "counter") < (log_1. "counter"))
          AND ((log_1. "counter") < (log_5. "succ"))) |}]

let%expect_test "join-groupby" =
  run_test
    {|join(r1.f = r1.g || x = y, groupby([r1.f, sum((r1.f * r1.g)) as x], [r1.f], r1), groupby([r1.g, sum((r1.f * r1.g)) as y], [r1.g], r1))|} ;
  [%expect
    {|
    SELECT
        "r1_6_f_9" AS "r1_6_f_9_12",
        "x_10" AS "x_10_13",
        "r1_1_g_4" AS "r1_1_g_4_15",
        "y_5" AS "y_5_16"
    FROM (
        SELECT
            r1_6. "f" AS "r1_6_f_9",
            sum((r1_6. "f") * (r1_6. "g")) AS "x_10"
        FROM
            "r1" AS "r1_6"
        GROUP BY
            (r1_6. "f")) AS "t10",
        (
            SELECT
                r1_1. "g" AS "r1_1_g_4",
                sum((r1_1. "f") * (r1_1. "g")) AS "y_5"
            FROM
                "r1" AS "r1_1"
            GROUP BY
                (r1_1. "g")) AS "t13"
    WHERE ((("r1_6_f_9") = ("r1_1_g_4"))
        OR (("x_10") = ("y_5"))) |}]

let%expect_test "join-cond" =
  run_test {|filter(true||false, join(true&&false, r1, r))|} ;
  [%expect
    {|
    SELECT
        r1_4. "f" AS "r1_4_f_5",
        r1_4. "g" AS "r1_4_g_6",
        r_1. "f" AS "r_1_f_2",
        r_1. "g" AS "r_1_g_3"
    FROM
        "r1" AS "r1_4",
        "r" AS "r_1"
    WHERE ((TRUE)
        OR (FALSE))
    AND ((TRUE)
        AND (FALSE)) |}]

let%expect_test "select-groupby" =
  run_test "select([max(x)], groupby([r1.f, sum((r1.f * r1.g)) as x], [r1.f], r1))" ;
  [%expect
    {|
      SELECT
          max("x_5") AS "x_5_9"
      FROM (
          SELECT
              r1_1. "f" AS "r1_1_f_4",
              sum((r1_1. "f") * (r1_1. "g")) AS "x_5"
          FROM
              "r1" AS "r1_1"
          GROUP BY
              (r1_1. "f")) AS "t5" |}]

let%expect_test "select-fusion-1" =
  run_test "select([max(x)], select([min(r1.f) as x], r1))" ;
  [%expect
    {|
      SELECT
          max("x_4") AS "x_4_7"
      FROM (
          SELECT
              min(r1_1. "f") AS "x_4"
          FROM
              "r1" AS "r1_1") AS "t4" |}]

let%expect_test "select-fusion-2" =
  run_test "select([max(x)], select([r1.f as x], r1))" ;
  [%expect
    {|
    SELECT
        max(r1_1. "f") AS "x_5"
    FROM
        "r1" AS "r1_1" |}]

let%expect_test "filter-fusion" =
  run_test "filter((x = 0), groupby([sum(r1.f) as x], [r1.g], r1))" ;
  [%expect
    {|
      SELECT
          "x_4" AS "x_4_6"
      FROM (
          SELECT
              sum(r1_1. "f") AS "x_4"
          FROM
              "r1" AS "r1_1"
          GROUP BY
              (r1_1. "g")) AS "t4"
      WHERE (("x_4") = (0)) |}]

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
