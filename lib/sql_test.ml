open Base
open Stdio
open Collections
open Abslayout
open Sql
open Test_util
module M = Abslayout_db.Make (Test_db)

let run_test s =
  let r = of_string_exn s |> M.resolve in
  M.annotate_schema r ;
  let ctx = Sql.create_ctx ~fresh:(Fresh.create ()) () in
  print_endline (of_ralgebra ctx r |> to_string_hum ctx)

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

let%expect_test "groupby-dedup" =
  run_test "groupby([sum(r1.f) as x], [r1.g], dedup(r1))" ;
  [%expect
    {|
    SELECT
        sum("r1_1_f_2") AS "x_7"
    FROM ( SELECT DISTINCT
            r1_1. "f" AS "r1_1_f_2",
            r1_1. "g" AS "r1_1_g_3"
        FROM
            "r1" AS "r1_1") AS "t3"
    GROUP BY
        ("r1_1_g_3") |}]
