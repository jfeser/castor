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
  let sql_str = of_ralgebra ctx r |> to_string_hum ctx in
  Db.check Test_db.conn sql_str |> Or_error.ok_exn ;
  print_endline sql_str

let%expect_test "select-agg" =
  run_test "select([(0.2 * avg(r.f)) as test], r)" ;
  [%expect
    {|
    SELECT
        (0.2) * (avg(r_1. "f")) AS "test_1"
    FROM
        "r" AS "r_1" |}]

let%expect_test "project" =
  run_test "Select([r.f], r)" ;
  [%expect
    {|
    SELECT
        r_1. "f" AS "r_1_f_2"
    FROM
        "r" AS "r_1" |}]

let%expect_test "filter" =
  run_test "Filter(r.f = r.g, r)" ;
  [%expect
    {|
      SELECT
          r_1. "f" AS "r_1_f_1",
          r_1. "g" AS "r_1_g_1"
      FROM
          "r" AS "r_1"
      WHERE ((r_1. "f") = (r_1. "g")) |}]

let%expect_test "eqjoin" =
  run_test "Join(r.f = s.g, r as r, s as s)" ;
  [%expect
    {|
      SELECT
          r_2. "f" AS "r_2_f_1",
          r_2. "g" AS "r_2_g_1",
          s_1. "f" AS "s_1_f_1",
          s_1. "g" AS "s_1_g_1"
      FROM
          "r" AS "r_2",
          "s" AS "s_1"
      WHERE ((r_2. "f") = (s_1. "g")) |}]

let%expect_test "order-by" =
  run_test "OrderBy([r1.f desc], Dedup(Select([r1.f], r1)))" ;
  [%expect
    {|
    SELECT DISTINCT
        r1_1. "f" AS "r1_1_f_2"
    FROM
        "r1" AS "r1_1"
    ORDER BY
        r1_1. "f" DESC |}]

let%expect_test "dedup" =
  run_test "Dedup(Select([r1.f], r1))" ;
  [%expect
    {|
    SELECT DISTINCT
        r1_1. "f" AS "r1_1_f_2"
    FROM
        "r1" AS "r1_1" |}]

let%expect_test "select" =
  run_test "Select([r1.f], r1)" ;
  [%expect
    {|
    SELECT
        r1_1. "f" AS "r1_1_f_2"
    FROM
        "r1" AS "r1_1" |}]

let%expect_test "scan" =
  run_test "r1" ;
  [%expect
    {|
    SELECT
        r1_1. "f" AS "r1_1_f_1",
        r1_1. "g" AS "r1_1_g_1"
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
          log_2. "counter" AS "log_2_counter_1",
          log_2. "succ" AS "log_2_succ_1",
          log_2. "id" AS "log_2_id_1",
          log_1. "counter" AS "log_1_counter_1",
          log_1. "succ" AS "log_1_succ_1",
          log_1. "id" AS "log_1_id_1"
      FROM
          "log" AS "log_2",
          "log" AS "log_1"
      WHERE (((log_2. "counter") < (log_1. "counter"))
          AND ((log_1. "counter") < (log_2. "succ"))) |}]

let%expect_test "join-groupby" =
  run_test
    {|join(r1.f = r1.g || x = y, groupby([r1.f, sum((r1.f * r1.g)) as x], [r1.f], r1), groupby([r1.g, sum((r1.f * r1.g)) as y], [r1.g], r1))|} ;
  [%expect
    {|
    SELECT
        "r1_2_f_2" AS "r1_2_f_3",
        "x_1" AS "x_2",
        "r1_1_g_2" AS "r1_1_g_3",
        "y_1" AS "y_2"
    FROM (
        SELECT
            r1_2. "f" AS "r1_2_f_2",
            sum((r1_2. "f") * (r1_2. "g")) AS "x_1"
        FROM
            "r1" AS "r1_2"
        GROUP BY
            (r1_2. "f")) AS "t2",
        (
            SELECT
                r1_1. "g" AS "r1_1_g_2",
                sum((r1_1. "f") * (r1_1. "g")) AS "y_1"
            FROM
                "r1" AS "r1_1"
            GROUP BY
                (r1_1. "g")) AS "t3"
    WHERE ((("r1_2_f_2") = ("r1_1_g_2"))
        OR (("x_1") = ("y_1"))) |}]

let%expect_test "join-cond" =
  run_test {|filter(true||false, join(true&&false, r1, r))|} ;
  [%expect
    {|
    SELECT
        r1_2. "f" AS "r1_2_f_1",
        r1_2. "g" AS "r1_2_g_1",
        r_1. "f" AS "r_1_f_1",
        r_1. "g" AS "r_1_g_1"
    FROM
        "r1" AS "r1_2",
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
          max("x_1") AS "x2"
      FROM (
          SELECT
              r1_1. "f" AS "r1_1_f_2",
              sum((r1_1. "f") * (r1_1. "g")) AS "x_1"
          FROM
              "r1" AS "r1_1"
          GROUP BY
              (r1_1. "f")) AS "t1" |}]

let%expect_test "select-fusion-1" =
  run_test "select([max(x)], select([min(r1.f) as x], r1))" ;
  [%expect
    {|
      SELECT
          max("x_1") AS "x2"
      FROM (
          SELECT
              min(r1_1. "f") AS "x_1"
          FROM
              "r1" AS "r1_1") AS "t1" |}]

let%expect_test "select-fusion-2" =
  run_test "select([max(x)], select([r1.f as x], r1))" ;
  [%expect
    {|
    SELECT
        max(r1_1. "f") AS "x1"
    FROM
        "r1" AS "r1_1" |}]

let%expect_test "filter-fusion" =
  run_test "filter((x = 0), groupby([sum(r1.f) as x], [r1.g], r1))" ;
  [%expect
    {|
      SELECT
          "x_1" AS "x_2"
      FROM (
          SELECT
              sum(r1_1. "f") AS "x_1"
          FROM
              "r1" AS "r1_1"
          GROUP BY
              (r1_1. "g")) AS "t1"
      WHERE (("x_1") = (0)) |}]

let%expect_test "groupby-dedup" =
  run_test "groupby([sum(r1.f) as x], [r1.g], dedup(r1))" ;
  [%expect
    {|
    SELECT
        sum("r1_1_f_1") AS "x_1"
    FROM ( SELECT DISTINCT
            r1_1. "f" AS "r1_1_f_1",
            r1_1. "g" AS "r1_1_g_1"
        FROM
            "r1" AS "r1_1") AS "t1"
    GROUP BY
        ("r1_1_g_1") |}]

let%expect_test "hash-idx" =
  run_test
    "ahashidx(select([r1.f as k], r1), select([r1.g], filter(r1.f = k, r1)), null)" ;
  [%expect
    {|
    SELECT
        "k_1" AS "k_2",
        "r1_2_g_2" AS "r1_2_g_3"
    FROM (
        SELECT
            r1_1. "f" AS "k_1"
        FROM
            "r1" AS "r1_1"
        WHERE ((r1_1. "f") = (NULL))) AS "t3", LATERAL (
            SELECT
                r1_2. "g" AS "r1_2_g_2"
            FROM
                "r1" AS "r1_2"
            WHERE ((r1_2. "f") = ("k_1"))) AS "t2" |}]

let%expect_test "ordered-idx" =
  run_test
    "aorderedidx(select([r1.f as k], r1), select([r1.g], filter(r1.f = k, r1)), \
     null, null)" ;
  [%expect
    {|
    SELECT
        "k_1" AS "k_2",
        "r1_2_g_2" AS "r1_2_g_3"
    FROM (
        SELECT
            r1_1. "f" AS "k_1"
        FROM
            "r1" AS "r1_1") AS "t3",
        LATERAL (
            SELECT
                r1_2. "g" AS "r1_2_g_2"
            FROM
                "r1" AS "r1_2"
            WHERE ((r1_2. "f") = ("k_1"))) AS "t2"
    WHERE (((NULL) < ("k_1"))
        AND (("k_1") < (NULL))) |}]
