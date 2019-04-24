open Base
open Stdio
open Collections
open Sql
open Test_util

let%test_module _ =
  ( module struct
    let run_test s =
      let module Test_db = (val make_test_db ()) in
      let module M = Abslayout_db.Make (Test_db) in
      let r = M.load_string s in
      let ctx = Sql.create_ctx ~fresh:(Fresh.create ()) () in
      let sql_str = of_ralgebra ctx r |> to_string_hum ctx in
      Db.check Test_db.conn sql_str |> Or_error.ok_exn ;
      print_endline sql_str

    let%expect_test "select-agg" =
      run_test "select([(0.2 * avg(f)) as test], r)" ;
      [%expect
        {|
    SELECT
        (0.2) * (avg(r_1. "f")) AS "test_1"
    FROM
        "r" AS "r_1" |}]

    let%expect_test "project" =
      run_test "Select([f], r)" ;
      [%expect
        {|
    SELECT
        r_1. "f" AS "r_1_f_2"
    FROM
        "r" AS "r_1" |}]

    let%expect_test "filter" =
      run_test "Filter(f = g, r)" ;
      [%expect
        {|
      SELECT
          r_1. "f" AS "r_1_f_1",
          r_1. "g" AS "r_1_g_1"
      FROM
          "r" AS "r_1"
      WHERE ((r_1. "f") = (r_1. "g")) |}]

    let%expect_test "eqjoin" =
      run_test
        "Join(r_f = s_g, select([f as r_f, g as r_g], r), select([f as s_f, g as \
         s_g], s))" ;
      [%expect
        {|
      SELECT
          r_2. "f" AS "r_f_1",
          r_2. "g" AS "r_g_1",
          s_1. "f" AS "s_f_1",
          s_1. "g" AS "s_g_1"
      FROM
          "r" AS "r_2",
          "s" AS "s_1"
      WHERE ((r_2. "f") = (s_1. "g")) |}]

    let%expect_test "join-select" =
      run_test "join(true, select([id as p_id], log), select([id as c_id], log))" ;
      [%expect {|
        SELECT
            log_2. "id" AS "p_id_1",
            log_1. "id" AS "c_id_1"
        FROM
            "log" AS "log_2",
            "log" AS "log_1"
        WHERE (TRUE) |}]

    let%expect_test "order-by" =
      run_test "OrderBy([f desc], Dedup(Select([f], r1)))" ;
      [%expect
        {|
    SELECT DISTINCT
        r1_1. "f" AS "r1_1_f_2"
    FROM
        "r1" AS "r1_1"
    ORDER BY
        r1_1. "f" DESC |}]

    let%expect_test "dedup" =
      run_test "Dedup(Select([f], r1))" ;
      [%expect
        {|
    SELECT DISTINCT
        r1_1. "f" AS "r1_1_f_2"
    FROM
        "r1" AS "r1_1" |}]

    let%expect_test "select" =
      run_test "Select([f], r1)" ;
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
        "join(p_counter < c_counter && c_counter < p_succ, \n\
        \          select([counter as p_counter, succ as p_succ], log),\n\
        \          select([counter as c_counter], log))" ;
      [%expect
        {|
      SELECT
          log_2. "counter" AS "p_counter_1",
          log_2. "succ" AS "p_succ_1",
          log_1. "counter" AS "c_counter_1"
      FROM
          "log" AS "log_2",
          "log" AS "log_1"
      WHERE (((log_2. "counter") < (log_1. "counter"))
          AND ((log_1. "counter") < (log_2. "succ"))) |}]

    let%expect_test "join-groupby" =
      run_test
        {|join(f = g || x = y, groupby([f, sum((f * g)) as x], [f], r1), groupby([g, sum((f * g)) as y], [g], r1))|} ;
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
    [WARNING] Shadowing of f@run.
    [WARNING] Shadowing of g@run.
    [WARNING] Shadowing of f@run.
    [WARNING] Shadowing of g@run.
    [WARNING] Output shadowing of f.
    [WARNING] Output shadowing of g.
    [WARNING] Shadowing of f@run.
    [WARNING] Shadowing of g@run.
    [WARNING] Shadowing of f@run.
    [WARNING] Shadowing of g@run.
    [WARNING] Output shadowing of f.
    [WARNING] Output shadowing of g.
    [WARNING] Shadowing of f@run.
    [WARNING] Shadowing of g@run.
    [WARNING] Duplicate name in schema f.
    [WARNING] Duplicate name in schema g.
    [WARNING] Duplicate name in schema f.
    [WARNING] Duplicate name in schema g.
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
      run_test "select([max(x)], groupby([f, sum((f * g)) as x], [f], r1))" ;
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
      run_test "select([max(x)], select([min(f) as x], r1))" ;
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
      run_test "select([max(x)], select([f as x], r1))" ;
      [%expect
        {|
    SELECT
        max(r1_1. "f") AS "x1"
    FROM
        "r1" AS "r1_1" |}]

    let%expect_test "filter-fusion" =
      run_test "filter((x = 0), groupby([sum(f) as x], [g], r1))" ;
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
      run_test "groupby([sum(f) as x], [g], dedup(r1))" ;
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
        "ahashidx(select([f], r1) as k, select([g], filter(f = k.f, r1)), null)" ;
      [%expect
        {|
    SELECT
        "r1_1_f_2" AS "r1_1_f_3",
        "r1_2_g_2" AS "r1_2_g_3"
    FROM (
        SELECT
            r1_1. "f" AS "r1_1_f_2"
        FROM
            "r1" AS "r1_1"
        WHERE ((r1_1. "f") = (NULL))) AS "t3", LATERAL (
            SELECT
                r1_2. "g" AS "r1_2_g_2"
            FROM
                "r1" AS "r1_2"
            WHERE ((r1_2. "f") = ("r1_1_f_2"))) AS "t2" |}]

    let%expect_test "ordered-idx" =
      run_test
        "aorderedidx(select([f], r1) as k, select([g], filter(f = k.f, r1)), null, \
         null)" ;
      [%expect
        {|
    SELECT
        "r1_1_f_2" AS "r1_1_f_3",
        "r1_2_g_2" AS "r1_2_g_3"
    FROM (
        SELECT
            r1_1. "f" AS "r1_1_f_2"
        FROM
            "r1" AS "r1_1") AS "t3",
        LATERAL (
            SELECT
                r1_2. "g" AS "r1_2_g_2"
            FROM
                "r1" AS "r1_2"
            WHERE ((r1_2. "f") = ("r1_1_f_2"))) AS "t2"
    WHERE (((NULL) < ("r1_1_f_2"))
        AND (("r1_1_f_2") < (NULL))) |}]
  end )
