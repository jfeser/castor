open Sql
open Test_util
open Abslayout_load

let run_test ?(conn = Test_util.test_db_conn) s =
  Logs.Src.set_level src (Some Debug);
  let conn = Lazy.force conn in
  let r = load_string_exn conn s in
  let sql_str = of_ralgebra r |> to_string_hum in
  (match Db.check conn sql_str with
  | Ok () -> ()
  | Error e -> print_endline (Error.to_string_hum e));
  print_endline sql_str;
  Logs.Src.set_level src None

let%expect_test "select-agg" =
  run_test "select([(0.2 * avg(f)) as test], r)";
  [%expect
    {|
    SELECT (0.2) * (avg("f_0")) AS "test_0"
    FROM
      (SELECT r_0."f" AS "f_0",
              r_0."g" AS "g_0"
       FROM "r" AS "r_0") AS "t0" |}]

let%expect_test "project" =
  run_test "Select([f], r)";
  [%expect {|
    SELECT r_0."f" AS "f_1"
    FROM "r" AS "r_0" |}]

let%expect_test "filter" =
  run_test "Filter(f = g, r)";
  [%expect
    {|
      SELECT r_0."f" AS "f_0",
             r_0."g" AS "g_0"
      FROM "r" AS "r_0"
      WHERE ((r_0."f") = (r_0."g")) |}]

let%expect_test "eqjoin" =
  run_test
    "Join(r_f = s_g, select([f as r_f, g as r_g], r), select([f as s_f, g as \
     s_g], s))";
  [%expect
    {|
      SELECT r_1."f" AS "r_f_0",
             r_1."g" AS "r_g_0",
             s_0."f" AS "s_f_0",
             s_0."g" AS "s_g_0"
      FROM "r" AS "r_1",
           "s" AS "s_0"
      WHERE ((r_1."f") = (s_0."g")) |}]

let%expect_test "join-select" =
  run_test "join(true, select([id as p_id], log), select([id as c_id], log))";
  [%expect
    {|
        SELECT log_1."id" AS "p_id_0",
               log_0."id" AS "c_id_0"
        FROM "log" AS "log_1",
             "log" AS "log_0"
        WHERE (TRUE) |}]

let%expect_test "order-by" =
  run_test "OrderBy([f desc], Dedup(Select([f], r1)))";
  [%expect
    {|
    SELECT DISTINCT r1_0."f" AS "f_1"
    FROM "r1" AS "r1_0"
    ORDER BY r1_0."f" DESC |}]

let%expect_test "order-by" =
  run_test "OrderBy([0 desc], Dedup(Select([f], r1)))";
  [%expect {|
    SELECT DISTINCT r1_0."f" AS "f_1"
    FROM "r1" AS "r1_0" |}]

let%expect_test "dedup" =
  run_test "dedup(r)";
  [%expect
    {|
    SELECT DISTINCT r_0."f" AS "f_0",
                    r_0."g" AS "g_0"
    FROM "r" AS "r_0" |}]

let%expect_test "dedup" =
  run_test "Dedup(Select([f], r1))";
  [%expect {|
    SELECT DISTINCT r1_0."f" AS "f_1"
    FROM "r1" AS "r1_0" |}]

let%expect_test "select" =
  run_test "Select([f], r1)";
  [%expect {|
    SELECT r1_0."f" AS "f_1"
    FROM "r1" AS "r1_0" |}]

let%expect_test "scan" =
  run_test "r1";
  [%expect
    {|
    SELECT r1_0."f" AS "f_0",
           r1_0."g" AS "g_0"
    FROM "r1" AS "r1_0" |}]

let%expect_test "join" =
  run_test
    "join(p_counter < c_counter && c_counter < p_succ, \n\
    \          select([counter as p_counter, succ as p_succ], log),\n\
    \          select([counter as c_counter], log))";
  [%expect
    {|
      SELECT log_1."counter" AS "p_counter_0",
             log_1."succ" AS "p_succ_0",
             log_0."counter" AS "c_counter_0"
      FROM "log" AS "log_1",
           "log" AS "log_0"
      WHERE (((log_1."counter") < (log_0."counter"))
             AND ((log_0."counter") < (log_1."succ"))) |}]

let%expect_test "join-groupby" =
  run_test
    {|join(f = g || x = y, groupby([f, sum((f * g)) as x], [f], r1), groupby([g, sum((f * g)) as y], [g], r1))|};
  [%expect
    {|
    SELECT "f_2" AS "f_2_0",
           "x_0" AS "x_0_0",
           "g_1" AS "g_1_0",
           "y_0" AS "y_0_0"
    FROM
      (SELECT "f_1" AS "f_2",
              sum(("f_1") * ("g_2")) AS "x_0"
       FROM
         (SELECT r1_1."f" AS "f_1",
                 r1_1."g" AS "g_2"
          FROM "r1" AS "r1_1") AS "t1"
       GROUP BY "f_1") AS "t2",

      (SELECT "g_0" AS "g_1",
              sum(("f_0") * ("g_0")) AS "y_0"
       FROM
         (SELECT r1_0."f" AS "f_0",
                 r1_0."g" AS "g_0"
          FROM "r1" AS "r1_0") AS "t0"
       GROUP BY "g_0") AS "t3"
    WHERE ((("f_2") = ("g_1"))
           OR (("x_0") = ("y_0"))) |}]

let%expect_test "join-cond" =
  run_test
    {|filter(true||false, join(true&&false, select([f], r1), select([g], r)))|};
  [%expect
    {|
    SELECT r1_1."f" AS "f_2",
           r_0."g" AS "g_1"
    FROM "r1" AS "r1_1",
         "r" AS "r_0"
    WHERE ((TRUE)
           OR (FALSE))
      AND ((TRUE)
           AND (FALSE)) |}]

let%expect_test "select-groupby" =
  run_test "select([max(x) as m], groupby([f, sum((f * g)) as x], [f], r1))";
  [%expect
    {|
      SELECT max("x_0") AS "m_0"
      FROM
        (SELECT "f_0" AS "f_1",
                sum(("f_0") * ("g_0")) AS "x_0"
         FROM
           (SELECT r1_0."f" AS "f_0",
                   r1_0."g" AS "g_0"
            FROM "r1" AS "r1_0") AS "t0"
         GROUP BY "f_0") AS "t1" |}]

let%expect_test "select-fusion-1" =
  run_test "select([max(x) as m], select([min(f) as x], r1))";
  [%expect
    {|
      SELECT max("x_0") AS "m_0"
      FROM
        (SELECT min("f_0") AS "x_0"
         FROM
           (SELECT r1_0."f" AS "f_0",
                   r1_0."g" AS "g_0"
            FROM "r1" AS "r1_0") AS "t0") AS "t1" |}]

let%expect_test "select-fusion-2" =
  run_test "select([max(x) as m], select([f as x], r1))";
  [%expect
    {|
    SELECT max("x_0") AS "m_0"
    FROM
      (SELECT r1_0."f" AS "x_0"
       FROM "r1" AS "r1_0") AS "t0" |}]

let%expect_test "filter-fusion" =
  run_test "filter((x = 0), groupby([sum(f) as x], [g], r1))";
  [%expect
    {|
      SELECT "x_0" AS "x_0_0"
      FROM
        (SELECT sum("f_0") AS "x_0"
         FROM
           (SELECT r1_0."f" AS "f_0",
                   r1_0."g" AS "g_0"
            FROM "r1" AS "r1_0") AS "t0"
         GROUP BY "g_0") AS "t1"
      WHERE (("x_0") = (0)) |}]

let%expect_test "groupby-dedup" =
  run_test "groupby([sum(f) as x], [g], dedup(r1))";
  [%expect
    {|
    SELECT sum("f_0") AS "x_0"
    FROM
      (SELECT DISTINCT r1_0."f" AS "f_0",
                       r1_0."g" AS "g_0"
       FROM "r1" AS "r1_0") AS "t0"
    GROUP BY "g_0" |}]

let%expect_test "hash-idx" =
  run_test
    "ahashidx(select([f], r1) as k, select([g], filter(f = k.f, r1)), null)";
  [%expect
    {|
    SELECT "f_3" AS "f_3_0",
           "g_3" AS "g_3_0"
    FROM
      (SELECT r1_0."f" AS "f_1"
       FROM "r1" AS "r1_0") AS "t1",
         LATERAL
      (SELECT "f_1" AS "f_3",
              r1_1."g" AS "g_3"
       FROM "r1" AS "r1_1"
       WHERE (("f_1") = (NULL))
         AND ((r1_1."f") = ("f_1"))) AS "t0" |}]

let%expect_test "ordered-idx" =
  run_test
    "aorderedidx(select([f], r1) as k, select([g], filter(f = k.f, r1)), null, \
     null)";
  [%expect
    {|
    SELECT "f_3" AS "f_3_0",
           "g_3" AS "g_3_0"
    FROM
      (SELECT r1_0."f" AS "f_1"
       FROM "r1" AS "r1_0") AS "t1",
         LATERAL
      (SELECT "f" AS "f_3",
              r1_1."g" AS "g_3"
       FROM "r1" AS "r1_1"
       WHERE ((("f") >= (NULL))
              AND (("f") < (NULL)))
         AND ((r1_1."f") = ("f_1"))) AS "t0" |}]

let%expect_test "depjoin-agg-1" =
  run_test
    "depjoin(select([f, g], r) as k, select([min(k.f) as l, max(k.g) as h], \
     ascalar(0 as z)))";
  [%expect
    {|
    SELECT "l_0" AS "l_0_0",
           "h_0" AS "h_0_0"
    FROM
      (SELECT r_0."f" AS "f_1",
              r_0."g" AS "g_1"
       FROM "r" AS "r_0") AS "t3",
         LATERAL
      (SELECT min("f_1") AS "l_0",
              max("g_1") AS "h_0"
       FROM
         (SELECT "z_0" AS "z_0_0",
                 "f_1",
                 "g_1"
          FROM
            (SELECT 0 AS "z_0") AS "t0") AS "t1") AS "t2"
|}]

let%expect_test "depjoin-agg-2" =
  run_test
    "depjoin(select([f, g], r) as k, select([count() as c, f], ascalar(k.f)))";
  [%expect
    {|
    SELECT "c_0" AS "c_0_0",
           "f_3" AS "f_3_0"
    FROM
      (SELECT r_0."f" AS "f_1",
              r_0."g" AS "g_1"
       FROM "r" AS "r_0") AS "t2",
         LATERAL
      (SELECT count(*) AS "c_0",
              min("f_2") AS "f_3"
       FROM
         (SELECT "f_1" AS "f_2") AS "t0") AS "t1"
|}]

let%expect_test "select-agg-window" =
  run_test "select([count() as c, min(f) as m, row_number() as n], r)";
  [%expect
    {|
    SELECT count(*) AS "c_0",
           min("f_0") AS "m_0",
           row_number() OVER () AS "n_0"
    FROM
      (SELECT r_0."f" AS "f_0",
              r_0."g" AS "g_0"
       FROM "r" AS "r_0") AS "t0"
|}]

(* FIXME *)
(* let%expect_test "orderby-filter" = *)
(*   run_test ~conn:Test_util.tpch_conn *)
(* {| *)
   (* select([p_partkey], *)
   (*   dedup(select([p_partkey], orderby([p_partkey, p_type], filter(0 = 0, part))))) *)
   (* |} *)

let%expect_test "select-fusion-window" =
  run_test
    {|
    join(((k1_f = bnd0) &&
                              ((k1_g = bnd1) && (k1_rn0 = bnd2))),
                           select([k1_rn0 as bnd2,
                                   k1_f as bnd0,
                                   k1_g as bnd1],
                             select([rn0 as k1_rn0, f as k1_f, g as k1_g],
                               select([row_number() as rn0, f, g], r1))),
                           select([k1_rn0 as x0,
                                   k1_f as x1,
                                   k1_g as x2,
                                   f as x3,
                                   k1_f,
                                   k1_g,
                                   k1_rn0],
                             select([k1_f as f, k1_f, k1_g, k1_rn0],
                               dedup(
                                 select([k1_f, k1_g, k1_rn0],
                                   select([rn0 as k1_rn0,
                                           f as k1_f,
                                           g as k1_g],
                                     select([row_number() as rn0, f, g],
                                       r1)))))))
|};
  [%expect
    {|
    SELECT "bnd2_0" AS "bnd2_0_0",
           "bnd0_0" AS "bnd0_0_0",
           "bnd1_0" AS "bnd1_0_0",
           "x0_0" AS "x0_0_0",
           "x1_0" AS "x1_0_0",
           "x2_0" AS "x2_0_0",
           "x3_0" AS "x3_0_0",
           "k1_f_3" AS "k1_f_3_0",
           "k1_g_3" AS "k1_g_3_0",
           "k1_rn0_3" AS "k1_rn0_3_0"
    FROM
      (SELECT row_number() OVER () AS "bnd2_0",
                                "f_3" AS "bnd0_0",
                                "g_2" AS "bnd1_0"
       FROM
         (SELECT r1_1."f" AS "f_3",
                 r1_1."g" AS "g_2"
          FROM "r1" AS "r1_1") AS "t2") AS "t3",

      (SELECT "k1_rn0_1" AS "x0_0",
              "k1_f_1" AS "x1_0",
              "k1_g_1" AS "x2_0",
              "k1_f_1" AS "x3_0",
              "k1_f_1" AS "k1_f_3",
              "k1_g_1" AS "k1_g_3",
              "k1_rn0_1" AS "k1_rn0_3"
       FROM
         (SELECT DISTINCT "f_0" AS "k1_f_1",
                          "g_0" AS "k1_g_1",
                          row_number() OVER () AS "k1_rn0_1"
          FROM
            (SELECT r1_0."f" AS "f_0",
                    r1_0."g" AS "g_0"
             FROM "r1" AS "r1_0") AS "t0") AS "t1") AS "t4"
    WHERE ((("k1_f_3") = ("bnd0_0"))
           AND ((("k1_g_3") = ("bnd1_0"))
                AND (("k1_rn0_3") = ("bnd2_0")))) |}]
