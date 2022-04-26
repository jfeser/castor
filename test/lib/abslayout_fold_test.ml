open Abslayout
open Abslayout_load
open Abslayout_fold

let test_conn = Test_util.test_db_conn |> Lazy.force

let%expect_test "" =
  let ralgebra =
    "alist(r1 as k, filter(k.f = g, ascalar(k.g)))" |> load_string_exn test_conn
  in
  let q = Q.of_ralgebra ralgebra in
  let r = Q.to_ralgebra q in
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
      SELECT DISTINCT
          "x0_0" AS "x0_0_0",
          "x1_0" AS "x1_0_0",
          "x2_0" AS "x2_0_0",
          "x3_0" AS "x3_0_0"
      FROM (
          SELECT
              count(*) AS "ct0_0",
              "f_0" AS "f_1",
              "g_0" AS "g_1"
          FROM (
              SELECT
                  r1_0. "f" AS "f_0",
                  r1_0. "g" AS "g_0"
              FROM
                  "r1" AS "r1_0") AS "t0"
          GROUP BY
              "f_0",
              "g_0") AS "t2",
          LATERAL (
              SELECT
                  "ct0_0" AS "x0_0",
                  "f_1" AS "x1_0",
                  "g_1" AS "x2_0",
                  "g_1" AS "x3_0") AS "t1"
      ORDER BY
          "x1_0",
          "x2_0" |}]

let%expect_test "" =
  let ralgebra =
    "depjoin(ascalar(0 as f) as k, select([k.f + g], alist(r1 as k1, \
     ascalar(k1.g))))" |> load_string_exn test_conn
  in
  let q = Q.of_ralgebra ralgebra in
  let r = Q.to_ralgebra q in
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
      SELECT DISTINCT
          "counter0_0" AS "counter0_0_0",
          "f_3" AS "f_3_0",
          "x4_0" AS "x4_0_0",
          "x5_0" AS "x5_0_0",
          "x6_0" AS "x6_0_0",
          "x7_0" AS "x7_0_0"
      FROM ((
              SELECT
                  0 AS "counter0_0",
                  0 AS "f_3",
                  (NULL::numeric) AS "x4_0",
                  (NULL::numeric) AS "x5_0",
                  (NULL::numeric) AS "x6_0",
                  (NULL::numeric) AS "x7_0")
          UNION ALL (
              SELECT
                  1 AS "counter0_1",
                  (NULL::numeric) AS "f_6",
                  "x4_1" AS "x4_2",
                  "x5_1" AS "x5_2",
                  "x6_1" AS "x6_2",
                  "x7_1" AS "x7_2"
              FROM (
                  SELECT
                      count(*) AS "ct1_0",
                      "f_4" AS "f_5",
                      "g_3" AS "g_4"
                  FROM (
                      SELECT
                          r1_1. "f" AS "f_4",
                          r1_1. "g" AS "g_3"
                      FROM
                          "r1" AS "r1_1") AS "t3"
                  GROUP BY
                      "f_4",
                      "g_3") AS "t5",
                  LATERAL (
                      SELECT
                          "ct1_0" AS "x4_1",
                          "f_5" AS "x5_1",
                          "g_4" AS "x6_1",
                          "g_4" AS "x7_1") AS "t4")) AS "t6"
      ORDER BY
          "counter0_0",
          "x5_0",
          "x6_0" |}]

let%expect_test "" =
  let ralgebra = load_string_exn test_conn Test_util.sum_complex in
  let q = Q.of_ralgebra ralgebra in
  let r = Q.to_ralgebra q in
  Format.printf "%a" pp r;
  [%expect
    {|
      orderby([x9, x10],
        dedup(
          depjoin(groupby([count() as ct2, f, g], [f, g], r1) as k,
            select([k.ct2 as x8, k.f as x9, k.g as x10, f as x11, v as x12],
              atuple([ascalar(k.f), ascalar((k.g - k.f) as v)], cross))))) |}];
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
      SELECT DISTINCT
          "x8_0" AS "x8_0_0",
          "x9_0" AS "x9_0_0",
          "x10_0" AS "x10_0_0",
          "x11_0" AS "x11_0_0",
          "x12_0" AS "x12_0_0"
      FROM (
          SELECT
              count(*) AS "ct2_0",
              "f_7" AS "f_8",
              "g_6" AS "g_7"
          FROM (
              SELECT
                  r1_2. "f" AS "f_7",
                  r1_2. "g" AS "g_6"
              FROM
                  "r1" AS "r1_2") AS "t7"
          GROUP BY
              "f_7",
              "g_6") AS "t9",
          LATERAL (
              SELECT
                  "ct2_0" AS "x8_0",
                  "f_8" AS "x9_0",
                  "g_7" AS "x10_0",
                  "f_8" AS "x11_0",
                  ("g_7") - ("f_8") AS "x12_0"
              WHERE (TRUE)) AS "t8"
      ORDER BY
          "x9_0",
          "x10_0" |}]

let%expect_test "" =
  let ralgebra =
    load_string_exn test_conn "alist(r1 as k, alist(r1 as j, ascalar(j.f)))"
  in
  let q = Q.of_ralgebra ralgebra in

  let r = Q.to_ralgebra q in
  Format.printf "%a" pp r;
  [%expect
    {|
        orderby([x18, x19, x21, x22],
          dedup(
            depjoin(groupby([count() as ct3, f, g], [f, g], r1) as k,
              select([k.ct3 as x17, k.f as x18, k.g as x19, x13 as x20, x14 as x21,
                      x15 as x22, x16 as x23],
                depjoin(groupby([count() as ct4, f, g], [f, g], r1) as j,
                  select([j.ct4 as x13, j.f as x14, j.g as x15, f as x16],
                    atuple([ascalar(j.f)], cross))))))) |}];
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
        SELECT DISTINCT
            "x17_0" AS "x17_0_0",
            "x18_0" AS "x18_0_0",
            "x19_0" AS "x19_0_0",
            "x20_0" AS "x20_0_0",
            "x21_0" AS "x21_0_0",
            "x22_0" AS "x22_0_0",
            "x23_0" AS "x23_0_0"
        FROM (
            SELECT
                count(*) AS "ct3_0",
                "f_10" AS "f_11",
                "g_8" AS "g_9"
            FROM (
                SELECT
                    r1_3. "f" AS "f_10",
                    r1_3. "g" AS "g_8"
                FROM
                    "r1" AS "r1_3") AS "t10"
            GROUP BY
                "f_10",
                "g_8") AS "t15",
            LATERAL (
                SELECT
                    "ct3_0" AS "x17_0",
                    "f_11" AS "x18_0",
                    "g_9" AS "x19_0",
                    "x13_0" AS "x20_0",
                    "x14_0" AS "x21_0",
                    "x15_0" AS "x22_0",
                    "x16_0" AS "x23_0"
                FROM (
                    SELECT
                        count(*) AS "ct4_0",
                        "f_12" AS "f_13",
                        "g_10" AS "g_11"
                    FROM (
                        SELECT
                            r1_4. "f" AS "f_12",
                            r1_4. "g" AS "g_10"
                        FROM
                            "r1" AS "r1_4") AS "t11"
                    GROUP BY
                        "f_12",
                        "g_10") AS "t13",
                    LATERAL (
                        SELECT
                            "ct4_0" AS "x13_0",
                            "f_13" AS "x14_0",
                            "g_11" AS "x15_0",
                            "f_13" AS "x16_0") AS "t12") AS "t14"
        ORDER BY
            "x18_0",
            "x19_0",
            "x21_0",
            "x22_0" |}]
