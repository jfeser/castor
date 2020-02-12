open! Core
open Abslayout
open Abslayout_load
open Abslayout_fold

let test_conn = Test_util.test_db_conn |> Lazy.force

let%expect_test "" =
  let ralgebra =
    "alist(r1 as k, filter(k.f = g, ascalar(k.g)))" |> load_string test_conn
    |> Abslayout_visitors.map_meta (fun _ -> Meta.empty ())
  in
  let q = Q.of_ralgebra ralgebra in
  let r = Q.to_ralgebra q in
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
      SELECT
          "x0_0" AS "x0_0_0",
          "x1_0" AS "x1_0_0",
          "x2_0" AS "x2_0_0",
          "x3_0" AS "x3_0_0"
      FROM (
          SELECT
              row_number() OVER () AS "rn0_0",
              r1_0. "f" AS "f_1",
              r1_0. "g" AS "g_1"
          FROM
              "r1" AS "r1_0") AS "t1",
          LATERAL (
              SELECT
                  "rn0_0" AS "x0_0",
                  "f_1" AS "x1_0",
                  "g_1" AS "x2_0",
                  "g_1" AS "x3_0") AS "t0"
      ORDER BY
          "x1_0",
          "x2_0",
          "x0_0" |}]

let%expect_test "" =
  let ralgebra =
    "depjoin(ascalar(0 as f) as k, select([k.f + g], alist(r1 as k1, \
     ascalar(k1.g))))" |> load_string test_conn
    |> Abslayout_visitors.map_meta (fun _ -> Meta.empty ())
  in
  let q = Q.of_ralgebra ralgebra in
  let r = Q.to_ralgebra q in
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
      SELECT
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
                  (NULL::integer) AS "x4_0",
                  (NULL::integer) AS "x5_0",
                  (NULL::integer) AS "x6_0",
                  (NULL::integer) AS "x7_0")
          UNION ALL (
              SELECT
                  1 AS "counter0_1",
                  (NULL::integer) AS "f_6",
                  "x4_1" AS "x4_2",
                  "x5_1" AS "x5_2",
                  "x6_1" AS "x6_2",
                  "x7_1" AS "x7_2"
              FROM (
                  SELECT
                      row_number() OVER () AS "rn1_0",
                      r1_1. "f" AS "f_5",
                      r1_1. "g" AS "g_4"
                  FROM
                      "r1" AS "r1_1") AS "t3",
                  LATERAL (
                      SELECT
                          "rn1_0" AS "x4_1",
                          "f_5" AS "x5_1",
                          "g_4" AS "x6_1",
                          "g_4" AS "x7_1") AS "t2")) AS "t4"
      ORDER BY
          "counter0_0",
          "x5_0",
          "x6_0",
          "x4_0" |}]

let%expect_test "" =
  let ralgebra =
    load_string test_conn Test_util.sum_complex
    |> Abslayout_visitors.map_meta (fun _ -> Meta.empty ())
  in
  let q = Q.of_ralgebra ralgebra in
  let r = Q.to_ralgebra q in
  Format.printf "%a" pp r;
  [%expect
    {|
      orderby([x9, x10, x8],
        depjoin(select([row_number() as rn2, f, g], r1) as k,
          select([k.rn2 as x8, k.f as x9, k.g as x10, f as x11, v as x12],
            atuple([ascalar(k.f), ascalar((k.g - k.f) as v)], cross)))) |}];
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
      SELECT
          "x8_0" AS "x8_0_0",
          "x9_0" AS "x9_0_0",
          "x10_0" AS "x10_0_0",
          "x11_0" AS "x11_0_0",
          "x12_0" AS "x12_0_0"
      FROM (
          SELECT
              row_number() OVER () AS "rn2_0",
              r1_2. "f" AS "f_8",
              r1_2. "g" AS "g_7"
          FROM
              "r1" AS "r1_2") AS "t6",
          LATERAL (
              SELECT
                  "rn2_0" AS "x8_0",
                  "f_8" AS "x9_0",
                  "g_7" AS "x10_0",
                  "f_8" AS "x11_0",
                  ("g_7") - ("f_8") AS "x12_0"
              WHERE (TRUE)) AS "t5"
      ORDER BY
          "x9_0",
          "x10_0",
          "x8_0" |}]

let%expect_test "" =
  let ralgebra =
    load_string test_conn "alist(r1 as k, alist(r1 as j, ascalar(j.f)))"
    |> Abslayout_visitors.map_meta (fun _ -> Meta.empty ())
  in
  let q = Q.of_ralgebra ralgebra in

  let r = Q.to_ralgebra q in
  Format.printf "%a" pp r;
  [%expect
    {|
        orderby([x18, x19, x17, x21, x22, x20],
          depjoin(select([row_number() as rn3, f, g], r1) as k,
            select([k.rn3 as x17,
                    k.f as x18,
                    k.g as x19,
                    x13 as x20,
                    x14 as x21,
                    x15 as x22,
                    x16 as x23],
              depjoin(select([row_number() as rn4, f, g], r1) as j,
                select([j.rn4 as x13, j.f as x14, j.g as x15, f as x16],
                  atuple([ascalar(j.f)], cross)))))) |}];
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
        SELECT
            "x17_0" AS "x17_0_0",
            "x18_0" AS "x18_0_0",
            "x19_0" AS "x19_0_0",
            "x20_0" AS "x20_0_0",
            "x21_0" AS "x21_0_0",
            "x22_0" AS "x22_0_0",
            "x23_0" AS "x23_0_0"
        FROM (
            SELECT
                row_number() OVER () AS "rn3_0",
                r1_3. "f" AS "f_11",
                r1_3. "g" AS "g_9"
            FROM
                "r1" AS "r1_3") AS "t10",
            LATERAL (
                SELECT
                    "rn3_0" AS "x17_0",
                    "f_11" AS "x18_0",
                    "g_9" AS "x19_0",
                    "x13_0" AS "x20_0",
                    "x14_0" AS "x21_0",
                    "x15_0" AS "x22_0",
                    "x16_0" AS "x23_0"
                FROM (
                    SELECT
                        row_number() OVER () AS "rn4_0",
                        r1_4. "f" AS "f_13",
                        r1_4. "g" AS "g_11"
                    FROM
                        "r1" AS "r1_4") AS "t8",
                    LATERAL (
                        SELECT
                            "rn4_0" AS "x13_0",
                            "f_13" AS "x14_0",
                            "g_11" AS "x15_0",
                            "f_13" AS "x16_0") AS "t7") AS "t9"
        ORDER BY
            "x18_0",
            "x19_0",
            "x17_0",
            "x21_0",
            "x22_0",
            "x20_0" |}]
