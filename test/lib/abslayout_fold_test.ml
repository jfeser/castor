open Abslayout
open Abslayout_load
open Abslayout_fold
module Q = Fold_query

let test_conn = Test_util.test_db_conn |> Lazy.force

let%expect_test "" =
  let ralgebra =
    "alist(r1 as k, filter(k.f = g, ascalar(k.g)))" |> load_string_exn test_conn
  in
  let q = Q.of_ralgebra @@ Equiv.annotate ralgebra in
  let r = Q.to_ralgebra q in
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
      SELECT DISTINCT t1."x0" AS "x0",
                      t1."x1" AS "x1",
                      t1."x2" AS "x2",
                      t1."x3" AS "x3"
      FROM
        (SELECT count(*) AS "ct0",
                t0."f" AS "f",
                t0."g" AS "g"
         FROM
           (SELECT r1_0."f" AS "f",
                   r1_0."g" AS "g"
            FROM "r1" AS "r1_0") AS "t0"
         GROUP BY t0."f",
                  t0."g") AS "t2",
           LATERAL
        (SELECT "ct0" AS "x0",
                "f" AS "x1",
                "g" AS "x2",
                "g" AS "x3") AS "t1"
      ORDER BY t1."x1",
               t1."x2" |}]

let%expect_test "" =
  let ralgebra =
    {|
depjoin(ascalar(0 as f) as k, select([(k.f + g) as s], alist(r1 as k1, ascalar(k1.g))))
|}
    |> load_string_exn test_conn
  in
  let q = Q.of_ralgebra @@ Equiv.annotate ralgebra in
  let r = Q.to_ralgebra q in
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
      SELECT DISTINCT t6."counter0" AS "counter0",
                      t6."f" AS "f",
                      t6."x4" AS "x4",
                      t6."x5" AS "x5",
                      t6."x6" AS "x6",
                      t6."x7" AS "x7"
      FROM (
              (SELECT 0 AS "counter0",
                      0 AS "f",
                      (NULL::numeric) AS "x4",
                      (NULL::numeric) AS "x5",
                      (NULL::numeric) AS "x6",
                      (NULL::numeric) AS "x7")
            UNION ALL
              (SELECT 1 AS "counter0",
                      (NULL::numeric) AS "f",
                      t4."x4" AS "x4",
                      t4."x5" AS "x5",
                      t4."x6" AS "x6",
                      t4."x7" AS "x7"
               FROM
                 (SELECT count(*) AS "ct1",
                         t3."f" AS "f",
                         t3."g" AS "g"
                  FROM
                    (SELECT r1_1."f" AS "f",
                            r1_1."g" AS "g"
                     FROM "r1" AS "r1_1") AS "t3"
                  GROUP BY t3."f",
                           t3."g") AS "t5",
                    LATERAL
                 (SELECT "ct1" AS "x4",
                         "f" AS "x5",
                         "g" AS "x6",
                         "g" AS "x7") AS "t4")) AS "t6"
      ORDER BY t6."counter0",
               t6."x5",
               t6."x6" |}]

let%expect_test "" =
  let ralgebra = load_string_exn test_conn Test_util.sum_complex in
  let q = Q.of_ralgebra @@ Equiv.annotate ralgebra in
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
      SELECT DISTINCT t8."x8" AS "x8",
                      t8."x9" AS "x9",
                      t8."x10" AS "x10",
                      t8."x11" AS "x11",
                      t8."x12" AS "x12"
      FROM
        (SELECT count(*) AS "ct2",
                t7."f" AS "f",
                t7."g" AS "g"
         FROM
           (SELECT r1_2."f" AS "f",
                   r1_2."g" AS "g"
            FROM "r1" AS "r1_2") AS "t7"
         GROUP BY t7."f",
                  t7."g") AS "t9",
           LATERAL
        (SELECT "ct2" AS "x8",
                "f" AS "x9",
                "g" AS "x10",
                "f" AS "x11",
                ("g") - ("f") AS "x12"
         WHERE (TRUE)) AS "t8"
      ORDER BY t8."x9",
               t8."x10" |}]

let%expect_test "" =
  let ralgebra =
    load_string_exn test_conn "alist(r1 as k, alist(r1 as j, ascalar(j.f)))"
  in
  let q = Q.of_ralgebra @@ Equiv.annotate ralgebra in

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
        SELECT DISTINCT t14."x17" AS "x17",
                        t14."x18" AS "x18",
                        t14."x19" AS "x19",
                        t14."x20" AS "x20",
                        t14."x21" AS "x21",
                        t14."x22" AS "x22",
                        t14."x23" AS "x23"
        FROM
          (SELECT count(*) AS "ct3",
                  t10."f" AS "f",
                  t10."g" AS "g"
           FROM
             (SELECT r1_3."f" AS "f",
                     r1_3."g" AS "g"
              FROM "r1" AS "r1_3") AS "t10"
           GROUP BY t10."f",
                    t10."g") AS "t15",
             LATERAL
          (SELECT "ct3" AS "x17",
                  "f" AS "x18",
                  "g" AS "x19",
                  t12."x13" AS "x20",
                  t12."x14" AS "x21",
                  t12."x15" AS "x22",
                  t12."x16" AS "x23"
           FROM
             (SELECT count(*) AS "ct4",
                     t11."f" AS "f",
                     t11."g" AS "g"
              FROM
                (SELECT r1_4."f" AS "f",
                        r1_4."g" AS "g"
                 FROM "r1" AS "r1_4") AS "t11"
              GROUP BY t11."f",
                       t11."g") AS "t13",
                LATERAL
             (SELECT "ct4" AS "x13",
                     "f" AS "x14",
                     "g" AS "x15",
                     "f" AS "x16") AS "t12") AS "t14"
        ORDER BY t14."x18",
                 t14."x19",
                 t14."x21",
                 t14."x22" |}]
