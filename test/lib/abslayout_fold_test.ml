open Abslayout
open Abslayout_load
open Abslayout_fold
module Q = Fold_query

let test_schema = Test_util.test_db_schema |> Lazy.force

let%expect_test "" =
  let ralgebra =
    "alist(r1, filter(0.f = g, ascalar(0.g)))" |> load_string_exn test_schema
  in
  let q = Q.of_ralgebra @@ Equiv.annotate ralgebra in
  let r = Q.to_ralgebra q in
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
         SELECT DISTINCT "t2"."x0" AS "x0",
                         "t2"."x1" AS "x1",
                         "t2"."x2" AS "x2",
                         "t2"."x3" AS "x3"
         FROM
           (SELECT count(*) AS "ct0",
                   "t0"."f" AS "f",
                   "t0"."g" AS "g"
            FROM
              (SELECT "r1"."f" AS "f",
                      "r1"."g" AS "g"
               FROM "r1") AS "t0"
            GROUP BY "t0"."f",
                     "t0"."g") AS "t1",
              LATERAL
           (SELECT "t1"."ct0" AS "x0",
                   "t1"."f" AS "x1",
                   "t1"."g" AS "x2",
                   "t1"."g" AS "x3") AS "t2"
         ORDER BY "t2"."x1",
                  "t2"."x2" |}]

let%expect_test "" =
  let ralgebra =
    {|
   depjoin(ascalar(0 as f), select([(0.f + g) as s], alist(r1, ascalar(0.g))))
   |}
    |> load_string_exn test_schema
  in
  let q = Q.of_ralgebra @@ Equiv.annotate ralgebra in
  let r = Q.to_ralgebra q in
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
         SELECT DISTINCT "t6"."counter0" AS "counter0",
                         "t6"."f" AS "f",
                         "t6"."x4" AS "x4",
                         "t6"."x5" AS "x5",
                         "t6"."x6" AS "x6",
                         "t6"."x7" AS "x7"
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
                         "t5"."x4" AS "x4",
                         "t5"."x5" AS "x5",
                         "t5"."x6" AS "x6",
                         "t5"."x7" AS "x7"
                  FROM
                    (SELECT count(*) AS "ct1",
                            "t3"."f" AS "f",
                            "t3"."g" AS "g"
                     FROM
                       (SELECT "r1"."f" AS "f",
                               "r1"."g" AS "g"
                        FROM "r1") AS "t3"
                     GROUP BY "t3"."f",
                              "t3"."g") AS "t4",
                       LATERAL
                    (SELECT "t4"."ct1" AS "x4",
                            "t4"."f" AS "x5",
                            "t4"."g" AS "x6",
                            "t4"."g" AS "x7") AS "t5")) AS "t6"
         ORDER BY "t6"."counter0",
                  "t6"."x5",
                  "t6"."x6" |}]

let%expect_test "" =
  let ralgebra = load_string_exn test_schema Test_util.sum_complex in
  let q = Q.of_ralgebra @@ Equiv.annotate ralgebra in
  let r = Q.to_ralgebra q in
  Format.printf "%a" pp r;
  [%expect
    {|
         orderby([x9, x10],
           dedup(
             depjoin(groupby([count() as ct2, f, g], [f, g], r1),
               select([0.ct2 as x8, 0.f as x9, 0.g as x10, f as x11, v as x12],
                 atuple([ascalar(0.f), ascalar((0.g - 0.f) as v)], cross))))) |}];
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
         SELECT DISTINCT "t9"."x8" AS "x8",
                         "t9"."x9" AS "x9",
                         "t9"."x10" AS "x10",
                         "t9"."x11" AS "x11",
                         "t9"."x12" AS "x12"
         FROM
           (SELECT count(*) AS "ct2",
                   "t7"."f" AS "f",
                   "t7"."g" AS "g"
            FROM
              (SELECT "r1"."f" AS "f",
                      "r1"."g" AS "g"
               FROM "r1") AS "t7"
            GROUP BY "t7"."f",
                     "t7"."g") AS "t8",
              LATERAL
           (SELECT "t8"."ct2" AS "x8",
                   "t8"."f" AS "x9",
                   "t8"."g" AS "x10",
                   "t8"."f" AS "x11",
                   ("t8"."g") - ("t8"."f") AS "x12"
            WHERE (TRUE)) AS "t9"
         ORDER BY "t9"."x9",
                  "t9"."x10" |}]

let%expect_test "" =
  let ralgebra =
    load_string_exn test_schema "alist(r1, alist(r1, ascalar(0.f)))"
  in
  let q = Q.of_ralgebra @@ Equiv.annotate ralgebra in

  let r = Q.to_ralgebra q in
  Format.printf "%a" pp r;
  [%expect
    {|
           orderby([x18, x19, x21, x22],
             dedup(
               depjoin(groupby([count() as ct3, f, g], [f, g], r1),
                 select([0.ct3 as x17, 0.f as x18, 0.g as x19, x13 as x20, x14 as x21,
                         x15 as x22, x16 as x23],
                   depjoin(groupby([count() as ct4, f, g], [f, g], r1),
                     select([0.ct4 as x13, 0.f as x14, 0.g as x15, f as x16],
                       atuple([ascalar(0.f)], cross))))))) |}];
  let sql = Sql.of_ralgebra r in
  printf "%s" (Sql.to_string_hum sql);
  [%expect
    {|
           SELECT DISTINCT "t15"."x17" AS "x17",
                           "t15"."x18" AS "x18",
                           "t15"."x19" AS "x19",
                           "t15"."x20" AS "x20",
                           "t15"."x21" AS "x21",
                           "t15"."x22" AS "x22",
                           "t15"."x23" AS "x23"
           FROM
             (SELECT count(*) AS "ct3",
                     "t10"."f" AS "f",
                     "t10"."g" AS "g"
              FROM
                (SELECT "r1"."f" AS "f",
                        "r1"."g" AS "g"
                 FROM "r1") AS "t10"
              GROUP BY "t10"."f",
                       "t10"."g") AS "t11",
                LATERAL
             (SELECT "t11"."ct3" AS "x17",
                     "t11"."f" AS "x18",
                     "t11"."g" AS "x19",
                     "t14"."x13" AS "x20",
                     "t14"."x14" AS "x21",
                     "t14"."x15" AS "x22",
                     "t14"."x16" AS "x23"
              FROM
                (SELECT count(*) AS "ct4",
                        "t12"."f" AS "f",
                        "t12"."g" AS "g"
                 FROM
                   (SELECT "r1"."f" AS "f",
                           "r1"."g" AS "g"
                    FROM "r1") AS "t12"
                 GROUP BY "t12"."f",
                          "t12"."g") AS "t13",
                   LATERAL
                (SELECT "t13"."ct4" AS "x13",
                        "t13"."f" AS "x14",
                        "t13"."g" AS "x15",
                        "t13"."f" AS "x16") AS "t14") AS "t15"
           ORDER BY "t15"."x18",
                    "t15"."x19",
                    "t15"."x21",
                    "t15"."x22" |}]
