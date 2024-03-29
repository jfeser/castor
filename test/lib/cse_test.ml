open Test_util
open Cse

let%expect_test "" =
  let r =
    "join(true, dedup(r), select([min(f) as m], dedup(r)))"
    |> Abslayout_load.load_string_exn (Lazy.force test_db_schema)
  in
  let common = extract_common r in
  Format.printf "%a" Fmt.Dump.(list @@ pair string Abslayout_pp.pp) common.views;
  [%expect {|
       [("r0", dedup(r))] |}];
  common |> to_sql |> Sql.format |> print_endline;
  [%expect
    {|
       BEGIN
       CREATE
       TEMPORARY TABLE r0 AS
       SELECT DISTINCT "r"."f" AS "f",
                       "r"."g" AS "g"
       FROM "r";

       ANALYZE r0;

       SELECT "t1"."f" AS "f",
              "t1"."g" AS "g",
              "t2"."m" AS "m"
       FROM
         (SELECT "r0"."f" AS "f",
                 "r0"."g" AS "g"
          FROM "r0") AS "t1",

         (SELECT min("t0"."f") AS "m"
          FROM
            (SELECT "r0"."f" AS "f",
                    "r0"."g" AS "g"
             FROM "r0") AS "t0") AS "t2"
       WHERE (TRUE);

       COMMIT |}]
