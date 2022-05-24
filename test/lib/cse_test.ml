open Test_util
open Cse

let%expect_test "" =
  let r =
    "join(true, dedup(r), select([min(f) as m], dedup(r)))"
    |> Abslayout_load.load_string_exn (Lazy.force test_db_conn)
  in
  let common = extract_common r in
  Format.printf "%a" Fmt.Dump.(list @@ pair string A.pp) common.views;
  [%expect {|
    [("r0", dedup(r))] |}];
  common |> to_sql |> Sql.format |> print_endline;
  [%expect
    {|
    BEGIN
    CREATE
    TEMPORARY TABLE r0 AS
    SELECT DISTINCT r_0."f" AS "f",
                    r_0."g" AS "g"
    FROM "r" AS "r_0";

    ANALYZE r0;

    SELECT "f",
           "g",
           "m"
    FROM
      (SELECT r0_2."f" AS "f",
              r0_2."g" AS "g"
       FROM "r0" AS "r0_2") AS "t1",

      (SELECT min("f") AS "m"
       FROM
         (SELECT r0_1."f" AS "f",
                 r0_1."g" AS "g"
          FROM "r0" AS "r0_1") AS "t0") AS "t2"
    WHERE (TRUE);

    COMMIT |}]
