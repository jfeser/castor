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
    SELECT DISTINCT r_0."f" AS "f_0",
                    r_0."g" AS "g_0"
    FROM "r" AS "r_0";

    ANALYZE r0;

    SELECT "f_2" AS "f_2_0",
           "g_2" AS "g_2_0",
           "m_0" AS "m_0_0"
    FROM
      (SELECT r0_2."f" AS "f_2",
              r0_2."g" AS "g_2"
       FROM "r0" AS "r0_2") AS "t1",

      (SELECT min("f_1") AS "m_0"
       FROM
         (SELECT r0_1."f" AS "f_1",
                 r0_1."g" AS "g_1"
          FROM "r0" AS "r0_1") AS "t0") AS "t2"
    WHERE (TRUE);

    COMMIT |}]
