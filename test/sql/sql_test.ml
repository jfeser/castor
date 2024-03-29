open Sql
open Test_util
open Abslayout_load

let run_test ?(conn = Test_util.test_db_conn) s =
  Logs.Src.set_level src (Some Debug);
  let conn = Lazy.force conn in
  let r = load_string_exn (Db.schema conn) s in
  let sql_str = of_ralgebra r |> to_string_hum in
  (match Db.check conn sql_str with
  | Ok () -> ()
  | Error e -> print_endline (Error.to_string_hum e));
  print_endline sql_str;
  Logs.Src.set_level src None

let sexp_diff_display = Sexp_diff.Display.Display_options.create Two_column

let run_eval_test ?(conn = Test_util.test_db_conn) s =
  let conn = Lazy.force conn in
  let r = load_string_exn (Db.schema conn) s in

  let sort_relation = List.sort ~compare:[%compare: Value.t list] in
  let expected =
    let scan r =
      let names =
        Relation.schema r |> List.map ~f:(fun n -> Name.create (Name.name n))
      in
      Db.scan_exn conn r |> List.map ~f:(List.zip_exn names)
    in
    Eval.eval scan [] r
    |> List.map ~f:(List.map ~f:Tuple.T2.get2)
    |> sort_relation
  in

  let schema = Schema.schema r in
  let types = List.map schema ~f:Name.type_exn in

  let sql = of_ralgebra r in
  let sql_str = to_string sql in
  (match Db.check conn sql_str with
  | Ok () -> ()
  | Error e ->
      print_endline (to_string_hum sql);
      print_endline (Error.to_string_hum e));

  let actual = Db.exec_exn conn types sql_str |> sort_relation in
  if not ([%equal: Value.t list list] expected actual) then (
    let diff =
      Sexp_diff.Algo.diff
        ~original:([%sexp_of: Value.t list list] expected)
        ~updated:([%sexp_of: Value.t list list] actual)
        ()
    in
    print_endline
    @@ Sexp_diff.Display.display_with_ansi_colors sexp_diff_display diff;
    print_endline (to_string_hum sql);
    raise_s
      [%message (expected : Value.t list list) (actual : Value.t list list)])

let%test_unit "" = run_eval_test "groupby([count() as ct1, f, g], [f, g], r1)"
let%test_unit "" = run_eval_test "depjoin(r1, ascalar(0.f))"
let%test_unit "" = run_eval_test "depjoin(r1, atuple([ascalar(0.f)], cross))"

let%test_unit "" =
  run_eval_test
    "depjoin(groupby([count() as ct1, f, g], [f, g], r1),\n\
    \  select([0.ct1 as x0, 0.f as x1, 0.g as x2, f as x3],\n\
    \    atuple([ascalar(0.f)], cross)))"

let%test_unit "" =
  run_eval_test
    "depjoin(r1, select([0.f as x5, x0 as x7],\n\
    \  depjoin(r1, ascalar(0.f as x0))))"

(* let%test_unit "" = *)
(*   run_eval_test *)
(* "orderby([x5, x6, x8, x9],\n\ *)
   (*     \  dedup(\n\ *)
   (*     \    depjoin(groupby([count() as ct0, f, g], [f, g], r1),\n\ *)
   (*     \      select([0.ct0 as x4, 0.f as x5, 0.g as x6, x0 as x7, x1 as x8,\n\ *)
   (*     \              x2 as x9, x3 as x10],\n\ *)
   (*     \        depjoin(groupby([count() as ct1, f, g], [f, g], r1),\n\ *)
   (*     \          select([0.ct1 as x0, 0.f as x1, 0.g as x2, f as x3],\n\ *)
   (*     \            atuple([ascalar(0.f)], cross)))))))\n" *)

let%expect_test "select-agg" =
  run_test "select([(0.2 * avg(f)) as test], r)";
  [%expect
    {|
       SELECT (0.2) * (avg("t0"."f")) AS "test"
       FROM
         (SELECT "r"."f" AS "f",
                 "r"."g" AS "g"
          FROM "r") AS "t0" |}]

let%expect_test "project" =
  run_test "Select([f], r)";
  [%expect {|
       SELECT "r"."f" AS "f"
       FROM "r" |}]

let%expect_test "filter" =
  run_test "Filter(f = g, r)";
  [%expect
    {|
         SELECT "r"."f" AS "f",
                "r"."g" AS "g"
         FROM "r"
         WHERE (("r"."f") = ("r"."g")) |}]

let%expect_test "eqjoin" =
  run_test
    "Join(r_f = s_g, select([f as r_f, g as r_g], r), select([f as s_f, g as \
     s_g], s))";
  [%expect
    {|
         SELECT "r"."f" AS "r_f",
                "r"."g" AS "r_g",
                "s"."f" AS "s_f",
                "s"."g" AS "s_g"
         FROM "r",
              "s"
         WHERE (("r"."f") = ("s"."g")) |}]

let%expect_test "join-select" =
  run_test "join(true, select([id as p_id], log), select([id as c_id], log))";
  [%expect
    {|
           SELECT "log"."id" AS "p_id",
                  "log_0"."id" AS "c_id"
           FROM "log",
                "log" AS "log_0"
           WHERE (TRUE) |}]

let%expect_test "order-by" =
  run_test "OrderBy([f desc], Dedup(Select([f], r1)))";
  [%expect
    {|
       SELECT DISTINCT "r1"."f" AS "f"
       FROM "r1"
       ORDER BY "r1"."f" DESC |}]

let%expect_test "order-by" =
  run_test "OrderBy([0 desc], Dedup(Select([f], r1)))";
  [%expect {|
       SELECT DISTINCT "r1"."f" AS "f"
       FROM "r1" |}]

let%expect_test "dedup" =
  run_test "dedup(r)";
  [%expect
    {|
       SELECT DISTINCT "r"."f" AS "f",
                       "r"."g" AS "g"
       FROM "r" |}]

let%expect_test "dedup" =
  run_test "Dedup(Select([f], r1))";
  [%expect {|
       SELECT DISTINCT "r1"."f" AS "f"
       FROM "r1" |}]

let%expect_test "select" =
  run_test "Select([f], r1)";
  [%expect {|
       SELECT "r1"."f" AS "f"
       FROM "r1" |}]

let%expect_test "scan" =
  run_test "r1";
  [%expect
    {|
       SELECT "r1"."f" AS "f",
              "r1"."g" AS "g"
       FROM "r1" |}]

let%expect_test "join" =
  run_test
    "join(p_counter < c_counter && c_counter < p_succ, \n\
    \          select([counter as p_counter, succ as p_succ], log),\n\
    \          select([counter as c_counter], log))";
  [%expect
    {|
         SELECT "log"."counter" AS "p_counter",
                "log"."succ" AS "p_succ",
                "log_0"."counter" AS "c_counter"
         FROM "log",
              "log" AS "log_0"
         WHERE ((("log"."counter") < ("log_0"."counter"))
                AND (("log_0"."counter") < ("log"."succ"))) |}]

let%expect_test "join-groupby" =
  run_test
    {|join(f = g || x = y, groupby([f, sum((f * g)) as x], [f], r1), groupby([g, sum((f * g)) as y], [g], r1))|};
  [%expect
    {|
       SELECT "t2"."f" AS "f",
              "t2"."x" AS "x",
              "t3"."g" AS "g",
              "t3"."y" AS "y"
       FROM
         (SELECT "t1"."f" AS "f",
                 sum(("t1"."f") * ("t1"."g")) AS "x"
          FROM
            (SELECT "r1"."f" AS "f",
                    "r1"."g" AS "g"
             FROM "r1") AS "t1"
          GROUP BY "t1"."f") AS "t2",

         (SELECT "t0"."g" AS "g",
                 sum(("t0"."f") * ("t0"."g")) AS "y"
          FROM
            (SELECT "r1"."f" AS "f",
                    "r1"."g" AS "g"
             FROM "r1") AS "t0"
          GROUP BY "t0"."g") AS "t3"
       WHERE ((("t2"."f") = ("t3"."g"))
              OR (("t2"."x") = ("t3"."y"))) |}]

let%expect_test "join-cond" =
  run_test
    {|filter(true||false, join(true&&false, select([f], r1), select([g], r)))|};
  [%expect
    {|
       SELECT "r1"."f" AS "f",
              "r"."g" AS "g"
       FROM "r1",
            "r"
       WHERE ((TRUE)
              OR (FALSE))
         AND ((TRUE)
              AND (FALSE)) |}]

let%expect_test "select-groupby" =
  run_test "select([max(x) as m], groupby([f, sum((f * g)) as x], [f], r1))";
  [%expect
    {|
         SELECT max("t1"."x") AS "m"
         FROM
           (SELECT "t0"."f" AS "f",
                   sum(("t0"."f") * ("t0"."g")) AS "x"
            FROM
              (SELECT "r1"."f" AS "f",
                      "r1"."g" AS "g"
               FROM "r1") AS "t0"
            GROUP BY "t0"."f") AS "t1" |}]

let%expect_test "select-fusion-1" =
  run_test "select([max(x) as m], select([min(f) as x], r1))";
  [%expect
    {|
         SELECT max("t1"."x") AS "m"
         FROM
           (SELECT min("t0"."f") AS "x"
            FROM
              (SELECT "r1"."f" AS "f",
                      "r1"."g" AS "g"
               FROM "r1") AS "t0") AS "t1" |}]

let%expect_test "select-fusion-2" =
  run_test "select([max(x) as m], select([f as x], r1))";
  [%expect
    {|
       SELECT max("t0"."x") AS "m"
       FROM
         (SELECT "r1"."f" AS "x"
          FROM "r1") AS "t0" |}]

let%expect_test "filter-fusion" =
  run_test "filter((x = 0), groupby([sum(f) as x], [g], r1))";
  [%expect
    {|
         SELECT "t1"."x" AS "x"
         FROM
           (SELECT sum("t0"."f") AS "x"
            FROM
              (SELECT "r1"."f" AS "f",
                      "r1"."g" AS "g"
               FROM "r1") AS "t0"
            GROUP BY "t0"."g") AS "t1"
         WHERE (("t1"."x") = (0)) |}]

let%expect_test "groupby-dedup" =
  run_test "groupby([sum(f) as x], [g], dedup(r1))";
  [%expect
    {|
       SELECT sum("t0"."f") AS "x"
       FROM
         (SELECT DISTINCT "r1"."f" AS "f",
                          "r1"."g" AS "g"
          FROM "r1") AS "t0"
       GROUP BY "t0"."g" |}]

let%expect_test "hash-idx" =
  run_test "ahashidx(select([f], r1), select([g], filter(f = 0.f, r1)), null)";
  [%expect
    {|
       SELECT "t1"."f" AS "f",
              "t1"."g" AS "g"
       FROM
         (SELECT "r1"."f" AS "f"
          FROM "r1") AS "t0",
            LATERAL
         (SELECT "t0"."f" AS "f",
                 "r1"."g" AS "g"
          FROM "r1"
          WHERE (("t0"."f") = (NULL))
            AND (("r1"."f") = ("t0"."f"))) AS "t1" |}]

let%expect_test "ordered-idx" =
  run_test
    "aorderedidx(select([f], r1), select([g], filter(f = 0.f, r1)), null, null)";
  [%expect
    {|
       SELECT "t1"."f" AS "f",
              "t1"."g" AS "g"
       FROM
         (SELECT "r1"."f" AS "f"
          FROM "r1") AS "t0",
            LATERAL
         (SELECT "f",
                 "r1"."g" AS "g"
          FROM "r1"
          WHERE ((("f") >= (NULL))
                 AND (("f") < (NULL)))
            AND (("r1"."f") = ("t0"."f"))) AS "t1" |}]

(* let%expect_test "depjoin-agg-1" = *)
(*   run_test *)
(* "depjoin(select([f, g], r), select([min(0.f) as l, max(0.g) as h], \ *)
   (*      ascalar(0 as z)))"; *)
(*   [%expect *)
(* {| *)
   (*        SELECT "t2"."l" AS "l", *)
   (*               "t2"."h" AS "h" *)
   (*        FROM *)
   (*          (SELECT "r"."f" AS "f", *)
   (*                  "r"."g" AS "g" *)
   (*           FROM "r") AS "t3", *)
   (*             LATERAL *)
   (*          (SELECT min("f") AS "l", *)
   (*                  max("g") AS "h" *)
   (*           FROM *)
   (*             (SELECT "t0"."z" AS "z", *)
   (*                     "f", *)
   (*                     "g" *)
   (*              FROM *)
   (*                (SELECT 0 AS "z") AS "t0") AS "t1") AS "t2" *)
   (*    |}] *)

let%expect_test "depjoin-agg-2" =
  run_test "depjoin(select([f, g], r), select([count() as c, f], ascalar(0.f)))";
  [%expect
    {|
       SELECT "t2"."c" AS "c",
              "t2"."f" AS "f"
       FROM
         (SELECT "r"."f" AS "f",
                 "r"."g" AS "g"
          FROM "r") AS "t0",
            LATERAL
         (SELECT count(*) AS "c",
                 min("t1"."f") AS "f"
          FROM
            (SELECT "t0"."f" AS "f") AS "t1") AS "t2"
   |}]

let%expect_test "select-agg-window" =
  run_test "select([count() as c, min(f) as m, row_number() as n], r)";
  [%expect
    {|
       SELECT count(*) AS "c",
              min("t0"."f") AS "m",
              row_number() OVER () AS "n"
       FROM
         (SELECT "r"."f" AS "f",
                 "r"."g" AS "g"
          FROM "r") AS "t0"
   |}]

(* FIXME *)
(* let%expect_test "orderby-filter" = *)
(*   run_test ~conn:Test_util.tpch_conn *)
(* {| *)
      (* select([p_partkey], *)
      (*   dedup(select([p_partkey], orderby([p_partkey, p_type], filter(0 = 0, part))))) *)
      (* |} *)

let%expect_test "depjoin-union" =
  run_test
    {|
depjoin(select([f], r),
  select([0.f as x1, f as x4],
    atuple([
      select([0 as counter0, f],
        atuple([ascalar(0.f)], cross)),
      select([1 as counter0, null:int as f],
        atuple([ascalar((0.f + 1) as x)], cross))],
      concat)))
|};
  [%expect
    {|
    SELECT "t2"."x1" AS "x1",
           "t2"."x4" AS "x4"
    FROM
      (SELECT "r"."f" AS "f"
       FROM "r") AS "t0",
         LATERAL
      (SELECT "t0"."f" AS "x1",
              "t1"."f" AS "x4"
       FROM (
               (SELECT 0 AS "counter0",
                       "t0"."f" AS "f")
             UNION ALL
               (SELECT 1 AS "counter0",
                       (NULL::numeric) AS "f")) AS "t1") AS "t2" |}]

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
       SELECT "t3"."bnd2" AS "bnd2",
              "t3"."bnd0" AS "bnd0",
              "t3"."bnd1" AS "bnd1",
              "t4"."x0" AS "x0",
              "t4"."x1" AS "x1",
              "t4"."x2" AS "x2",
              "t4"."x3" AS "x3",
              "t4"."k1_f" AS "k1_f",
              "t4"."k1_g" AS "k1_g",
              "t4"."k1_rn0" AS "k1_rn0"
       FROM
         (SELECT row_number() OVER () AS "bnd2",
                                   "t2"."f" AS "bnd0",
                                   "t2"."g" AS "bnd1"
          FROM
            (SELECT "r1"."f" AS "f",
                    "r1"."g" AS "g"
             FROM "r1") AS "t2") AS "t3",

         (SELECT "t1"."k1_rn0" AS "x0",
                 "t1"."k1_f" AS "x1",
                 "t1"."k1_g" AS "x2",
                 "t1"."k1_f" AS "x3",
                 "t1"."k1_f" AS "k1_f",
                 "t1"."k1_g" AS "k1_g",
                 "t1"."k1_rn0" AS "k1_rn0"
          FROM
            (SELECT DISTINCT "t0"."f" AS "k1_f",
                             "t0"."g" AS "k1_g",
                             row_number() OVER () AS "k1_rn0"
             FROM
               (SELECT "r1"."f" AS "f",
                       "r1"."g" AS "g"
                FROM "r1") AS "t0") AS "t1") AS "t4"
       WHERE ((("t4"."k1_f") = ("t3"."bnd0"))
              AND ((("t4"."k1_g") = ("t3"."bnd1"))
                   AND (("t4"."k1_rn0") = ("t3"."bnd2")))) |}]
