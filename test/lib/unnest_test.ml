open Castor
open Ast
open Unnest
open Unnest.Private
open Test_util
module A = Constructors.Annot

let n n = `Name (Name.create n)

let%expect_test "" =
  run_eval_test "depjoin(r, r)" unnest;
  [%expect
    {|
    select([f, g],
      select([f, g],
        join(true,
          select([1_f, 1_g], select([f as 1_f, g as 1_g], r)),
          select([f, g], r)))) |}]

let%expect_test "" =
  run_eval_test "depjoin(r, filter(0.f = g, r))" unnest;
  [%expect
    {|
    select([f, g],
      select([f, g],
        join((1_f = lhs_1_f),
          select([1_f as lhs_1_f, 1_g], select([f as 1_f, g as 1_g], r)),
          filter((1_f = g), select([g as 1_f, f, g], r))))) |}]

let%expect_test "" =
  run_eval_test "depjoin(r, select([g], filter(0.f = f, r1)))" unnest;
  [%expect
    {|
    select([g],
      select([g],
        join((1_f = lhs_1_f),
          select([1_f as lhs_1_f, 1_g], select([f as 1_f, g as 1_g], r)),
          select([1_f, g], filter((1_f = f), select([f as 1_f, f, g], r1)))))) |}]

let%expect_test "" =
  run_eval_test
    "join(true, depjoin(r, select([0.f], r)), depjoin(r, select([0.g], r)))"
    unnest;
  [%expect
    {|
    join(true,
      select([f],
        select([f],
          join((2_f = lhs_2_f),
            select([2_f as lhs_2_f, 2_g], select([f as 2_f, g as 2_g], r)),
            select([2_f, 2_f as f],
              join(true,
                dedup(select([2_f], select([f as 2_f, g as 2_g], r))),
                r))))),
      select([g],
        select([g],
          join((1_g = lhs_1_g),
            select([1_f, 1_g as lhs_1_g], select([f as 1_f, g as 1_g], r)),
            select([1_g, 1_g as g],
              join(true,
                dedup(select([1_g], select([f as 1_f, g as 1_g], r))),
                r)))))) |}]

let%expect_test "" =
  run_eval_test "depjoin(orderby([f], r), orderby([g], filter(0.f = g, r)))"
    unnest;
  [%expect
    {|
    select([f, g],
      select([f, g],
        join((1_f = lhs_1_f),
          select([1_f as lhs_1_f, 1_g],
            select([f as 1_f, g as 1_g], orderby([f], r))),
          orderby([g], filter((1_f = g), select([g as 1_f, f, g], r)))))) |}]

let%expect_test "" =
  run_eval_test "depjoin(depjoin(r, filter(0.f = g, r)), filter(0.f = g, r))"
    unnest;
  [%expect
    {|
    select([f, g],
      select([f, g],
        join((2_f = lhs_2_f),
          select([2_f as lhs_2_f, 2_g],
            select([f as 2_f, g as 2_g],
              select([f, g],
                select([f, g],
                  join((1_f = lhs_1_f),
                    select([1_f as lhs_1_f, 1_g],
                      select([f as 1_f, g as 1_g], r)),
                    filter((1_f = g), select([g as 1_f, f, g], r))))))),
          filter((2_f = g), select([g as 2_f, f, g], r))))) |}]

let%expect_test "" =
  run_eval_test "depjoin(r, depjoin(r1, select([1.f, 0.g], ascalar(0 as z))))"
    unnest;
  [%expect
    {|
    select([f, g],
      select([f, g],
        join((1_f = lhs_1_f),
          select([1_f as lhs_1_f, 1_g], select([f as 1_f, g as 1_g], r)),
          select([1_f, f, g],
            select([1_f, f, g],
              select([1_f, 2_f, lhs_2_g, 2_g, f, g],
                join((2_g = lhs_2_g),
                  select([2_f, 2_g as lhs_2_g], select([f as 2_f, g as 2_g], r1)),
                  select([1_f, 2_g, 1_f as f, 2_g as g],
                    join(true,
                      dedup(select([1_f], select([f as 1_f, g as 1_g], r))),
                      join(true,
                        dedup(select([2_g], select([f as 2_f, g as 2_g], r1))),
                        ascalar(0 as z))))))))))) |}]

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in

  let q =
    "depjoin(select([f], r), select([g], filter(0.f = f, r1)))"
    |> Abslayout_load.load_string_exn conn
    |> to_visible_depjoin
    |> map_meta (fun _ ->
           object
             method was_depjoin = false
           end)
  in

  pp Fmt.stdout q;
  [%expect
    {|
    select([g], vdepjoin(select([f as 1_f], select([f], r)), select([g],
    filter((1_f = f),
    r1)))) |}];

  let d =
    match q.node with
    | Query (Select (_, { node = Visible_depjoin d; _ })) -> d
    | _ -> assert false
  in
  let t1_attr = attrs d.d_lhs in
  let t2_free = free d.d_rhs in
  t2_free |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect "(((name (Simple 1_f))))"];
  t1_attr |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect "(((name (Simple 1_f)) (type_ (IntT))))"];
  Set.inter t1_attr t2_free |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect "(((name (Simple 1_f)) (type_ (IntT))))"];

  {
    node = to_nice_depjoin d.d_lhs d.d_rhs;
    meta =
      object
        method was_depjoin = true
      end;
  }
  |> Format.printf "%a" pp;
  [%expect
    {|
             select([g], join((1_f = lhs_1_f), select([1_f as lhs_1_f], select([f as 1_f],
             select([f], r))), vdepjoin(dedup(select([1_f], select([f as 1_f], select(
             [f], r)))), select([g], filter((1_f = f),
             r1))))) |}]

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in
  let q =
    "depjoin(r, select([g], filter(0.f = f, r1)))"
    |> Abslayout_load.load_string_exn conn
    |> strip_meta |> to_visible_depjoin
  in
  Fmt.pr "%a" pp q;
  [%expect
    {|
                select([g], vdepjoin(select([f as 1_f, g as 1_g], r), select([g],
                filter((1_f = f),
                r1)))) |}]
