open! Core
open Castor
open Abslayout
open Unnest

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in

  let q =
    Infix.(
      dep_join
        (select [ Pred.Infix.as_ (n "f") "k_f" ] @@ r "r")
        "k"
        (select [ n "g" ] (filter (n "k_f" = n "f") (r "r1"))))
    |> Abslayout_load.annotate_relations conn
  in

  let d = match q.node with DepJoin d -> d | _ -> assert false in
  let t1_attr = attrs d.d_lhs in
  let t2_free = free d.d_rhs in
  t2_free |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect "(((scope ()) (name k_f)))"];
  t1_attr |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect "(((scope ()) (name k_f)))"];
  Set.inter t1_attr t2_free |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect "(((scope ()) (name k_f)))"];
  to_nice_depjoin d |> Format.printf "%a" pp;
  [%expect
    {|
    join((k_f = bnd0),
      select([k_f as bnd0], select([f as k_f], r)),
      depjoin(dedup(select([k_f], select([f as k_f], r))) as k,
        select([g], filter((k_f = f), r1)))) |}]

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in

  let q =
    Infix.(
      dep_join (r "r") "k"
        (select [ n "g" ] (filter (n "k.f" = n "f") (r "r1"))))
    |> Abslayout_load.load_layout conn
    |> strip_meta |> to_visible_depjoin
  in

  q |> Format.printf "%a" pp;
  [%expect
    {|
    select([g],
      depjoin(select([f as k_f, g as k_g], r) as k,
        select([g], filter((k_f = f), r1)))) |}]

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in

  let q =
    Infix.(
      dep_join (r "r") "k"
        (select [ n "g" ] (filter (n "k.f" = n "f") (r "r1"))))
    |> Abslayout_load.load_layout conn
  in

  unnest q |> Format.printf "%a" pp;
  [%expect
    {|
    select([g],
      join((k_f = bnd1),
        select([k_f as bnd1, k_g], select([f as k_f, g as k_g], r)),
        select([g, k_f],
          filter((k_f = f),
            join(true, dedup(select([k_f], select([f as k_f, g as k_g], r))), r1))))) |}]
