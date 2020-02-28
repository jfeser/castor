open Castor
open Abslayout
open Unnest
open Test_util

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
  [%expect "(((name k_f) (meta <opaque>)))"];
  t1_attr |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect "(((name k_f) (meta <opaque>)))"];
  Set.inter t1_attr t2_free |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect "(((name k_f) (meta <opaque>)))"];
  to_nice_depjoin d.d_lhs d.d_rhs |> Format.printf "%a" pp;
  [%expect
    {|
    join((k_f = bnd0),
      select([k_f as bnd0], select([f as k_f], r)),
      depjoin(dedup(select([k_f], select([f as k_f], r))) as x,
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
      join((k_f = bnd0),
        select([k_f as bnd0, k_g], select([f as k_f, g as k_g], r)),
        select([g, k_f], filter((k_f = f), select([f as k_f, f, g], r1))))) |}]

let%test_unit "" =
  let conn = Lazy.force tpch_conn in
  let r =
    {|
depjoin(select([row_number() as rn2, agg5, agg4, agg3, agg2, agg1, agg0, l_returnflag, l_linestatus],
                    select([sum(l_discount) as agg5, count() as agg4,
                            sum(((l_extendedprice * (1 - l_discount)) * (1 + l_tax))) as agg3,
                            sum((l_extendedprice * (1 - l_discount))) as agg2, 
                            sum(l_extendedprice) as agg1, sum(l_quantity) as agg0, 
                            l_returnflag as l_returnflag, l_linestatus as l_linestatus],
                      filter(true, filter(true, lineitem)))) as s3,
            select([s3.rn2 as x0, s3.agg5 as x1, s3.agg4 as x2, s3.agg3 as x3, 
                    s3.agg2 as x4, s3.agg1 as x5, s3.agg0 as x6, s3.l_returnflag as x7, 
                    s3.l_linestatus as x8, agg5 as x9, agg4 as x10, agg3 as x11, 
                    agg2 as x12, agg1 as x13, agg0 as x14, l_returnflag as x15, 
                    l_linestatus as x16],
              atuple([ascalar(s3.agg5 as agg5), ascalar(s3.agg4 as agg4), 
                      ascalar(s3.agg3 as agg3), ascalar(s3.agg2 as agg2), 
                      ascalar(s3.agg1 as agg1), ascalar(s3.agg0 as agg0), 
                      ascalar(s3.l_returnflag as l_returnflag), ascalar(s3.l_linestatus as l_linestatus)],
                cross)))
    |}
    |> Abslayout_load.load_string conn
  in
  let r' = unnest r in
  Validate.equiv conn r r' |> Or_error.ok_exn

let%test_unit "" =
  let conn = Lazy.force tpch_conn in
  let r =
    {|
depjoin(dedup(select([l_shipdate as k2], lineitem)) as s2,
  depjoin(select([sum(l_discount) as agg5],
              filter((s2.k2 = l_shipdate), lineitem)) as s3,
                atuple([ascalar(s3.agg5 as agg5)], cross)))
    |}
    |> Abslayout_load.load_string conn
  in
  let r' = unnest r in
  Validate.equiv conn r r' |> Or_error.ok_exn

let%expect_test "" =
  let conn = Lazy.force tpch_conn in
  {|
  select([n_name as nation],
    depjoin(nation as s2,
      select([s2.n_name], ascalar(0 as x))))
|}
  |> Abslayout_load.load_string conn
  |> strip_meta |> to_visible_depjoin |> Format.printf "%a" pp;
  [%expect {|
    select([n_name as nation],
      select([s2_n_name as n_name],
        depjoin(select([n_nationkey as s2_n_nationkey, n_name as s2_n_name,
                        n_regionkey as s2_n_regionkey, n_comment as s2_n_comment],
                  nation) as s2,
          select([s2_n_name], ascalar(0 as x))))) |}]
