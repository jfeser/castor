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
  [%expect
    {|
    select([n_name as nation],
      select([s2_n_name as n_name],
        depjoin(select([n_nationkey as s2_n_nationkey, n_name as s2_n_name,
                        n_regionkey as s2_n_regionkey, n_comment as s2_n_comment],
                  nation) as s2,
          select([s2_n_name], ascalar(0 as x))))) |}]

let%expect_test "" =
  let conn = Lazy.force tpch_conn in
  {|
    depjoin(select([min(o_orderdate) as lo, max((o_orderdate + month(3))) as hi], orders) as k1,
      range(k1.lo, k1.hi))
|}
  |> Abslayout_load.load_string conn
  |> unnest |> Format.printf "%a" pp;
  [%expect
    {|
    select([range],
      join(((k1_hi = bnd0) && (k1_lo = bnd1)),
        select([k1_lo as bnd1, k1_hi as bnd0],
          select([lo as k1_lo, hi as k1_hi],
            select([min(o_orderdate) as lo, max((o_orderdate + month(3))) as hi],
              orders))),
        join(((k1_lo <= range) && (range <= k1_hi)),
          dedup(
            select([k1_hi, k1_lo],
              select([lo as k1_lo, hi as k1_hi],
                select([min(o_orderdate) as lo,
                        max((o_orderdate + month(3))) as hi],
                  orders)))),
          range((groupby([min(k1_lo) as min0],
                   [],
                   dedup(
                     select([k1_hi, k1_lo],
                       select([lo as k1_lo, hi as k1_hi],
                         select([min(o_orderdate) as lo,
                                 max((o_orderdate + month(3))) as hi],
                           orders)))))), (groupby([max(k1_hi) as max0],
                                            [],
                                            dedup(
                                              select([k1_hi, k1_lo],
                                                select([lo as k1_lo, hi as k1_hi],
                                                  select([min(o_orderdate) as lo,
                                                          max((o_orderdate +
                                                              month(3))) as hi],
                                                    orders)))))))))) |}]

let%expect_test "" =
  let conn = Lazy.force tpch_conn in
  let r =
    {|
    depjoin(select([ps_availqty], partsupp) as s41,
      depjoin(select([s41.ps_availqty], ascalar(0 as y)) as s46,
          select([s46.ps_availqty], ascalar(0 as x))))

|}
    |> Abslayout_load.load_string conn
  in
  r |> strip_meta |> to_visible_depjoin |> Format.printf "%a" pp;
  [%expect
    {|
    select([ps_availqty],
      depjoin(select([ps_availqty as s41_ps_availqty],
                select([ps_availqty], partsupp)) as s41,
        select([s46_ps_availqty as ps_availqty],
          depjoin(select([s41_ps_availqty as s46_ps_availqty],
                    select([s41_ps_availqty], ascalar(0 as y))) as s46,
            select([s46_ps_availqty], ascalar(0 as x)))))) |}];
  r |> unnest |> Format.printf "%a" pp;
  [%expect
    {|
    select([ps_availqty],
      join((s41_ps_availqty = bnd1),
        select([s41_ps_availqty as bnd1],
          select([ps_availqty as s41_ps_availqty],
            select([ps_availqty], partsupp))),
        select([s46_ps_availqty as ps_availqty, s41_ps_availqty],
          select([bnd0, s41_ps_availqty, s46_ps_availqty],
            join(((s46_ps_availqty = bnd0) && (s41_ps_availqty = d0)),
              select([s46_ps_availqty as bnd0, s41_ps_availqty],
                select([s41_ps_availqty, s41_ps_availqty as s46_ps_availqty],
                  select([s41_ps_availqty],
                    join(true,
                      dedup(
                        select([s41_ps_availqty],
                          select([ps_availqty as s41_ps_availqty],
                            select([ps_availqty], partsupp)))),
                      ascalar(0 as y))))),
              select([s41_ps_availqty as d0, s46_ps_availqty],
                select([s41_ps_availqty, s46_ps_availqty],
                  join(true,
                    dedup(
                      select([s41_ps_availqty, s46_ps_availqty],
                        select([s41_ps_availqty,
                                s41_ps_availqty as s46_ps_availqty],
                          select([s41_ps_availqty],
                            join(true,
                              dedup(
                                select([s41_ps_availqty],
                                  select([ps_availqty as s41_ps_availqty],
                                    select([ps_availqty], partsupp)))),
                              ascalar(0 as y)))))),
                    ascalar(0 as x))))))))) |}]

let%expect_test "" =
  let conn = Lazy.force tpch_conn in
  let r =
    {|
        alist(select([l_extendedprice, l_discount, p_type],
               join(true, lineitem, part)) as s15,
            select([count() as count0, l_extendedprice, l_discount, p_type],
              atuple([ascalar(s15.l_extendedprice), ascalar(s15.l_discount), ascalar(s15.p_type)], cross)))
    |}
    |> Abslayout_load.load_string conn
  in
  r |> strip_meta |> to_visible_depjoin |> Format.printf "%a" pp;
  [%expect {|
    alist(select([l_extendedprice, l_discount, p_type],
            join(true, lineitem, part)) as s15,
      select([count() as count0, l_extendedprice, l_discount, p_type],
        atuple([ascalar(s15_l_extendedprice as l_extendedprice),
                ascalar(s15_l_discount as l_discount),
                ascalar(s15_p_type as p_type)],
          cross))) |}];
  r |> unnest |> Format.printf "%a" pp;
  [%expect {|
    select([count0, l_extendedprice, l_discount, p_type],
      join(((s15_l_discount = bnd0) &&
           ((s15_l_extendedprice = bnd1) && (s15_p_type = bnd2))),
        select([s15_l_extendedprice as bnd1, s15_l_discount as bnd0,
                s15_p_type as bnd2],
          select([l_extendedprice as s15_l_extendedprice,
                  l_discount as s15_l_discount, p_type as s15_p_type],
            select([l_extendedprice, l_discount, p_type],
              join(true, lineitem, part)))),
        groupby([count() as count0, min(l_discount) as l_discount,
                 min(l_extendedprice) as l_extendedprice, min(p_type) as p_type,
                 min(s15_l_discount) as s15_l_discount,
                 min(s15_l_extendedprice) as s15_l_extendedprice,
                 min(s15_p_type) as s15_p_type],
          [s15_l_discount, s15_l_extendedprice, s15_p_type],
          select([s15_l_discount as l_discount,
                  s15_l_extendedprice as l_extendedprice, s15_p_type as p_type,
                  s15_l_discount, s15_l_extendedprice, s15_p_type],
            dedup(
              select([s15_l_discount, s15_l_extendedprice, s15_p_type],
                select([l_extendedprice as s15_l_extendedprice,
                        l_discount as s15_l_discount, p_type as s15_p_type],
                  select([l_extendedprice, l_discount, p_type],
                    join(true, lineitem, part))))))))) |}]
