open Castor
open Ast
open Abslayout
open Unnest
open Unnest.Private
open Test_util

let run r = unnest ~params:(Set.empty (module Name)) r
let n n = Name (Name.create n)

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in

  let q =
    Infix.(
      dep_join
        (select [ (n "f", "k_f") ] @@ r "r")
        "k"
        (select [ (n "g", "g") ] (filter (n "k_f" = n "f") (r "r1"))))
    |> Abslayout_load.annotate_relations conn
  in

  let d = match q.node with DepJoin d -> d | _ -> assert false in
  let t1_attr = attrs d.d_lhs in
  let t2_free = free d.d_rhs in
  t2_free |> [%sexp_of: Set.M(String).t] |> print_s;
  [%expect "(k_f)"];
  t1_attr |> [%sexp_of: Set.M(String).t] |> print_s;
  [%expect "(k_f)"];
  Set.inter t1_attr t2_free |> [%sexp_of: Set.M(String).t] |> print_s;
  [%expect "(k_f)"];
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
        (select [ (n "g", "g") ] (filter (n "k.f" = n "f") (r "r1"))))
    |> Abslayout_load.load_layout_exn conn
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
        (select [ (n "g", "g") ] (filter (n "k.f" = n "f") (r "r1"))))
    |> Abslayout_load.load_layout_exn conn
  in

  run q |> Format.printf "%a" pp;
  [%expect
    {|
    select([g],
      join((k_f = bnd0),
        select([k_f as bnd0, k_g], select([f as k_f, g as k_g], r)),
        select([k_f, g], filter((k_f = f), select([f as k_f, f, g], r1))))) |}]

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
    |> Abslayout_load.load_string_exn conn
  in
  run r |> ignore

let%test_unit "" =
  let conn = Lazy.force tpch_conn in
  let r =
    {|
depjoin(dedup(select([l_shipdate as k2], lineitem)) as s2,
  depjoin(select([sum(l_discount) as agg5],
              filter((s2.k2 = l_shipdate), lineitem)) as s3,
                atuple([ascalar(s3.agg5 as agg5)], cross)))
    |}
    |> Abslayout_load.load_string_exn conn
  in
  run r |> ignore

let%expect_test "" =
  let conn = Lazy.force tpch_conn in
  {|
  select([n_name as nation],
    depjoin(nation as s2,
      select([s2.n_name], ascalar(0 as x))))
|}
  |> Abslayout_load.load_string_exn conn
  |> strip_meta |> to_visible_depjoin |> Format.printf "%a" pp;
  [%expect
    {|
    select([n_name as nation],
      select([n_name],
        depjoin(select([n_nationkey as s2_n_nationkey, n_name as s2_n_name,
                        n_regionkey as s2_n_regionkey, n_comment as s2_n_comment],
                  nation) as s2,
          select([s2_n_name as n_name], ascalar(0 as x))))) |}]

let%expect_test "" =
  let conn = Lazy.force tpch_conn in
  {|
    depjoin(select([min(o_orderdate) as lo, max((o_orderdate + month(3))) as hi], orders) as k1,
      range(k1.lo, k1.hi))
|}
  |> Abslayout_load.load_string_exn conn
  |> run |> Format.printf "%a" pp;
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
    |> Abslayout_load.load_string_exn conn
  in
  r |> strip_meta |> to_visible_depjoin |> Format.printf "%a" pp;
  [%expect
    {|
    select([ps_availqty],
      depjoin(select([ps_availqty as s41_ps_availqty],
                select([ps_availqty], partsupp)) as s41,
        select([ps_availqty],
          depjoin(select([ps_availqty as s46_ps_availqty],
                    select([s41_ps_availqty as ps_availqty], ascalar(0 as y))) as s46,
            select([s46_ps_availqty as ps_availqty], ascalar(0 as x)))))) |}];
  r |> run |> Format.printf "%a" pp;
  [%expect
    {|
    select([ps_availqty],
      join((s41_ps_availqty = bnd1),
        select([s41_ps_availqty as bnd1],
          select([ps_availqty as s41_ps_availqty],
            select([ps_availqty], partsupp))),
        select([s41_ps_availqty, ps_availqty],
          select([s41_ps_availqty, bnd0, s46_ps_availqty, ps_availqty],
            join(((s46_ps_availqty = bnd0) && (s41_ps_availqty = d0)),
              select([s41_ps_availqty, s46_ps_availqty as bnd0],
                select([s41_ps_availqty, ps_availqty as s46_ps_availqty],
                  select([s41_ps_availqty, s41_ps_availqty as ps_availqty],
                    join(true,
                      dedup(
                        select([s41_ps_availqty],
                          select([ps_availqty as s41_ps_availqty],
                            select([ps_availqty], partsupp)))),
                      ascalar(0 as y))))),
              select([s41_ps_availqty as d0, s46_ps_availqty, ps_availqty],
                select([s41_ps_availqty, s46_ps_availqty,
                        s46_ps_availqty as ps_availqty],
                  join(true,
                    dedup(
                      select([s41_ps_availqty, s46_ps_availqty],
                        select([s41_ps_availqty, ps_availqty as s46_ps_availqty],
                          select([s41_ps_availqty, s41_ps_availqty as ps_availqty],
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
    |> Abslayout_load.load_string_exn conn
  in
  r |> strip_meta |> to_visible_depjoin |> Format.printf "%a" pp;
  [%expect
    {|
    alist(select([l_extendedprice, l_discount, p_type],
            join(true, lineitem, part)) as s15,
      select([count() as count0, l_extendedprice, l_discount, p_type],
        atuple([ascalar(s15_l_extendedprice as l_extendedprice),
                ascalar(s15_l_discount as l_discount),
                ascalar(s15_p_type as p_type)],
          cross))) |}];
  r |> run |> Format.printf "%a" pp;
  [%expect
    {|
    select([count0, l_extendedprice, l_discount, p_type],
      join(((s15_l_discount = bnd0) &&
           ((s15_l_extendedprice = bnd1) && (s15_p_type = bnd2))),
        select([s15_l_extendedprice as bnd1, s15_l_discount as bnd0,
                s15_p_type as bnd2],
          select([l_extendedprice as s15_l_extendedprice,
                  l_discount as s15_l_discount, p_type as s15_p_type],
            select([l_extendedprice, l_discount, p_type],
              join(true, lineitem, part)))),
        groupby([s15_l_discount, s15_l_extendedprice, s15_p_type,
                 count() as count0, min(l_extendedprice) as l_extendedprice,
                 min(l_discount) as l_discount, min(p_type) as p_type],
          [s15_l_discount, s15_l_extendedprice, s15_p_type],
          select([s15_l_discount, s15_l_extendedprice, s15_p_type,
                  s15_l_extendedprice as l_extendedprice,
                  s15_l_discount as l_discount, s15_p_type as p_type],
            join(true,
              dedup(
                select([s15_l_discount, s15_l_extendedprice, s15_p_type],
                  select([l_extendedprice as s15_l_extendedprice,
                          l_discount as s15_l_discount, p_type as s15_p_type],
                    select([l_extendedprice, l_discount, p_type],
                      join(true, lineitem, part))))),
              ascalar(0 as x0)))))) |}]

let%expect_test "" =
  let r =
    {|
dedup(
depjoin(partsupp as s1,
  groupby([ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment],
          [ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment],
    select([s1.ps_partkey, s1.ps_suppkey, s1.ps_availqty, s1.ps_supplycost, s1.ps_comment],
      filter((s1.ps_partkey = p_partkey),
        filter((strpos(p_name, "test") = 1), part))))))
|}
    |> Abslayout_load.load_string_exn (Lazy.force tpch_conn)
  in
  Simplify_tactic.simplify (Lazy.force tpch_conn) r |> Fmt.pr "%a@." pp;
  [%expect
    {|
    dedup(
      select([ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment],
        groupby([s1_ps_availqty, s1_ps_comment, s1_ps_partkey, s1_ps_suppkey,
                 s1_ps_supplycost, ps_partkey, ps_suppkey, ps_availqty,
                 ps_supplycost, ps_comment],
          [ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment,
           s1_ps_availqty, s1_ps_comment, s1_ps_partkey, s1_ps_suppkey,
           s1_ps_supplycost],
          select([s1_ps_availqty, s1_ps_comment, s1_ps_partkey, s1_ps_suppkey,
                  s1_ps_supplycost, s1_ps_partkey as ps_partkey,
                  s1_ps_suppkey as ps_suppkey, s1_ps_availqty as ps_availqty,
                  s1_ps_supplycost as ps_supplycost, s1_ps_comment as ps_comment],
            join(((s1_ps_partkey = p_partkey) && true),
              dedup(
                select([ps_availqty as s1_ps_availqty,
                        ps_comment as s1_ps_comment, ps_partkey as s1_ps_partkey,
                        ps_suppkey as s1_ps_suppkey,
                        ps_supplycost as s1_ps_supplycost],
                  partsupp)),
              filter((strpos(p_name, "test") = 1), part)))))) |}]

let%expect_test "" =
  let conn = Lazy.force tpch_conn in
  let r =
    {|

                              depjoin(dedup(
                                        select([o_orderdate as o_orderdate],
                                          dedup(
                                            select([o_orderdate as o_orderdate],
                                              orders)))) as s7,
                                select([s7.o_orderdate as x122,
                                        counter2 as x123, var0 as x124,
                                        x118 as x125, x119 as x126,
                                        x120 as x127, x121 as x128],
                                  atuple([select([0 as counter2, var0,
                                                  null:int as x118,
                                                  null:fixed as x119,
                                                  null:fixed as x120,
                                                  null:fixed as x121],
                                            ascalar(0 as var0)),
                                          select([1 as counter2,
                                                  null:int as var0, x118,
                                                  x119, x120, x121],
                                            depjoin(groupby([count() as ct21,
                                                             l_extendedprice,
                                                             l_discount],
                                                      [l_extendedprice,
                                                       l_discount],
                                                      select([l_extendedprice as l_extendedprice,
                                                              l_discount as l_discount],
                                                        join(((o_orderdate =
                                                              s7.o_orderdate)
                                                             &&
                                                             ((l_returnflag =
                                                              "R") &&
                                                             (l_orderkey =
                                                             o_orderkey))),
                                                          lineitem,
                                                          orders))) as s2,
                                              select([s2.ct21 as x118,
                                                      s2.l_extendedprice as x119,
                                                      s2.l_discount as x120,
                                                      l_discount as x121],
                                                atuple([ascalar(s2.l_discount as l_discount)],
                                                  cross))))],
                                    concat)))
|}
    |> Abslayout_load.load_string_exn conn
  in
  run r |> Fmt.pr "%a" pp;
  [%expect
    {|
    select([x122, x123, x124, x125, x126, x127, x128],
      join((s7_o_orderdate = bnd3),
        select([s7_o_orderdate as bnd3],
          select([o_orderdate as s7_o_orderdate],
            dedup(select([o_orderdate], dedup(select([o_orderdate], orders)))))),
        select([s7_o_orderdate, s7_o_orderdate as x122, counter2 as x123,
                var0 as x124, x118 as x125, x119 as x126, x120 as x127,
                x121 as x128],
          atuple([join(true,
                    dedup(
                      select([s7_o_orderdate],
                        select([o_orderdate as s7_o_orderdate],
                          dedup(
                            select([o_orderdate],
                              dedup(select([o_orderdate], orders))))))),
                    select([0 as counter2, var0, null:int as x118,
                            null:fixed as x119, null:fixed as x120,
                            null:fixed as x121],
                      ascalar(0 as var0))),
                  select([s7_o_orderdate, 1 as counter2, null:int as var0,
                          x118, x119, x120, x121],
                    select([s7_o_orderdate, x118, x119, x120, x121],
                      select([s7_o_orderdate, bnd0, bnd2, bnd1, s2_ct21,
                              s2_l_discount, s2_l_extendedprice, x118, x119,
                              x120, x121],
                        join((((s2_ct21 = bnd0) &&
                              ((s2_l_discount = bnd1) &&
                              (s2_l_extendedprice = bnd2))) &&
                             (s7_o_orderdate = d0)),
                          select([s7_o_orderdate, s2_ct21 as bnd0,
                                  s2_l_extendedprice as bnd2,
                                  s2_l_discount as bnd1],
                            select([s7_o_orderdate, ct21 as s2_ct21,
                                    l_extendedprice as s2_l_extendedprice,
                                    l_discount as s2_l_discount],
                              groupby([s7_o_orderdate, count() as ct21,
                                       l_extendedprice, l_discount],
                                [l_extendedprice, l_discount, s7_o_orderdate],
                                select([s7_o_orderdate, l_extendedprice,
                                        l_discount],
                                  join(((o_orderdate = s7_o_orderdate) &&
                                       ((l_returnflag = "R") &&
                                       (l_orderkey = o_orderkey))),
                                    join(true,
                                      dedup(
                                        select([s7_o_orderdate],
                                          select([o_orderdate as s7_o_orderdate],
                                            dedup(
                                              select([o_orderdate],
                                                dedup(
                                                  select([o_orderdate], orders))))))),
                                      lineitem),
                                    orders))))),
                          select([s7_o_orderdate as d0, s2_ct21, s2_l_discount,
                                  s2_l_extendedprice, x118, x119, x120, x121],
                            select([s7_o_orderdate, s2_ct21, s2_l_discount,
                                    s2_l_extendedprice, s2_ct21 as x118,
                                    s2_l_extendedprice as x119,
                                    s2_l_discount as x120, l_discount as x121],
                              select([s7_o_orderdate, s2_ct21, s2_l_discount,
                                      s2_l_extendedprice,
                                      s2_l_discount as l_discount],
                                join(true,
                                  dedup(
                                    select([s7_o_orderdate, s2_ct21,
                                            s2_l_discount, s2_l_extendedprice],
                                      select([s7_o_orderdate, ct21 as s2_ct21,
                                              l_extendedprice as s2_l_extendedprice,
                                              l_discount as s2_l_discount],
                                        groupby([s7_o_orderdate, count() as ct21,
                                                 l_extendedprice, l_discount],
                                          [l_extendedprice, l_discount,
                                           s7_o_orderdate],
                                          select([s7_o_orderdate,
                                                  l_extendedprice, l_discount],
                                            join(((o_orderdate = s7_o_orderdate)
                                                 &&
                                                 ((l_returnflag = "R") &&
                                                 (l_orderkey = o_orderkey))),
                                              join(true,
                                                dedup(
                                                  select([s7_o_orderdate],
                                                    select([o_orderdate as s7_o_orderdate],
                                                      dedup(
                                                        select([o_orderdate],
                                                          dedup(
                                                            select([o_orderdate],
                                                              orders))))))),
                                                lineitem),
                                              orders)))))),
                                  ascalar(0 as x0)))))))))],
            concat)))) |}]
