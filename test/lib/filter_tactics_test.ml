open Abslayout
open Abslayout_load
open Test_util

module Test_db = struct
  module C = struct
    let params =
      Set.of_list
        (module Name)
        [
          Name.create ~type_:Prim_type.int_t "param";
          Name.create ~type_:Prim_type.string_t "param1";
        ]

    let conn = Lazy.force test_db_conn
    let cost_conn = Lazy.force test_db_conn
  end

  module C_tpch = struct
    let conn = Lazy.force tpch_conn
    let cost_conn = Lazy.force tpch_conn
  end

  open Filter_tactics.Make (C)
  open Ops.Make (C)

  let load_string ?params s = load_string_exn ?params C.conn s

  let%expect_test "push-filter-comptime" =
    let r =
      load_string
        "alist(r as r1, filter(r1.f = f, alist(r as r2, ascalar(r2.f))))"
    in
    Option.iter
      (apply
         (at_ push_filter Path.(all >>? is_filter >>| shallowest))
         Path.root r)
      ~f:(Format.printf "%a\n" pp);
    [%expect
      {| alist(r as r1, alist(filter((r1.f = f), r) as r2, ascalar(r2.f))) |}]

  let%expect_test "push-filter-runtime" =
    let r =
      load_string
        "depjoin(r as r1, filter(r1.f = f, alist(r as r2, ascalar(r2.f))))"
    in
    Option.iter
      (apply
         (at_ push_filter Path.(all >>? is_filter >>| shallowest))
         Path.root r)
      ~f:(Format.printf "%a\n" pp);
    [%expect
      {| depjoin(r as r1, alist(r as r2, filter((r1.f = f), ascalar(r2.f)))) |}]

  let%expect_test "push-filter-support" =
    let r =
      load_string ~params:C.params
        "filter(f > param, ahashidx(select([f], r) as k, ascalar(0 as x), 0))"
    in
    Option.iter
      (apply
         (at_ push_filter Path.(all >>? is_filter >>| shallowest))
         Path.root r)
      ~f:(Format.printf "%a\n" pp);
    [%expect
      {|
      ahashidx(select([f], r) as k, filter((k.f > param), ascalar(0 as x)), 0) |}]

  let%expect_test "push-filter-support" =
    let r =
      load_string
        {|
alist(filter((0 = g),
        depjoin(ascalar(0 as f) as k,
          select([k.f, g], ascalar(0 as g)))) as k1, ascalar(0 as x))

|}
    in
    Option.iter
      (apply
         (at_ push_filter Path.(all >>? is_filter >>| shallowest))
         Path.root r)
      ~f:(Format.printf "%a\n" pp);
    [%expect
      {|
      alist(depjoin(ascalar(0 as f) as k,
              filter((0 = g), select([k.f, g], ascalar(0 as g)))) as k1,
        ascalar(0 as x)) |}]

  let%expect_test "push-filter-select" =
    let r =
      load_string "filter(test > 0, select([x as test], ascalar(0 as x)))"
    in
    Option.iter (apply push_filter Path.root r) ~f:(Format.printf "%a\n" pp);
    [%expect {| select([x as test], filter((x > 0), ascalar(0 as x))) |}]

  let%expect_test "push-filter-select" =
    let r =
      load_string
        "filter(a = b, select([(x - 1) as a, (x + 1) as b], ascalar(0 as x)))"
    in
    Option.iter (apply push_filter Path.root r) ~f:(Format.printf "%a\n" pp);
    [%expect
      {|
      select([(x - 1) as a, (x + 1) as b],
        filter(((x - 1) = (x + 1)), ascalar(0 as x))) |}]

  let%expect_test "elim-eq-filter" =
    let r =
      load_string ~params:C.params "depjoin(r as k, filter(k.f = param, r))"
    in
    Option.iter
      (apply
         (at_ elim_eq_filter Path.(all >>? is_filter >>| shallowest))
         Path.root r)
      ~f:(Format.printf "%a\n" pp);
    [%expect {| |}]

  let%expect_test "partition" =
    let r = load_string ~params:C.params "filter(f = param, r)" in
    Sequence.iter
      (Branching.apply partition Path.root r)
      ~f:(Format.printf "%a\n" pp);
    [%expect
      {|
        select([f, g],
          ahashidx(depjoin(select([min(f) as lo, max(f) as hi],
                             select([f], select([f], dedup(select([f], r))))) as k1,
                     select([range as k0], range(k1.lo, k1.hi))) as s0,
            filter((f = s0.k0), r),
            param))
 |}]

  let%expect_test "elim-eq-filter" =
    let r =
      load_string ~params:C.params
        "filter(fresh = param, select([f as fresh], r))"
    in
    Option.iter (apply elim_eq_filter Path.root r) ~f:(Format.printf "%a\n" pp);
    [%expect
      {|
          select([fresh],
            ahashidx(dedup(
                       atuple([select([p1 as x0],
                                 dedup(select([fresh as p1], select([f as fresh], r))))],
                         cross)) as s0,
              filter((fresh = s0.x0), select([f as fresh], r)),
              param)) |}]

  let%expect_test "elim-eq-filter-approx" =
    let r =
      load_string ~params:C.params
        "filter(fresh = param, select([f as fresh], filter(g = param, r)))"
    in
    Option.iter (apply elim_eq_filter Path.root r) ~f:(Format.printf "%a\n" pp);
    [%expect
      {|
          select([fresh],
            ahashidx(dedup(
                       atuple([select([p1 as x0],
                                 select([f as p1], dedup(select([f], r))))],
                         cross)) as s0,
              filter((fresh = s0.x0), select([f as fresh], filter((g = param), r))),
              param)) |}]

  let%expect_test "elim-eq-filter" =
    let r =
      load_string ~params:C.params
        "filter((fresh = param) && true, select([f as fresh, g], r))"
    in
    Option.iter (apply elim_eq_filter Path.root r) ~f:(Format.printf "%a\n" pp);
    [%expect
      {|
        select([fresh, g],
          filter(true,
            ahashidx(dedup(
                       atuple([select([p1 as x0],
                                 dedup(
                                   select([fresh as p1], select([f as fresh, g], r))))],
                         cross)) as s0,
              filter((fresh = s0.x0), select([f as fresh, g], r)),
              param))) |}]

  let%expect_test "elim-eq-filter" =
    let r =
      load_string ~params:C.params
        "filter((fresh1 = param && fresh2 = (param +1)) || (fresh2 = param && \
         fresh1 = (param +1)), select([f as fresh1, g as fresh2], r))"
    in
    Option.iter (apply elim_eq_filter Path.root r) ~f:(Format.printf "%a\n" pp);
    [%expect
      {|
        select([fresh1, fresh2],
          ahashidx(dedup(
                     atuple([dedup(
                               atuple([select([x0 as x2],
                                         select([p1 as x0],
                                           dedup(
                                             select([fresh1 as p1],
                                               select([f as fresh1, g as fresh2], r))))),
                                       select([x1 as x2],
                                         select([p1 as x1],
                                           dedup(
                                             select([fresh2 as p1],
                                               select([f as fresh1, g as fresh2], r)))))],
                                 concat)),
                             dedup(
                               atuple([select([x3 as x5],
                                         select([p1 as x3],
                                           dedup(
                                             select([fresh2 as p1],
                                               select([f as fresh1, g as fresh2], r))))),
                                       select([x4 as x5],
                                         select([p1 as x4],
                                           dedup(
                                             select([fresh1 as p1],
                                               select([f as fresh1, g as fresh2], r)))))],
                                 concat))],
                       cross)) as s0,
            filter((((fresh1 = s0.x2) && (fresh2 = s0.x5)) ||
                   ((fresh2 = s0.x2) && (fresh1 = s0.x5))),
              select([f as fresh1, g as fresh2], r)),
            (param, (param + 1)))) |}]
end

module Tpch = struct
  module C = struct
    let params = Set.empty (module Name)
    let conn = Lazy.force tpch_conn
    let cost_conn = Lazy.force tpch_conn
  end

  open Filter_tactics.Make (C)
  open Simplify_tactic.Make (C)
  open Ops.Make (C)

  let load_string ?params s = load_string_exn ?params C.conn s

  let%expect_test "push-filter-tuple" =
    let r =
      Abslayout_load.load_string_exn (Lazy.force tpch_conn)
        {|
filter((strpos(p_name, "") > 0),
            atuple([ascalar(0 as x),
                    alist(select([s_suppkey],
                              select([s_suppkey, s_nationkey], supplier)) as s5,
                      alist(select([l_partkey, l_suppkey, l_quantity,
                                    l_extendedprice, l_discount, o_orderdate,
                                    p_name],
                              filter(((s5.s_suppkey = l_suppkey)),
                                select([l_partkey, l_suppkey, l_quantity,
                                        l_extendedprice, l_discount,
                                        o_orderdate, p_name],
                                  join((p_partkey = l_partkey),
                                    join((o_orderkey = l_orderkey),
                                      lineitem,
                                      orders),
                                    part)))) as s4,
                        atuple([ascalar(s4.l_quantity),
                                ascalar(s4.l_extendedprice),
                                ascalar(s4.l_discount),
                                ascalar(s4.o_orderdate), ascalar(s4.p_name),
                                alist(select([ps_supplycost],
                                        filter(((ps_partkey = s4.l_partkey)
                                               &&
                                               ((ps_suppkey = s4.l_suppkey)
                                               &&
                                               (ps_suppkey = s4.l_suppkey))),
                                          partsupp)) as s3,
                                  ascalar(s3.ps_supplycost))],
                          cross)))],
              cross))
|}
    in
    Option.iter (apply push_filter Path.root r) ~f:(Format.printf "%a\n" pp);
    [%expect
      {|
    atuple([ascalar(0 as x),
            filter((strpos(p_name, "") > 0),
              alist(select([s_suppkey], supplier) as s5,
                alist(select([l_partkey, l_suppkey, l_quantity, l_extendedprice,
                              l_discount, o_orderdate, p_name],
                        filter((s5.s_suppkey = l_suppkey),
                          select([l_partkey, l_suppkey, l_quantity,
                                  l_extendedprice, l_discount, o_orderdate,
                                  p_name],
                            join((p_partkey = l_partkey),
                              join((o_orderkey = l_orderkey), lineitem, orders),
                              part)))) as s4,
                  atuple([ascalar(s4.l_quantity), ascalar(s4.l_extendedprice),
                          ascalar(s4.l_discount), ascalar(s4.o_orderdate),
                          ascalar(s4.p_name),
                          alist(select([ps_supplycost],
                                  filter(((ps_partkey = s4.l_partkey) &&
                                         ((ps_suppkey = s4.l_suppkey) &&
                                         (ps_suppkey = s4.l_suppkey))),
                                    partsupp)) as s3,
                            ascalar(s3.ps_supplycost))],
                    cross))))],
      cross) |}]

  let with_log src f =
    Log.setup_log Error;
    Logs.Src.set_level src (Some Debug);
    Exn.protect ~f ~finally:(fun () -> Logs.Src.set_level src None)

  let%expect_test "elim-subquery" =
    let r =
      Abslayout_load.load_string_exn (Lazy.force tpch_conn)
        {|
filter((0 =
               (select([max(total_revenue_i) as tot],
                  alist(dedup(select([l_suppkey], lineitem)) as k1,
                    select([sum(agg2) as total_revenue_i],
                      aorderedidx(dedup(select([l_shipdate], lineitem)) as s41,
                        filter((count2 > 0),
                          select([count() as count2,
                                  sum((l_extendedprice * (1 - l_discount))) as agg2],
                            alist(select([l_extendedprice, l_discount],
                                    filter(((l_suppkey = k1.l_suppkey) &&
                                           (l_shipdate = s41.l_shipdate)),
                                      lineitem)) as s42,
                              atuple([ascalar(s42.l_extendedprice),
                                      ascalar(s42.l_discount)],
                                cross)))),
                        >= date("0000-01-01"), < (date("0000-01-01") + month(3)))))))),
  ascalar(0 as x))
|}
    in
    Option.iter (apply elim_subquery Path.root r) ~f:(Format.printf "%a\n" pp);
    [%expect
      {|
        depjoin(select([(select([max(total_revenue_i) as tot],
                           alist(dedup(select([l_suppkey], lineitem)) as k1,
                             select([sum(agg2) as total_revenue_i],
                               aorderedidx(dedup(select([l_shipdate], lineitem)) as s41,
                                 filter((count2 > 0),
                                   select([count() as count2,
                                           sum((l_extendedprice * (1 - l_discount))) as agg2],
                                     alist(select([l_extendedprice, l_discount],
                                             filter(((l_suppkey = k1.l_suppkey) &&
                                                    (l_shipdate = s41.l_shipdate)),
                                               lineitem)) as s42,
                                       atuple([ascalar(s42.l_extendedprice),
                                               ascalar(s42.l_discount)],
                                         cross)))),
                                 >= date("0000-01-01"), < (date("0000-01-01") +
                                                          month(3))))))) as q0],
                  ascalar(0 as dummy)) as s0,
          filter((0 = s0.q0), ascalar(0 as x)))
 |}]

  let%expect_test "hoist-filter" =
    let r =
      Abslayout_load.load_string_exn (Lazy.force tpch_conn)
        {|
aorderedidx(select([l_shipdate, o_orderdate],
              join(true, dedup(select([l_shipdate], lineitem)), dedup(select([o_orderdate], orders)))) as s68,
  filter((c_mktsegment = ""),
    alist(select([c_custkey, c_mktsegment], customer) as s65,
      atuple([ascalar(s65.c_mktsegment),
              alist(select([l_orderkey, l_discount, l_extendedprice, o_shippriority, o_orderdate],
                      join(true,
                        lineitem,
                        orders)) as s64,
                atuple([ascalar(s64.l_orderkey), ascalar(s64.l_discount),
                        ascalar(s64.l_extendedprice), ascalar(s64.o_shippriority),
                        ascalar(s64.o_orderdate)],
                  cross))],
        cross))),
  > date("0000-01-01"), , , < date("0000-01-01"))
|}
    in
    Option.iter (apply hoist_filter Path.root r) ~f:(Fmt.pr "%a" pp);
    [%expect
      {|
    filter((c_mktsegment = ""),
      aorderedidx(select([l_shipdate, o_orderdate],
                    join(true,
                      dedup(select([l_shipdate], lineitem)),
                      dedup(select([o_orderdate], orders)))) as s68,
        alist(select([c_custkey, c_mktsegment], customer) as s65,
          atuple([ascalar(s65.c_mktsegment),
                  alist(select([l_orderkey, l_discount, l_extendedprice,
                                o_shippriority, o_orderdate],
                          join(true, lineitem, orders)) as s64,
                    atuple([ascalar(s64.l_orderkey), ascalar(s64.l_discount),
                            ascalar(s64.l_extendedprice),
                            ascalar(s64.o_shippriority), ascalar(s64.o_orderdate)],
                      cross))],
            cross)),
        > date("0000-01-01"), , , < date("0000-01-01"))) |}]

  let%expect_test "partition" =
    let param1 = Name.create ~type_:Prim_type.string_t "param1" in
    let params = Set.of_list (module Name) [ param1 ] in
    let r =
      load_string_exn (Lazy.force tpch_conn) ~params
        {|
orderby([nation, o_year desc],
  groupby([nation, o_year, sum(amount) as sum_profit],
    [nation, o_year],
    select([n_name as nation, to_year(o_orderdate) as o_year,
            ((l_extendedprice * (1 - l_discount)) - (ps_supplycost * l_quantity)) as amount],
      join((s_suppkey = l_suppkey),
        join(((ps_suppkey = l_suppkey) && (ps_partkey = l_partkey)),
          join((p_partkey = l_partkey),
            filter((strpos(p_name, param1) > 0), part),
            join((o_orderkey = l_orderkey), orders, lineitem)),
          partsupp),
        join((s_nationkey = n_nationkey), supplier, nation)))))
|}
    in
    Option.iter (partition_on r param1) ~f:(Fmt.pr "%a" pp)

  let%expect_test "elim-correlated-subquery" =
    let r =
      load_string_exn (Lazy.force tpch_conn)
        {|
filter((ps_availqty >
  (select([(0.5 * sum(l_quantity)) as tot],
     filter(((l_partkey = ps_partkey) &&
            ((l_suppkey = ps_suppkey))),
       lineitem)))),
  partsupp)
|}
    in
    apply elim_correlated_first_subquery Path.root r
    |> Fmt.pr "%a@." Fmt.(option pp);
    [%expect
      {|
      depjoin(partsupp as s0,
        select([s0.ps_availqty, s0.ps_comment, s0.ps_partkey, s0.ps_suppkey,
                s0.ps_supplycost],
          filter((s0.ps_availqty > q0),
            select([tot as q0],
              select([(0.5 * sum(l_quantity)) as tot],
                filter(((l_partkey = s0.ps_partkey) && (l_suppkey = s0.ps_suppkey)),
                  lineitem)))))) |}]

  let%expect_test "elim-correlated-subquery" =
    let r =
      load_string_exn (Lazy.force tpch_conn)
        {|
filter(exists(filter((ps_partkey = p_partkey),
                filter((strpos(p_name, "test") = 1), part))),
  partsupp)
|}
    in
    apply elim_correlated_exists_subquery Path.root r
    |> Fmt.pr "%a@." Fmt.(option pp);
    [%expect
      {|
      depjoin(partsupp as s0,
        dedup(
          filter(true,
            groupby([ps_availqty, ps_comment, ps_partkey, ps_suppkey, ps_supplycost],
              [ps_availqty, ps_comment, ps_partkey, ps_suppkey, ps_supplycost],
              select([s0.ps_availqty, s0.ps_comment, s0.ps_partkey, s0.ps_suppkey,
                      s0.ps_supplycost],
                filter((s0.ps_partkey = p_partkey),
                  filter((strpos(p_name, "test") = 1), part))))))) |}]

  let%expect_test "partition" =
    let s_suppkey = Name.create "s_suppkey" in
    let r =
      load_string_exn
        ~params:(Set.singleton (module Name) s_suppkey)
        (Lazy.force tpch_conn)
        {|
filter((s_suppkey = ps_suppkey),
  filter(exists(filter((ps_partkey = p_partkey), filter((strpos(p_name, "test") = 1), part))),
    filter((ps_availqty >
           (select([(0.5 * sum(l_quantity)) as tot],
              filter(((l_partkey = ps_partkey) && ((l_suppkey = ps_suppkey))),
                lineitem)))),
      partsupp)))
|}
    in
    partition_on r s_suppkey |> Option.iter ~f:(Fmt.pr "%a@." pp)

  let%expect_test "elim-correlated-subquery" =
    let r =
      load_string_exn (Lazy.force tpch_conn)
        {|
dedup(select([s_name, s_address],
  orderby([s_name],
    join((s_nationkey = n_nationkey),
      filter((n_name = "test"), nation),
      filter(exists(filter((s_suppkey = ps_suppkey),
                      filter(exists(filter((ps_partkey = p_partkey),
                                      filter((strpos(p_name, "test") = 1),
                                        part))),
                        filter((ps_availqty >
                               (select([(0.5 * sum(l_quantity)) as tot],
                                  filter(((l_partkey = ps_partkey) &&
                                         ((l_suppkey = ps_suppkey))),
                                    lineitem)))),
                          partsupp)))),
        supplier)))))
|}
    in
    apply
      (seq_many [ elim_all_correlated_subqueries; unnest_and_simplify ])
      Path.root r
    |> Fmt.pr "%a@." Fmt.(option pp);
    [%expect
      {|
      dedup(
        select([s_name, s_address],
          orderby([s_name],
            join((s_nationkey = n_nationkey),
              filter((n_name = "test"), nation),
              select([s_acctbal, s_address, s_comment, s_name, s_nationkey,
                      s_phone, s_suppkey],
                groupby([s1_s_acctbal, s1_s_address, s1_s_comment, s1_s_name,
                         s1_s_nationkey, s1_s_phone, s1_s_suppkey, s_acctbal,
                         s_address, s_comment, s_name, s_nationkey, s_phone,
                         s_suppkey],
                  [s_acctbal, s_address, s_comment, s_name, s_nationkey, s_phone,
                   s_suppkey, s1_s_acctbal, s1_s_address, s1_s_comment, s1_s_name,
                   s1_s_nationkey, s1_s_phone, s1_s_suppkey],
                  select([s1_s_acctbal, s1_s_address, s1_s_comment, s1_s_name,
                          s1_s_nationkey, s1_s_phone, s1_s_suppkey,
                          s1_s_acctbal as s_acctbal, s1_s_address as s_address,
                          s1_s_comment as s_comment, s1_s_name as s_name,
                          s1_s_nationkey as s_nationkey, s1_s_phone as s_phone,
                          s1_s_suppkey as s_suppkey],
                    join(((s1_s_suppkey = ps_suppkey) && true),
                      dedup(
                        select([s_acctbal as s1_s_acctbal,
                                s_address as s1_s_address,
                                s_comment as s1_s_comment, s_name as s1_s_name,
                                s_nationkey as s1_s_nationkey,
                                s_phone as s1_s_phone, s_suppkey as s1_s_suppkey],
                          supplier)),
                      select([ps_availqty, ps_comment, ps_partkey, ps_suppkey,
                              ps_supplycost],
                        groupby([s3_ps_availqty, s3_ps_comment, s3_ps_partkey,
                                 s3_ps_suppkey, s3_ps_supplycost, ps_availqty,
                                 ps_comment, ps_partkey, ps_suppkey, ps_supplycost],
                          [ps_availqty, ps_comment, ps_partkey, ps_suppkey,
                           ps_supplycost, s3_ps_availqty, s3_ps_comment,
                           s3_ps_partkey, s3_ps_suppkey, s3_ps_supplycost],
                          select([s3_ps_availqty, s3_ps_comment, s3_ps_partkey,
                                  s3_ps_suppkey, s3_ps_supplycost,
                                  s3_ps_availqty as ps_availqty,
                                  s3_ps_comment as ps_comment,
                                  s3_ps_partkey as ps_partkey,
                                  s3_ps_suppkey as ps_suppkey,
                                  s3_ps_supplycost as ps_supplycost],
                            join(((s3_ps_partkey = p_partkey) && true),
                              dedup(
                                select([s4_ps_availqty as s3_ps_availqty,
                                        s4_ps_comment as s3_ps_comment,
                                        s4_ps_partkey as s3_ps_partkey,
                                        s4_ps_suppkey as s3_ps_suppkey,
                                        s4_ps_supplycost as s3_ps_supplycost],
                                  filter((s4_ps_availqty > q2),
                                    groupby([s4_ps_availqty, s4_ps_comment,
                                             s4_ps_partkey, s4_ps_suppkey,
                                             s4_ps_supplycost,
                                             (0.5 * sum(l_quantity)) as q2],
                                      [s4_ps_availqty, s4_ps_comment,
                                       s4_ps_partkey, s4_ps_suppkey,
                                       s4_ps_supplycost],
                                      join((((l_partkey = s4_ps_partkey) &&
                                            (l_suppkey = s4_ps_suppkey)) && true),
                                        dedup(
                                          select([ps_availqty as s4_ps_availqty,
                                                  ps_comment as s4_ps_comment,
                                                  ps_partkey as s4_ps_partkey,
                                                  ps_suppkey as s4_ps_suppkey,
                                                  ps_supplycost as s4_ps_supplycost],
                                            partsupp)),
                                        lineitem))))),
                              filter((strpos(p_name, "test") = 1), part)))))))))))))
|}]
end
