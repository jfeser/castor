open Project
open Abslayout_load
open Test_util

let conn = Lazy.force tpch_conn

let params =
  let open Prim_type in
  Set.of_list
    (module Name)
    [
      Name.create ~type_:string_t "param1";
      Name.create ~type_:string_t "param2";
      Name.create ~type_:string_t "param3";
    ]

let%expect_test "" =
  let r =
    load_string_exn ~params conn
      {|
         alist(dedup(
                 select([o_year],
                   select([to_year(o_orderdate) as o_year,
                           (l_extendedprice * (1 - l_discount)) as volume,
                           n2_name as nation_name],
                     join((p_partkey = l_partkey),
                       join((s_suppkey = l_suppkey),
                         join((l_orderkey = o_orderkey),
                           join((o_custkey = c_custkey),
                             join((c_nationkey = n1_nationkey),
                               join((n1_regionkey = r_regionkey),
                                 select([n_regionkey as n1_regionkey,
                                         n_nationkey as n1_nationkey],
                                   nation),
                                 region),
                               customer),
                             filter(((o_orderdate >= date("1995-01-01")) &&
                                    (o_orderdate <= date("1996-12-31"))),
                               orders)),
                           lineitem),
                         join((s_nationkey = n2_nationkey),
                           select([n_nationkey as n2_nationkey, n_name as n2_name],
                             nation),
                           supplier)),
                       part)))),
           select([o_year,
                   (sum((if (nation_name = param1) then volume else 0.0)) /
                   sum(volume)) as mkt_share],
             atuple([ascalar(0.o_year),
                     select([volume, nation_name],
                       filter((o_year = 0.o_year),
                         select([to_year(o_orderdate) as o_year,
                                 (l_extendedprice * (1 - l_discount)) as volume,
                                 n2_name as nation_name],
                           join((p_partkey = l_partkey),
                             join((s_suppkey = l_suppkey),
                               join((l_orderkey = o_orderkey),
                                 join((o_custkey = c_custkey),
                                   join((c_nationkey = n1_nationkey),
                                     join((n1_regionkey = r_regionkey),
                                       select([n_regionkey as n1_regionkey,
                                               n_nationkey as n1_nationkey],
                                         nation),
                                       filter((r_name = param2), region)),
                                     customer),
                                   filter(((o_orderdate >= date("1995-01-01")) &&
                                          (o_orderdate <= date("1996-12-31"))),
                                     orders)),
                                 lineitem),
                               join((s_nationkey = n2_nationkey),
                                 select([n_nationkey as n2_nationkey,
                                         n_name as n2_name],
                                   nation),
                                 supplier)),
                             filter((p_type = param3), part)))))],
               cross)))
   |}
  in
  project ~params r |> Format.printf "%a@." Abslayout.pp;
  [%expect
    {|
         alist(dedup(
                 select([o_year],
                   select([to_year(o_orderdate) as o_year],
                     join((p_partkey = l_partkey),
                       join((s_suppkey = l_suppkey),
                         join((l_orderkey = o_orderkey),
                           join((o_custkey = c_custkey),
                             join((c_nationkey = n1_nationkey),
                               join((n1_regionkey = r_regionkey),
                                 select([n_regionkey as n1_regionkey,
                                         n_nationkey as n1_nationkey],
                                   nation),
                                 region),
                               customer),
                             filter(((o_orderdate >= date("1995-01-01")) &&
                                    (o_orderdate <= date("1996-12-31"))),
                               orders)),
                           lineitem),
                         join((s_nationkey = n2_nationkey),
                           select([n_nationkey as n2_nationkey], nation),
                           supplier)),
                       part)))),
           select([o_year,
                   (sum((if (nation_name = param1) then volume else 0.0)) / sum(volume)) as mkt_share],
             atuple([ascalar(0.o_year),
                     select([volume, nation_name],
                       filter((o_year = 0.o_year),
                         select([to_year(o_orderdate) as o_year,
                                 (l_extendedprice * (1 - l_discount)) as volume,
                                 n2_name as nation_name],
                           join((p_partkey = l_partkey),
                             join((s_suppkey = l_suppkey),
                               join((l_orderkey = o_orderkey),
                                 join((o_custkey = c_custkey),
                                   join((c_nationkey = n1_nationkey),
                                     join((n1_regionkey = r_regionkey),
                                       select([n_regionkey as n1_regionkey,
                                               n_nationkey as n1_nationkey],
                                         nation),
                                       filter((r_name = param2), region)),
                                     customer),
                                   filter(((o_orderdate >= date("1995-01-01")) &&
                                          (o_orderdate <= date("1996-12-31"))),
                                     orders)),
                                 lineitem),
                               join((s_nationkey = n2_nationkey),
                                 select([n_nationkey as n2_nationkey,
                                         n_name as n2_name],
                                   nation),
                                 supplier)),
                             filter((p_type = param3), part)))))],
               cross))) |}]

let%expect_test "" =
  let r =
    load_string_exn ~params conn
      {|groupby([count() as c], [o_orderdate], dedup(select([o_orderdate, o_orderkey], orders)))|}
  in
  project ~params r |> Format.printf "%a@." Abslayout.pp;
  [%expect
    {|
         groupby([count() as c],
           [o_orderdate],
           dedup(select([o_orderdate, o_orderkey], orders))) |}]

let run_test ?(params = Set.empty (module Name)) conn s =
  load_string_exn ~params conn s
  |> project ~params
  |> Format.printf "%a@." Abslayout.pp

let%expect_test "" =
  run_test
    (Lazy.force Test_util.test_db_conn)
    {|
   groupby([min(ct2) as x12, max(ct2) as x13], [],
    groupby([count() as ct2], [], select([f], r1)))
   |};
  [%expect
    {|
       groupby([min(ct2) as x12, max(ct2) as x13],
         [],
         groupby([count() as ct2], [], select([false as dummy], r1))) |}]

let%expect_test "filter-exists" =
  run_test
    (Lazy.force Test_util.tpch_conn)
    {|
       filter(exists(groupby([l_orderkey, sum(l_quantity) as sum_l_quantity], [l_orderkey], lineitem)),
                     orders)
   |};
  [%expect
    {|
       filter(exists(groupby([l_orderkey, sum(l_quantity) as sum_l_quantity],
                       [l_orderkey],
                       lineitem)),
         orders) |}]

let%expect_test "" =
  run_test
    (Lazy.force Test_util.tpch_conn)
    {|
   groupby([min(ct0) as x0, max(ct0) as x1],
     [],
     groupby([count() as ct0], [], select([c_mktsegment as k0], dedup(select([c_mktsegment], customer)))))
   |};
  [%expect
    {|
       groupby([min(ct0) as x0, max(ct0) as x1],
         [],
         groupby([count() as ct0],
           [],
           select([false as dummy], dedup(select([c_mktsegment], customer))))) |}]

let%expect_test "" =
  run_test
    (Lazy.force Test_util.tpch_conn)
    {|
   groupby([min(ct0) as x0, max(ct0) as x1],
     [],
     groupby([count() as ct0], [], select([c_mktsegment as k0], dedup(select([c_mktsegment], customer)))))
   |};
  [%expect
    {|
       groupby([min(ct0) as x0, max(ct0) as x1],
         [],
         groupby([count() as ct0],
           [],
           select([false as dummy], dedup(select([c_mktsegment], customer))))) |}]

let%expect_test "" =
  run_test
    (Lazy.force Test_util.tpch_conn)
    {|
   select([p_brand, p_type, p_size, count() as supplier_cnt],
   dedup(select([p_type, p_brand, p_size, ps_suppkey],
   dedup(join((p_partkey = ps_partkey), part, partsupp)))))
   |};
  [%expect
    {|
       select([p_brand, p_type, p_size, count() as supplier_cnt],
         dedup(
           select([p_type, p_brand, p_size, ps_suppkey],
             dedup(join((p_partkey = ps_partkey), part, partsupp)))))
       |}]

let%expect_test "" =
  run_test
    (Lazy.force Test_util.tpch_conn)
    {|
       select([p_brand, p_type, p_size, count() as supplier_cnt],
         alist(dedup(join(true, part, partsupp)),
           select([p_type, p_brand, p_size, ps_suppkey],
               atuple([ascalar(0.p_partkey), ascalar(0.p_name), ascalar(0.p_mfgr),
                       ascalar(0.p_brand), ascalar(0.p_type), ascalar(0.p_size),
                       ascalar(0.p_container), ascalar(0.p_retailprice),
                       ascalar(0.p_comment), ascalar(0.ps_partkey), ascalar(0.ps_suppkey),
                       ascalar(0.ps_availqty), ascalar(0.ps_supplycost),
                       ascalar(0.ps_comment)],
                 cross))))
   |};
  [%expect
    {|
       select([p_brand, p_type, p_size, count() as supplier_cnt],
         alist(select([p_brand, p_size, p_type],
                 select([p_brand, p_size, p_type],
                   dedup(join(true, part, partsupp)))),
           select([p_type, p_brand, p_size],
             atuple([ascalar(0.p_brand), ascalar(0.p_type), ascalar(0.p_size)],
               cross)))) |}]

let%expect_test "" =
  run_test
    (Lazy.force Test_util.tpch_conn)
    {|
       alist(dedup(
               select([p_brand, p_type, p_size],
                 dedup(
                   select([p_type, p_brand, p_size, ps_suppkey],
                     join(((p_partkey = ps_partkey) && true),
                       part,
                       filter(not(exists(filter(((ps_suppkey = s_suppkey) &&
                                                ((strpos(s_comment, "Customer") >=
                                                 1) &&
                                                (strpos(s_comment, "Complaints") >=
                                                1))),
                                           supplier))),
                         partsupp)))))),
         select([p_brand, p_type, p_size, supplier_cnt],
           select([p_brand, p_type, p_size, count() as supplier_cnt],
             dedup(
               alist(join(((not(exists(filter(((ps_suppkey = s_suppkey) &&
                                              ((strpos(s_comment, "Customer") >= 1)
                                              &&
                                              (strpos(s_comment, "Complaints") >= 1))),
                                         supplier))) &&
                           ((p_brand = 0.p_brand) &&
                           ((p_type = 0.p_type) && (p_size = 0.p_size)))) &&
                          (p_partkey = ps_partkey)),
                       part,
                       partsupp),
                 select([p_type, p_brand, p_size, ps_suppkey],
                   filter((not((p_brand = "")) &&
                          (not((strpos(p_type, "") = 1)) &&
                          ((p_size = 0) ||
                          ((p_size = 0) ||
                          ((p_size = 0) ||
                          ((p_size = 0) ||
                          ((p_size = 0) ||
                          ((p_size = 0) || ((p_size = 0) || (p_size = 0)))))))))),
                     atuple([ascalar(0.p_partkey), ascalar(0.p_name),
                             ascalar(0.p_mfgr), ascalar(0.p_brand),
                             ascalar(0.p_type), ascalar(0.p_size),
                             ascalar(0.p_container), ascalar(0.p_retailprice),
                             ascalar(0.p_comment), ascalar(0.ps_partkey),
                             ascalar(0.ps_suppkey), ascalar(0.ps_availqty),
                             ascalar(0.ps_supplycost), ascalar(0.ps_comment)],
                       cross))))))))
   |};
  [%expect
    {|
       alist(dedup(
               select([p_brand, p_type, p_size],
                 dedup(
                   select([p_type, p_brand, p_size],
                     join(((p_partkey = ps_partkey) && true),
                       part,
                       filter(not(exists(filter(((ps_suppkey = s_suppkey) &&
                                                ((strpos(s_comment, "Customer") >=
                                                 1) &&
                                                (strpos(s_comment, "Complaints") >=
                                                1))),
                                           supplier))),
                         partsupp)))))),
         select([p_brand, p_type, p_size, supplier_cnt],
           select([p_brand, p_type, p_size, count() as supplier_cnt],
             dedup(
               alist(select([p_brand, p_size, p_type, ps_suppkey],
                       select([p_brand, p_size, p_type, ps_suppkey],
                         join(((not(exists(filter(((ps_suppkey = s_suppkey) &&
                                                  ((strpos(s_comment, "Customer") >=
                                                   1) &&
                                                  (strpos(s_comment, "Complaints")
                                                  >= 1))),
                                             supplier))) &&
                               ((p_brand = 0.p_brand) &&
                               ((p_type = 0.p_type) && (p_size = 0.p_size)))) &&
                              (p_partkey = ps_partkey)),
                           part,
                           partsupp))),
                 select([p_type, p_brand, p_size, ps_suppkey],
                   filter((not((p_brand = "")) &&
                          (not((strpos(p_type, "") = 1)) &&
                          ((p_size = 0) ||
                          ((p_size = 0) ||
                          ((p_size = 0) ||
                          ((p_size = 0) ||
                          ((p_size = 0) ||
                          ((p_size = 0) || ((p_size = 0) || (p_size = 0)))))))))),
                     atuple([ascalar(0.p_brand), ascalar(0.p_type),
                             ascalar(0.p_size), ascalar(0.ps_suppkey)],
                       cross)))))))) |}]
