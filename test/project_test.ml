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
    load_string ~params conn
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
                    part)))) as k0,
        select([o_year,
                (sum((if (nation_name = param1) then volume else 0.0)) /
                sum(volume)) as mkt_share],
          atuple([ascalar(k0.o_year),
                  select([volume, nation_name],
                    filter((o_year = k0.o_year),
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
                    part)))) as k0,
        select([o_year,
                (sum((if (nation_name = param1) then volume else 0.0)) /
                sum(volume)) as mkt_share],
          atuple([ascalar(k0.o_year),
                  select([volume, nation_name],
                    filter((o_year = k0.o_year),
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
    load_string ~params conn
      {|groupby([count() as c], [o_orderdate], dedup(select([o_orderdate, o_orderkey], orders)))|}
  in
  project ~params r |> Format.printf "%a@." Abslayout.pp;
  [%expect
    {|
      groupby([count() as c],
        [o_orderdate],
        dedup(select([o_orderdate, o_orderkey], orders))) |}]

let run_test ?params conn s =
  load_string ?params conn s |> project ?params
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
      groupby([count() as ct2], [], select([false as dummy9], r1))) |}]

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
  [%expect {|
    groupby([min(ct0) as x0, max(ct0) as x1],
      [],
      groupby([count() as ct0],
        [],
        select([false as dummy9], dedup(select([c_mktsegment], customer))))) |}]
