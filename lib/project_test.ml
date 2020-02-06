open! Core
open Project

module C = struct
  let conn = Db.create "postgresql:///tpch_1k"

  let verbose = false

  let validate = false

  let params =
    let open Prim_type in
    Set.of_list
      (module Name)
      [
        Name.create ~type_:string_t "param1";
        Name.create ~type_:string_t "param2";
        Name.create ~type_:string_t "param3";
      ]

  let param_ctx = Map.empty (module Name)

  let fresh = Fresh.create ()

  let simplify = None
end

open C
module M = Abslayout_db.Make (C)

let%expect_test "" =
  let r =
    M.load_string ~params
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
    M.load_string ~params
      {|groupby([count() as c], [o_orderdate], dedup(select([o_orderdate, o_orderkey], orders)))|}
  in
  project ~params r |> Format.printf "%a@." Abslayout.pp;
  [%expect
    {|
      groupby([count() as c],
        [o_orderdate],
        dedup(select([o_orderdate, o_orderkey], orders))) |}]
