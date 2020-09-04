open Schema
open Abslayout_load
open Test_util

let%expect_test "" =
  let r =
    {|
select([x122, x123, x124, x125, x126, x127, x128],
      join((s7_o_orderdate = bnd3),
        select([s7_o_orderdate as bnd3],
          select([o_orderdate as s7_o_orderdate],
            dedup(
              select([o_orderdate as o_orderdate],
                dedup(select([o_orderdate as o_orderdate], orders)))))),
        select([s7_o_orderdate, s7_o_orderdate as x122, counter2 as x123,
                var0 as x124, x118 as x125, x119 as x126, x120 as x127,
                x121 as x128],
          atuple([join(true,
                    dedup(
                      select([s7_o_orderdate],
                        select([o_orderdate as s7_o_orderdate],
                          dedup(
                            select([o_orderdate as o_orderdate],
                              dedup(select([o_orderdate as o_orderdate], orders))))))),
                    select([0 as counter2, var0, null:int as x118,
                            null:fixed as x119, null:fixed as x120,
                            null:fixed as x121],
                      ascalar(0 as var0))),
                  select([1 as counter2, s7_o_orderdate, null:int as var0,
                          x118, x119, x120, x121],
                    select([s7_o_orderdate, x118, x119, x120, x121],
                      select([bnd0, bnd1, bnd2, s2_ct21, s2_l_discount,
                              s2_l_extendedprice, s7_o_orderdate, x118, x119,
                              x120, x121],
                        join((((s2_ct21 = bnd0) &&
                              ((s2_l_discount = bnd1) &&
                              (s2_l_extendedprice = bnd2))) &&
                             (s7_o_orderdate = d0)),
                          select([s2_ct21 as bnd0, s2_l_discount as bnd1,
                                  s2_l_extendedprice as bnd2, s7_o_orderdate],
                            select([ct21 as s2_ct21, l_discount as s2_l_discount,
                                    l_extendedprice as s2_l_extendedprice,
                                    s7_o_orderdate],
                              groupby([count() as ct21, l_extendedprice,
                                       l_discount, s7_o_orderdate],
                                [l_extendedprice, l_discount, s7_o_orderdate],
                                select([l_discount as l_discount,
                                        l_extendedprice as l_extendedprice,
                                        s7_o_orderdate],
                                  join(((o_orderdate = s7_o_orderdate) &&
                                       ((l_returnflag = "R") &&
                                       (l_orderkey = o_orderkey))),
                                    join(true,
                                      dedup(
                                        select([s7_o_orderdate],
                                          select([o_orderdate as s7_o_orderdate],
                                            dedup(
                                              select([o_orderdate as o_orderdate],
                                                dedup(
                                                  select([o_orderdate as o_orderdate],
                                                    orders))))))),
                                      lineitem),
                                    orders))))),
                          select([s7_o_orderdate as d0, s2_ct21, s2_l_discount,
                                  s2_l_extendedprice, x118, x119, x120, x121],
                            select([s2_ct21, s2_l_discount, s2_l_extendedprice,
                                    s7_o_orderdate, s2_ct21 as x118,
                                    s2_l_extendedprice as x119,
                                    s2_l_discount as x120, l_discount as x121],
                              select([s2_l_discount as l_discount, s2_ct21,
                                      s2_l_discount, s2_l_extendedprice,
                                      s7_o_orderdate],
                                join(true,
                                  dedup(
                                    select([s2_ct21, s2_l_discount,
                                            s2_l_extendedprice, s7_o_orderdate],
                                      select([ct21 as s2_ct21,
                                              l_discount as s2_l_discount,
                                              l_extendedprice as s2_l_extendedprice,
                                              s7_o_orderdate],
                                        groupby([count() as ct21,
                                                 l_extendedprice, l_discount,
                                                 s7_o_orderdate],
                                          [l_extendedprice, l_discount,
                                           s7_o_orderdate],
                                          select([l_discount as l_discount,
                                                  l_extendedprice as l_extendedprice,
                                                  s7_o_orderdate],
                                            join(((o_orderdate = s7_o_orderdate)
                                                 &&
                                                 ((l_returnflag = "R") &&
                                                 (l_orderkey = o_orderkey))),
                                              join(true,
                                                dedup(
                                                  select([s7_o_orderdate],
                                                    select([o_orderdate as s7_o_orderdate],
                                                      dedup(
                                                        select([o_orderdate as o_orderdate],
                                                          dedup(
                                                            select([o_orderdate as o_orderdate],
                                                              orders))))))),
                                                lineitem),
                                              orders)))))),
                                  ascalar(0 as x0)))))))))],
            concat))))
|}
    |> Abslayout_load.load_string_exn (Lazy.force Test_util.tpch_conn)
  in
  let r1 =
    {|
    join(true,
                    dedup(
                      select([s7_o_orderdate],
                        select([o_orderdate as s7_o_orderdate],
                          dedup(
                            select([o_orderdate as o_orderdate],
                              dedup(select([o_orderdate as o_orderdate], orders))))))),
                    select([0 as counter2, var0, null:int as x118,
                            null:fixed as x119, null:fixed as x120,
                            null:fixed as x121],
                      ascalar(0 as var0)))
|}
    |> Abslayout_load.load_string_exn (Lazy.force Test_util.tpch_conn)
  in
  let r2 =
    {|
select([1 as counter2, s7_o_orderdate, null:int as var0,
                          x118, x119, x120, x121],
                    select([s7_o_orderdate, x118, x119, x120, x121],
                      select([bnd0, bnd1, bnd2, s2_ct21, s2_l_discount,
                              s2_l_extendedprice, s7_o_orderdate, x118, x119,
                              x120, x121],
                        join((((s2_ct21 = bnd0) &&
                              ((s2_l_discount = bnd1) &&
                              (s2_l_extendedprice = bnd2))) &&
                             (s7_o_orderdate = d0)),
                          select([s2_ct21 as bnd0, s2_l_discount as bnd1,
                                  s2_l_extendedprice as bnd2, s7_o_orderdate],
                            select([ct21 as s2_ct21, l_discount as s2_l_discount,
                                    l_extendedprice as s2_l_extendedprice,
                                    s7_o_orderdate],
                              groupby([count() as ct21, l_extendedprice,
                                       l_discount, s7_o_orderdate],
                                [l_extendedprice, l_discount, s7_o_orderdate],
                                select([l_discount as l_discount,
                                        l_extendedprice as l_extendedprice,
                                        s7_o_orderdate],
                                  join(((o_orderdate = s7_o_orderdate) &&
                                       ((l_returnflag = "R") &&
                                       (l_orderkey = o_orderkey))),
                                    join(true,
                                      dedup(
                                        select([s7_o_orderdate],
                                          select([o_orderdate as s7_o_orderdate],
                                            dedup(
                                              select([o_orderdate as o_orderdate],
                                                dedup(
                                                  select([o_orderdate as o_orderdate],
                                                    orders))))))),
                                      lineitem),
                                    orders))))),
                          select([s7_o_orderdate as d0, s2_ct21, s2_l_discount,
                                  s2_l_extendedprice, x118, x119, x120, x121],
                            select([s2_ct21, s2_l_discount, s2_l_extendedprice,
                                    s7_o_orderdate, s2_ct21 as x118,
                                    s2_l_extendedprice as x119,
                                    s2_l_discount as x120, l_discount as x121],
                              select([s2_l_discount as l_discount, s2_ct21,
                                      s2_l_discount, s2_l_extendedprice,
                                      s7_o_orderdate],
                                join(true,
                                  dedup(
                                    select([s2_ct21, s2_l_discount,
                                            s2_l_extendedprice, s7_o_orderdate],
                                      select([ct21 as s2_ct21,
                                              l_discount as s2_l_discount,
                                              l_extendedprice as s2_l_extendedprice,
                                              s7_o_orderdate],
                                        groupby([count() as ct21,
                                                 l_extendedprice, l_discount,
                                                 s7_o_orderdate],
                                          [l_extendedprice, l_discount,
                                           s7_o_orderdate],
                                          select([l_discount as l_discount,
                                                  l_extendedprice as l_extendedprice,
                                                  s7_o_orderdate],
                                            join(((o_orderdate = s7_o_orderdate)
                                                 &&
                                                 ((l_returnflag = "R") &&
                                                 (l_orderkey = o_orderkey))),
                                              join(true,
                                                dedup(
                                                  select([s7_o_orderdate],
                                                    select([o_orderdate as s7_o_orderdate],
                                                      dedup(
                                                        select([o_orderdate as o_orderdate],
                                                          dedup(
                                                            select([o_orderdate as o_orderdate],
                                                              orders))))))),
                                                lineitem),
                                              orders)))))),
                                  ascalar(0 as x0)))))))))
  |}
    |> load_string_exn @@ Lazy.force tpch_conn
  in
  schema r |> Fmt.pr "%a" pp;
  [%expect {| [x122; x123; x124; x125; x126; x127; x128] |}];
  let s1 = schema r1 and s2 = schema r2 in
  Fmt.pr "%a" pp s1;
  [%expect {| [s7_o_orderdate; counter2; var0; x118; x119; x120; x121] |}];
  Fmt.pr "%a" pp s2;
  [%expect {| [counter2; s7_o_orderdate; var0; x118; x119; x120; x121] |}];
  [%compare.equal: Schema.t] s1 s2 |> Fmt.pr "%b";
  [%expect "false"];
  [%compare.equal: Name.t list] s1 s2 |> Fmt.pr "%b";
  [%expect "false"]

let%expect_test "" =
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
  Fmt.pr "%a@." pp (schema r);
  [%expect {| [ps_partkey; ps_suppkey; ps_availqty; ps_supplycost; ps_comment] |}]
