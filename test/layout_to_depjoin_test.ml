let%expect_test "" =
  Abslayout.of_string_exn
    {|
      ahashidx(dedup(
                         select([c_nationkey],
                           alist(join((l_orderkey = o_orderkey),
                                   join((c_custkey = o_custkey), customer, orders),
                                   lineitem) as s4,
                             atuple([ascalar(s4.c_nationkey)], cross)))) as s3,
                filter((s3.c_nationkey = c_nationkey),
                  alist(join((l_orderkey = o_orderkey), join((c_custkey = o_custkey), customer, orders),
 lineitem) as s1,
                    atuple([ascalar(s1.c_custkey), ascalar(s1.c_name),
                            ascalar(s1.c_address), ascalar(s1.c_nationkey),
                            ascalar(s1.c_phone), ascalar(s1.c_acctbal),
                            ascalar(s1.c_mktsegment), ascalar(s1.c_comment),
                            ascalar(s1.o_orderkey), ascalar(s1.o_custkey),
                            ascalar(s1.o_orderstatus), ascalar(s1.o_totalprice),
                            ascalar(s1.o_orderdate), ascalar(s1.o_orderpriority),
                            ascalar(s1.o_clerk), ascalar(s1.o_shippriority),
                            ascalar(s1.o_comment), ascalar(s1.l_orderkey),
                            ascalar(s1.l_partkey), ascalar(s1.l_suppkey),
                            ascalar(s1.l_linenumber), ascalar(s1.l_quantity),
                            ascalar(s1.l_extendedprice), ascalar(s1.l_discount),
                            ascalar(s1.l_tax), ascalar(s1.l_returnflag),
                            ascalar(s1.l_linestatus), ascalar(s1.l_shipdate),
                            ascalar(s1.l_commitdate), ascalar(s1.l_receiptdate),
                            ascalar(s1.l_shipinstruct), ascalar(s1.l_shipmode),
                            ascalar(s1.l_comment)],
                      cross))),
                s2.n_nationkey)
|}
  |> Layout_to_depjoin.annot |> Abslayout.pp Fmt.stdout;
  [%expect {|
    depjoin(dedup(
              select([c_nationkey],
                depjoin(join((l_orderkey = o_orderkey),
                          join((c_custkey = o_custkey), customer, orders),
                          lineitem) as s4,
                  select([s4.c_nationkey], ascalar(0 as x2))))) as s3,
      select([s3.c_nationkey, c_custkey, c_name, c_address, c_phone, c_acctbal,
              c_mktsegment, c_comment, o_orderkey, o_custkey, o_orderstatus,
              o_totalprice, o_orderdate, o_orderpriority, o_clerk,
              o_shippriority, o_comment, l_orderkey, l_partkey, l_suppkey,
              l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
              l_returnflag, l_linestatus, l_shipdate, l_commitdate,
              l_receiptdate, l_shipinstruct, l_shipmode, l_comment],
        filter((s3.c_nationkey = s2.n_nationkey),
          filter((s3.c_nationkey = c_nationkey),
            depjoin(join((l_orderkey = o_orderkey),
                      join((c_custkey = o_custkey), customer, orders),
                      lineitem) as s1,
              select([s1.c_custkey, s1.c_name, s1.c_address, s1.c_nationkey,
                      s1.c_phone, s1.c_acctbal, s1.c_mktsegment, s1.c_comment,
                      s1.o_orderkey, s1.o_custkey, s1.o_orderstatus,
                      s1.o_totalprice, s1.o_orderdate, s1.o_orderpriority,
                      s1.o_clerk, s1.o_shippriority, s1.o_comment, s1.l_orderkey,
                      s1.l_partkey, s1.l_suppkey, s1.l_linenumber, s1.l_quantity,
                      s1.l_extendedprice, s1.l_discount, s1.l_tax,
                      s1.l_returnflag, s1.l_linestatus, s1.l_shipdate,
                      s1.l_commitdate, s1.l_receiptdate, s1.l_shipinstruct,
                      s1.l_shipmode, s1.l_comment],
                ascalar(0 as x1))))))) |}]
