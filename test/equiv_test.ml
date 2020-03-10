open Test_util

let run_test conn str =
  Abslayout_load.load_string conn str
  |> Equiv.eqs |> Set.to_list
  |> Format.printf "%a" Fmt.Dump.(list @@ pair Name.pp Name.pp)

let%expect_test "" =
  run_test (Lazy.force test_db_conn)
    "join(true, select([id as p_id], log), select([id as c_id], log))";
  [%expect {| [] |}]

let%expect_test "" =
  run_test (Lazy.force tpch_conn)
    {|
alist(dedup(select([l_suppkey as l1_suppkey], lineitem)) as k0,
                  select([l_suppkey as supplier_no, sum(agg0) as total_revenue],
                    aorderedidx(dedup(select([l_shipdate], lineitem)) as s4,
                      filter((count0 > 0),
                        select([count() as count0,
                                sum((l_extendedprice * (1 - l_discount))) as agg0,
                                l_suppkey, l_extendedprice, l_discount],
                          atuple([ascalar(s4.l_shipdate),
                                  alist(select([l_suppkey, l_extendedprice,
                                                l_discount],
                                          filter(((l_suppkey = k0.l1_suppkey) &&
                                                 (l_shipdate = s4.l_shipdate)),
                                            lineitem)) as s5,
                                    atuple([ascalar(s5.l_suppkey),
                                            ascalar(s5.l_extendedprice),
                                            ascalar(s5.l_discount)],
                                      cross))],
                            cross))),
                      >= date("0000-01-01"), < (date("0000-01-01") + month(3)))))
|};
  [%expect {|
    [(l1_suppkey, k0.l1_suppkey); (l_suppkey, l1_suppkey);
     (l_suppkey, supplier_no)] |}]
