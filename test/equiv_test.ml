open Test_util

let run_test conn str =
  Abslayout_load.load_string_exn conn str
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
    [(l_suppkey, l1_suppkey); (l_suppkey, supplier_no)] |}]

let%expect_test "" =
  run_test (Lazy.force tpch_conn)
    {|
join(((l_partkey = s3_ps_partkey) &&
     ((l_suppkey = s3_ps_suppkey) &&
     ((l_shipdate >= date("1995-01-02")) && ((l_shipdate < (date("1995-01-02") + year(1))) && true)))),
  dedup(
    select([ps_availqty as s3_ps_availqty, 
            ps_comment as s3_ps_comment, 
            ps_partkey as s3_ps_partkey, 
            ps_suppkey as s3_ps_suppkey, 
            ps_supplycost as s3_ps_supplycost], partsupp)),
  lineitem)
|};
  [%expect {|
    [(l_partkey, s3_ps_partkey); (l_suppkey, s3_ps_suppkey);
     (ps_availqty, s3_ps_availqty); (ps_comment, s3_ps_comment);
     (ps_partkey, s3_ps_partkey); (ps_suppkey, s3_ps_suppkey);
     (ps_supplycost, s3_ps_supplycost)] |}]
