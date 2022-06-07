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
   alist(dedup(select([l_suppkey as l1_suppkey], lineitem)),
                     select([l_suppkey as supplier_no, sum(agg0) as total_revenue],
                       aorderedidx(dedup(select([l_shipdate], lineitem)),
                         filter((count0 > 0),
                           select([count() as count0,
                                   sum((l_extendedprice * (1 - l_discount))) as agg0,
                                   l_suppkey, l_extendedprice, l_discount],
                             atuple([ascalar(0.l_shipdate),
                                     alist(select([l_suppkey, l_extendedprice,
                                                   l_discount],
                                             filter(((l_suppkey = 1.l1_suppkey) &&
                                                    (l_shipdate = 0.l_shipdate)),
                                               lineitem)),
                                       atuple([ascalar(0.l_suppkey),
                                               ascalar(0.l_extendedprice),
                                               ascalar(0.l_discount)],
                                         cross))],
                               cross))),
                         >= date("0000-01-01"), < (date("0000-01-01") + month(3)))))
   |};
  [%expect
    {|
       [(l1_suppkey, l_suppkey); (l1_suppkey, supplier_no); (l_suppkey, l1_suppkey);
        (l_suppkey, supplier_no); (supplier_no, l1_suppkey);
        (supplier_no, l_suppkey)] |}]

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
  [%expect
    {|
       [(l_partkey, ps_partkey); (l_partkey, s3_ps_partkey);
        (l_suppkey, ps_suppkey); (l_suppkey, s3_ps_suppkey);
        (ps_availqty, s3_ps_availqty); (ps_comment, s3_ps_comment);
        (ps_partkey, l_partkey); (ps_partkey, s3_ps_partkey);
        (ps_suppkey, l_suppkey); (ps_suppkey, s3_ps_suppkey);
        (ps_supplycost, s3_ps_supplycost); (s3_ps_availqty, ps_availqty);
        (s3_ps_comment, ps_comment); (s3_ps_partkey, l_partkey);
        (s3_ps_partkey, ps_partkey); (s3_ps_suppkey, l_suppkey);
        (s3_ps_suppkey, ps_suppkey); (s3_ps_supplycost, ps_supplycost)] |}]

let%expect_test "" =
  run_test (Lazy.force tpch_conn)
    {|
   dedup(
       select([substring(c1_phone, 0, 2) as cntrycode, c1_acctbal],
         filter((true &&
                ((substring(c1_phone, 0, 2) = "") ||
                ((substring(c1_phone, 0, 2) = "") ||
                ((substring(c1_phone, 0, 2) = "") ||
                ((substring(c1_phone, 0, 2) = "") ||
                ((substring(c1_phone, 0, 2) = "") ||
                ((substring(c1_phone, 0, 2) = "") || (substring(c1_phone, 0, 2) = "")))))))),
           select([s0_c1_phone as c1_phone, s0_c1_acctbal as c1_acctbal, s0_c1_custkey as c1_custkey],
             join((s0_c1_acctbal > q0),
               dedup(
                 select([c1_acctbal as s0_c1_acctbal, c1_custkey as s0_c1_custkey, c1_phone as s0_c1_phone],
                   alist(select([c_phone as c1_phone, c_acctbal as c1_acctbal,
                                 c_custkey as c1_custkey, c_acctbal as s1_c1_acctbal,
                                 c_custkey as s1_c1_custkey, c_phone as s1_c1_phone],
                           dedup(select([c_phone, c_acctbal, c_custkey], customer))),
                     select([s1_c1_acctbal, s1_c1_custkey, s1_c1_phone,
                             s1_c1_phone as c1_phone, s1_c1_acctbal as c1_acctbal,
                             s1_c1_custkey as c1_custkey],
                       join((o_custkey = s1_c1_custkey),
                         dedup(
                           select([c_acctbal as s1_c1_acctbal, c_custkey as s1_c1_custkey, c_phone as s1_c1_phone],
                             filter(((c_phone = 0.c1_phone) &&
                                    ((c_acctbal = 0.c1_acctbal) &&
                                    ((c_custkey = 0.c1_custkey) &&
                                    ((c_acctbal = 0.s1_c1_acctbal) &&
                                    ((c_custkey = 0.s1_c1_custkey) && ((c_phone = 0.s1_c1_phone) && true)))))),
                               customer))),
                         orders))))),
               select([avg(c_acctbal) as q0],
                 filter(((c_acctbal > 0.0) &&
                        ((substring(c_phone, 0, 2) = "") ||
                        ((substring(c_phone, 0, 2) = "") ||
                        ((substring(c_phone, 0, 2) = "") ||
                        ((substring(c_phone, 0, 2) = "") ||
                        ((substring(c_phone, 0, 2) = "") ||
                        ((substring(c_phone, 0, 2) = "") || (substring(c_phone, 0, 2) = "")))))))),
                   customer)))))))
   |};
  [%expect
    {|
       [(c1_acctbal, c_acctbal); (c1_acctbal, s0_c1_acctbal);
        (c1_acctbal, s1_c1_acctbal); (c1_acctbal, 0.c1_acctbal);
        (c1_acctbal, 0.s1_c1_acctbal); (c1_custkey, c_custkey);
        (c1_custkey, o_custkey); (c1_custkey, s0_c1_custkey);
        (c1_custkey, s1_c1_custkey); (c1_custkey, 0.c1_custkey);
        (c1_custkey, 0.s1_c1_custkey); (c1_phone, c_phone); (c1_phone, s0_c1_phone);
        (c1_phone, s1_c1_phone); (c1_phone, 0.c1_phone); (c1_phone, 0.s1_c1_phone);
        (c_acctbal, c1_acctbal); (c_acctbal, s0_c1_acctbal);
        (c_acctbal, s1_c1_acctbal); (c_acctbal, 0.c1_acctbal);
        (c_acctbal, 0.s1_c1_acctbal); (c_custkey, c1_custkey);
        (c_custkey, o_custkey); (c_custkey, s0_c1_custkey);
        (c_custkey, s1_c1_custkey); (c_custkey, 0.c1_custkey);
        (c_custkey, 0.s1_c1_custkey); (c_phone, c1_phone); (c_phone, s0_c1_phone);
        (c_phone, s1_c1_phone); (c_phone, 0.c1_phone); (c_phone, 0.s1_c1_phone);
        (o_custkey, c1_custkey); (o_custkey, c_custkey); (o_custkey, s0_c1_custkey);
        (o_custkey, s1_c1_custkey); (o_custkey, 0.c1_custkey);
        (o_custkey, 0.s1_c1_custkey); (s0_c1_acctbal, c1_acctbal);
        (s0_c1_acctbal, c_acctbal); (s0_c1_acctbal, s1_c1_acctbal);
        (s0_c1_acctbal, 0.c1_acctbal); (s0_c1_acctbal, 0.s1_c1_acctbal);
        (s0_c1_custkey, c1_custkey); (s0_c1_custkey, c_custkey);
        (s0_c1_custkey, o_custkey); (s0_c1_custkey, s1_c1_custkey);
        (s0_c1_custkey, 0.c1_custkey); (s0_c1_custkey, 0.s1_c1_custkey);
        (s0_c1_phone, c1_phone); (s0_c1_phone, c_phone); (s0_c1_phone, s1_c1_phone);
        (s0_c1_phone, 0.c1_phone); (s0_c1_phone, 0.s1_c1_phone);
        (s1_c1_acctbal, c1_acctbal); (s1_c1_acctbal, c_acctbal);
        (s1_c1_acctbal, s0_c1_acctbal); (s1_c1_acctbal, 0.c1_acctbal);
        (s1_c1_acctbal, 0.s1_c1_acctbal); (s1_c1_custkey, c1_custkey);
        (s1_c1_custkey, c_custkey); (s1_c1_custkey, o_custkey);
        (s1_c1_custkey, s0_c1_custkey); (s1_c1_custkey, 0.c1_custkey);
        (s1_c1_custkey, 0.s1_c1_custkey); (s1_c1_phone, c1_phone);
        (s1_c1_phone, c_phone); (s1_c1_phone, s0_c1_phone);
        (s1_c1_phone, 0.c1_phone); (s1_c1_phone, 0.s1_c1_phone);
        (0.c1_acctbal, c1_acctbal); (0.c1_acctbal, c_acctbal);
        (0.c1_acctbal, s0_c1_acctbal); (0.c1_acctbal, s1_c1_acctbal);
        (0.c1_acctbal, 0.s1_c1_acctbal); (0.c1_custkey, c1_custkey);
        (0.c1_custkey, c_custkey); (0.c1_custkey, o_custkey);
        (0.c1_custkey, s0_c1_custkey); (0.c1_custkey, s1_c1_custkey);
        (0.c1_custkey, 0.s1_c1_custkey); (0.c1_phone, c1_phone);
        (0.c1_phone, c_phone); (0.c1_phone, s0_c1_phone); (0.c1_phone, s1_c1_phone);
        (0.c1_phone, 0.s1_c1_phone); (0.s1_c1_acctbal, c1_acctbal);
        (0.s1_c1_acctbal, c_acctbal); (0.s1_c1_acctbal, s0_c1_acctbal);
        (0.s1_c1_acctbal, s1_c1_acctbal); (0.s1_c1_acctbal, 0.c1_acctbal);
        (0.s1_c1_custkey, c1_custkey); (0.s1_c1_custkey, c_custkey);
        (0.s1_c1_custkey, o_custkey); (0.s1_c1_custkey, s0_c1_custkey);
        (0.s1_c1_custkey, s1_c1_custkey); (0.s1_c1_custkey, 0.c1_custkey);
        (0.s1_c1_phone, c1_phone); (0.s1_c1_phone, c_phone);
        (0.s1_c1_phone, s0_c1_phone); (0.s1_c1_phone, s1_c1_phone);
        (0.s1_c1_phone, 0.c1_phone)] |}]
