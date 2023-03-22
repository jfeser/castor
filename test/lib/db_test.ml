open Db
open Relation

let schema = Test_util.tpch_schema |> Lazy.force

let%expect_test "" =
  Db.Schema.all_relations schema
  |> List.sort ~compare:[%compare: Relation.t]
  |> List.iter ~f:(fun r ->
         Option.iter r.r_schema ~f:(fun s ->
             [%sexp_of: (string * Prim_type.t) list] s |> print_s));
  [%expect
    {|
    ((c_acctbal (FixedT)) (c_address (StringT)) (c_comment (StringT))
     (c_custkey (IntT)) (c_mktsegment (StringT (padded))) (c_name (StringT))
     (c_nationkey (IntT)) (c_phone (StringT (padded))))
    ((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
     (l_extendedprice (FixedT)) (l_linenumber (IntT))
     (l_linestatus (StringT (padded))) (l_orderkey (IntT)) (l_partkey (IntT))
     (l_quantity (IntT)) (l_receiptdate (DateT))
     (l_returnflag (StringT (padded))) (l_shipdate (DateT))
     (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
     (l_suppkey (IntT)) (l_tax (FixedT)))
    ((n_comment (StringT)) (n_name (StringT (padded))) (n_nationkey (IntT))
     (n_regionkey (IntT)))
    ((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
     (o_orderdate (DateT)) (o_orderkey (IntT))
     (o_orderpriority (StringT (padded))) (o_orderstatus (StringT (padded)))
     (o_shippriority (IntT)) (o_totalprice (FixedT)))
    ((p_brand (StringT (padded))) (p_comment (StringT))
     (p_container (StringT (padded))) (p_mfgr (StringT (padded)))
     (p_name (StringT)) (p_partkey (IntT)) (p_retailprice (FixedT))
     (p_size (IntT)) (p_type (StringT)))
    ((ps_availqty (IntT)) (ps_comment (StringT)) (ps_partkey (IntT))
     (ps_suppkey (IntT)) (ps_supplycost (FixedT)))
    ((r_comment (StringT)) (r_name (StringT (padded))) (r_regionkey (IntT)))
    ((s_acctbal (FixedT)) (s_address (StringT)) (s_comment (StringT))
     (s_name (StringT (padded))) (s_nationkey (IntT))
     (s_phone (StringT (padded))) (s_suppkey (IntT))) |}]
