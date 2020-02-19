open Db
open Relation

let conn = Test_util.tpch_conn |> Lazy.force

let%expect_test "" =
  Db.all_relations conn
  |> List.iter ~f:(fun r ->
         Option.iter r.r_schema ~f:(fun s ->
             List.map s ~f:(fun n -> (n, Name.type_exn n))
             |> [%sexp_of: (Name.t * Prim_type.t) list] |> print_s));
  [%expect
    {|
    ((((name r_regionkey)) (IntT)) (((name r_name)) (StringT (padded)))
     (((name r_comment)) (StringT)))
    ((((name n_nationkey)) (IntT)) (((name n_name)) (StringT (padded)))
     (((name n_regionkey)) (IntT)) (((name n_comment)) (StringT)))
    ((((name l_orderkey)) (IntT)) (((name l_partkey)) (IntT))
     (((name l_suppkey)) (IntT)) (((name l_linenumber)) (IntT))
     (((name l_quantity)) (IntT)) (((name l_extendedprice)) (FixedT))
     (((name l_discount)) (FixedT)) (((name l_tax)) (FixedT))
     (((name l_returnflag)) (StringT (padded)))
     (((name l_linestatus)) (StringT (padded))) (((name l_shipdate)) (DateT))
     (((name l_commitdate)) (DateT)) (((name l_receiptdate)) (DateT))
     (((name l_shipinstruct)) (StringT (padded)))
     (((name l_shipmode)) (StringT (padded))) (((name l_comment)) (StringT)))
    ((((name o_orderkey)) (IntT)) (((name o_custkey)) (IntT))
     (((name o_orderstatus)) (StringT (padded))) (((name o_totalprice)) (FixedT))
     (((name o_orderdate)) (DateT)) (((name o_orderpriority)) (StringT (padded)))
     (((name o_clerk)) (StringT (padded))) (((name o_shippriority)) (IntT))
     (((name o_comment)) (StringT)))
    ((((name s_suppkey)) (IntT)) (((name s_name)) (StringT (padded)))
     (((name s_address)) (StringT)) (((name s_nationkey)) (IntT))
     (((name s_phone)) (StringT (padded))) (((name s_acctbal)) (FixedT))
     (((name s_comment)) (StringT)))
    ((((name p_partkey)) (IntT)) (((name p_name)) (StringT))
     (((name p_mfgr)) (StringT (padded))) (((name p_brand)) (StringT (padded)))
     (((name p_type)) (StringT)) (((name p_size)) (IntT))
     (((name p_container)) (StringT (padded))) (((name p_retailprice)) (FixedT))
     (((name p_comment)) (StringT)))
    ((((name c_custkey)) (IntT)) (((name c_name)) (StringT))
     (((name c_address)) (StringT)) (((name c_nationkey)) (IntT))
     (((name c_phone)) (StringT (padded))) (((name c_acctbal)) (FixedT))
     (((name c_mktsegment)) (StringT (padded))) (((name c_comment)) (StringT)))
    ((((name ps_partkey)) (IntT)) (((name ps_suppkey)) (IntT))
     (((name ps_availqty)) (IntT)) (((name ps_supplycost)) (FixedT))
     (((name ps_comment)) (StringT))) |}]
