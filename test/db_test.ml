open Db
open Relation

let conn = Test_util.tpch_conn |> Lazy.force

let%expect_test "" =
  Db.all_relations conn
  |> List.sort ~compare:[%compare: Relation.t]
  |> List.iter ~f:(fun r ->
         Option.iter r.r_schema ~f:(fun s ->
             [%sexp_of: (Name.t * Prim_type.t) list] s |> print_s));
  [%expect
    {|
    ((((name c_custkey) (meta <opaque>)) (IntT))
     (((name c_name) (meta <opaque>)) (StringT))
     (((name c_address) (meta <opaque>)) (StringT))
     (((name c_nationkey) (meta <opaque>)) (IntT))
     (((name c_phone) (meta <opaque>)) (StringT (padded)))
     (((name c_acctbal) (meta <opaque>)) (FixedT))
     (((name c_mktsegment) (meta <opaque>)) (StringT (padded)))
     (((name c_comment) (meta <opaque>)) (StringT)))
    ((((name l_orderkey) (meta <opaque>)) (IntT))
     (((name l_partkey) (meta <opaque>)) (IntT))
     (((name l_suppkey) (meta <opaque>)) (IntT))
     (((name l_linenumber) (meta <opaque>)) (IntT))
     (((name l_quantity) (meta <opaque>)) (IntT))
     (((name l_extendedprice) (meta <opaque>)) (FixedT))
     (((name l_discount) (meta <opaque>)) (FixedT))
     (((name l_tax) (meta <opaque>)) (FixedT))
     (((name l_returnflag) (meta <opaque>)) (StringT (padded)))
     (((name l_linestatus) (meta <opaque>)) (StringT (padded)))
     (((name l_shipdate) (meta <opaque>)) (DateT))
     (((name l_commitdate) (meta <opaque>)) (DateT))
     (((name l_receiptdate) (meta <opaque>)) (DateT))
     (((name l_shipinstruct) (meta <opaque>)) (StringT (padded)))
     (((name l_shipmode) (meta <opaque>)) (StringT (padded)))
     (((name l_comment) (meta <opaque>)) (StringT)))
    ((((name n_nationkey) (meta <opaque>)) (IntT))
     (((name n_name) (meta <opaque>)) (StringT (padded)))
     (((name n_regionkey) (meta <opaque>)) (IntT))
     (((name n_comment) (meta <opaque>)) (StringT)))
    ((((name o_orderkey) (meta <opaque>)) (IntT))
     (((name o_custkey) (meta <opaque>)) (IntT))
     (((name o_orderstatus) (meta <opaque>)) (StringT (padded)))
     (((name o_totalprice) (meta <opaque>)) (FixedT))
     (((name o_orderdate) (meta <opaque>)) (DateT))
     (((name o_orderpriority) (meta <opaque>)) (StringT (padded)))
     (((name o_clerk) (meta <opaque>)) (StringT (padded)))
     (((name o_shippriority) (meta <opaque>)) (IntT))
     (((name o_comment) (meta <opaque>)) (StringT)))
    ((((name p_partkey) (meta <opaque>)) (IntT))
     (((name p_name) (meta <opaque>)) (StringT))
     (((name p_mfgr) (meta <opaque>)) (StringT (padded)))
     (((name p_brand) (meta <opaque>)) (StringT (padded)))
     (((name p_type) (meta <opaque>)) (StringT))
     (((name p_size) (meta <opaque>)) (IntT))
     (((name p_container) (meta <opaque>)) (StringT (padded)))
     (((name p_retailprice) (meta <opaque>)) (FixedT))
     (((name p_comment) (meta <opaque>)) (StringT)))
    ((((name ps_partkey) (meta <opaque>)) (IntT))
     (((name ps_suppkey) (meta <opaque>)) (IntT))
     (((name ps_availqty) (meta <opaque>)) (IntT))
     (((name ps_supplycost) (meta <opaque>)) (FixedT))
     (((name ps_comment) (meta <opaque>)) (StringT)))
    ((((name r_regionkey) (meta <opaque>)) (IntT))
     (((name r_name) (meta <opaque>)) (StringT (padded)))
     (((name r_comment) (meta <opaque>)) (StringT)))
    ((((name s_suppkey) (meta <opaque>)) (IntT))
     (((name s_name) (meta <opaque>)) (StringT (padded)))
     (((name s_address) (meta <opaque>)) (StringT))
     (((name s_nationkey) (meta <opaque>)) (IntT))
     (((name s_phone) (meta <opaque>)) (StringT (padded)))
     (((name s_acctbal) (meta <opaque>)) (FixedT))
     (((name s_comment) (meta <opaque>)) (StringT))) |}]
