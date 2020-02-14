open Db
open Relation

let conn = Test_util.tpch_conn |> Lazy.force

let%expect_test "" =
  Db.all_relations conn
  |> List.iter ~f:(fun r ->
         Option.iter r.r_schema ~f:(fun s ->
             List.map s ~f:(fun n -> (n, Name.type_exn n))
             |> [%sexp_of: (Name.t * Prim_type.t) list] |> print_s));
  [%expect {|
    ((((scope ()) (name r_regionkey)) (IntT (nullable false)))
     (((scope ()) (name r_name)) (StringT (nullable false) (padded true)))
     (((scope ()) (name r_comment)) (StringT (nullable false) (padded false))))
    ((((scope ()) (name n_nationkey)) (IntT (nullable false)))
     (((scope ()) (name n_name)) (StringT (nullable false) (padded true)))
     (((scope ()) (name n_regionkey)) (IntT (nullable false)))
     (((scope ()) (name n_comment)) (StringT (nullable false) (padded false))))
    ((((scope ()) (name l_orderkey)) (IntT (nullable false)))
     (((scope ()) (name l_partkey)) (IntT (nullable false)))
     (((scope ()) (name l_suppkey)) (IntT (nullable false)))
     (((scope ()) (name l_linenumber)) (IntT (nullable false)))
     (((scope ()) (name l_quantity)) (IntT (nullable false)))
     (((scope ()) (name l_extendedprice)) (FixedT (nullable false)))
     (((scope ()) (name l_discount)) (FixedT (nullable false)))
     (((scope ()) (name l_tax)) (FixedT (nullable false)))
     (((scope ()) (name l_returnflag)) (StringT (nullable false) (padded true)))
     (((scope ()) (name l_linestatus)) (StringT (nullable false) (padded true)))
     (((scope ()) (name l_shipdate)) (DateT (nullable false)))
     (((scope ()) (name l_commitdate)) (DateT (nullable false)))
     (((scope ()) (name l_receiptdate)) (DateT (nullable false)))
     (((scope ()) (name l_shipinstruct))
      (StringT (nullable false) (padded true)))
     (((scope ()) (name l_shipmode)) (StringT (nullable false) (padded true)))
     (((scope ()) (name l_comment)) (StringT (nullable false) (padded false))))
    ((((scope ()) (name o_orderkey)) (IntT (nullable false)))
     (((scope ()) (name o_custkey)) (IntT (nullable false)))
     (((scope ()) (name o_orderstatus)) (StringT (nullable false) (padded true)))
     (((scope ()) (name o_totalprice)) (FixedT (nullable false)))
     (((scope ()) (name o_orderdate)) (DateT (nullable false)))
     (((scope ()) (name o_orderpriority))
      (StringT (nullable false) (padded true)))
     (((scope ()) (name o_clerk)) (StringT (nullable false) (padded true)))
     (((scope ()) (name o_shippriority)) (IntT (nullable false)))
     (((scope ()) (name o_comment)) (StringT (nullable false) (padded false))))
    ((((scope ()) (name s_suppkey)) (IntT (nullable false)))
     (((scope ()) (name s_name)) (StringT (nullable false) (padded true)))
     (((scope ()) (name s_address)) (StringT (nullable false) (padded false)))
     (((scope ()) (name s_nationkey)) (IntT (nullable false)))
     (((scope ()) (name s_phone)) (StringT (nullable false) (padded true)))
     (((scope ()) (name s_acctbal)) (FixedT (nullable false)))
     (((scope ()) (name s_comment)) (StringT (nullable false) (padded false))))
    ((((scope ()) (name p_partkey)) (IntT (nullable false)))
     (((scope ()) (name p_name)) (StringT (nullable false) (padded false)))
     (((scope ()) (name p_mfgr)) (StringT (nullable false) (padded true)))
     (((scope ()) (name p_brand)) (StringT (nullable false) (padded true)))
     (((scope ()) (name p_type)) (StringT (nullable false) (padded false)))
     (((scope ()) (name p_size)) (IntT (nullable false)))
     (((scope ()) (name p_container)) (StringT (nullable false) (padded true)))
     (((scope ()) (name p_retailprice)) (FixedT (nullable false)))
     (((scope ()) (name p_comment)) (StringT (nullable false) (padded false))))
    ((((scope ()) (name c_custkey)) (IntT (nullable false)))
     (((scope ()) (name c_name)) (StringT (nullable false) (padded false)))
     (((scope ()) (name c_address)) (StringT (nullable false) (padded false)))
     (((scope ()) (name c_nationkey)) (IntT (nullable false)))
     (((scope ()) (name c_phone)) (StringT (nullable false) (padded true)))
     (((scope ()) (name c_acctbal)) (FixedT (nullable false)))
     (((scope ()) (name c_mktsegment)) (StringT (nullable false) (padded true)))
     (((scope ()) (name c_comment)) (StringT (nullable false) (padded false))))
    ((((scope ()) (name ps_partkey)) (IntT (nullable false)))
     (((scope ()) (name ps_suppkey)) (IntT (nullable false)))
     (((scope ()) (name ps_availqty)) (IntT (nullable false)))
     (((scope ()) (name ps_supplycost)) (FixedT (nullable false)))
     (((scope ()) (name ps_comment)) (StringT (nullable false) (padded false)))) |}]
