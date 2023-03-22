open Core
open Castor
open Option.Let_syntax
open Ast

let%expect_test "" =
  Db.with_conn "postgresql:///tpch_1k" @@ fun conn ->
  let schema = Db.schema conn in
  let q =
    Abslayout_load.load_string_exn
      ~params:(Set.of_list (module Name) [ Name.create "param0" ])
      schema "select([l_shipdate], filter(l_orderkey = param0, lineitem))"
  in
  let q' =
    Abslayout_load.load_string_exn schema
      "join(l_orderkey = o_orderkey, lineitem, orders)"
  in
  let module G = Egraph.AstEGraph in
  let g = G.create () in
  let _ = G.add_annot g q in
  let _ = G.add_annot g q' in
  let sqls =
    G.classes g
    |> Iter.filter_map (fun id ->
           let r = G.choose_exn g id in
           let%bind query = Subsumption.of_sql (Sql.of_ralgebra r) in
           return (id, query))
    |> Iter.to_list
  in
  let can_use =
    List.map sqls ~f:(fun (id, q) ->
        List.filter_map sqls ~f:(fun (id', q') ->
            if [%equal: Egraph.Id.t] id id' then None
            else
              match Subsumption.subsumes ~view:q ~query:q' with
              | Ok x -> return (id', x)
              | Error e ->
                  Error.pp Fmt.stderr e;
                  None))
  in
  print_s
    [%message
      (can_use
        : (Egraph.Id.t * (Sql.select_entry list * _ annot pred list)) list list)];
  Fmt.pr "%a" G.pp_dot g;
  [%expect
    {|
    (can_use
     (((1
        ((((pred (Name ((name (Simple l_comment))))) (alias l_comment) (cast ()))
          ((pred (Name ((name (Simple l_commitdate))))) (alias l_commitdate)
           (cast ()))
          ((pred (Name ((name (Simple l_discount))))) (alias l_discount)
           (cast ()))
          ((pred (Name ((name (Simple l_extendedprice)))))
           (alias l_extendedprice) (cast ()))
          ((pred (Name ((name (Simple l_linenumber))))) (alias l_linenumber)
           (cast ()))
          ((pred (Name ((name (Simple l_linestatus))))) (alias l_linestatus)
           (cast ()))
          ((pred (Name ((name (Simple l_orderkey))))) (alias l_orderkey)
           (cast ()))
          ((pred (Name ((name (Simple l_partkey))))) (alias l_partkey) (cast ()))
          ((pred (Name ((name (Simple l_quantity))))) (alias l_quantity)
           (cast ()))
          ((pred (Name ((name (Simple l_receiptdate))))) (alias l_receiptdate)
           (cast ()))
          ((pred (Name ((name (Simple l_returnflag))))) (alias l_returnflag)
           (cast ()))
          ((pred (Name ((name (Simple l_shipdate))))) (alias l_shipdate)
           (cast ()))
          ((pred (Name ((name (Simple l_shipinstruct))))) (alias l_shipinstruct)
           (cast ()))
          ((pred (Name ((name (Simple l_shipmode))))) (alias l_shipmode)
           (cast ()))
          ((pred (Name ((name (Simple l_suppkey))))) (alias l_suppkey) (cast ()))
          ((pred (Name ((name (Simple l_tax))))) (alias l_tax) (cast ())))
         ((Binop
           (Eq (Name ((name (Simple l_orderkey))))
            (Name ((name (Simple l_orderkey)))))))))
       (2
        ((((pred (Name ((name (Simple l_shipdate))))) (alias l_shipdate)
           (cast ())))
         ((Binop
           (Eq (Name ((name (Simple l_orderkey))))
            (Name ((name (Simple l_orderkey))))))))))
      ((2
        ((((pred (Name ((name (Simple l_shipdate))))) (alias l_shipdate)
           (cast ())))
         ())))
      () () ()))
    digraph {
    subgraph cluster_0 {
    label="Class 0"
    c0 [shape=point];
    n0_0 [label="lineitem"];
    }
    subgraph cluster_1 {
    label="Class 1"
    c1 [shape=point];
    n1_0 [label="filter((l_orderkey = param0), )"];
    }
    subgraph cluster_2 {
    label="Class 2"
    c2 [shape=point];
    n2_0 [label="select([l_shipdate], )"];
    }
    subgraph cluster_3 {
    label="Class 3"
    c3 [shape=point];
    n3_0 [label="orders"];
    }
    subgraph cluster_4 {
    label="Class 4"
    c4 [shape=point];
    n4_0 [label="join((l_orderkey = o_orderkey), , )"];
    }
    n0_0 -> {};
    n1_0 -> {c0};
    n2_0 -> {c1};
    n3_0 -> {};
    n4_0 -> {c0 c3};
    }
    ("different relations"
     (view_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)))
     (query_relations
      ((((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders))))("different relations"
     (view_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)))
     (query_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)
       (((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders))))("extra view filter"
     (p
      (((name (Simple param0)))
       ((name (Attr lineitem l_orderkey)) (type_ (IntT))))))("different relations"
     (view_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)))
     (query_relations
      ((((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders))))("different relations"
     (view_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)))
     (query_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)
       (((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders))))("extra view filter"
     (p
      (((name (Simple param0)))
       ((name (Attr lineitem l_orderkey)) (type_ (IntT))))))("no output column matching"
     (n ((name (Attr lineitem l_comment)) (type_ (StringT)))))("different relations"
     (view_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)))
     (query_relations
      ((((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders))))("different relations"
     (view_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)))
     (query_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)
       (((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders))))("different relations"
     (view_relations
      ((((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders)))
     (query_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem))))("different relations"
     (view_relations
      ((((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders)))
     (query_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem))))("different relations"
     (view_relations
      ((((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders)))
     (query_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem))))("different relations"
     (view_relations
      ((((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders)))
     (query_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)
       (((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders))))("different relations"
     (view_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)
       (((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders)))
     (query_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem))))("different relations"
     (view_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)
       (((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders)))
     (query_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem))))("different relations"
     (view_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)
       (((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders)))
     (query_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem))))("different relations"
     (view_relations
      ((((r_name lineitem)
         (r_schema
          (((l_comment (StringT)) (l_commitdate (DateT)) (l_discount (FixedT))
            (l_extendedprice (FixedT)) (l_linenumber (IntT))
            (l_linestatus (StringT (padded))) (l_orderkey (IntT))
            (l_partkey (IntT)) (l_quantity (IntT)) (l_receiptdate (DateT))
            (l_returnflag (StringT (padded))) (l_shipdate (DateT))
            (l_shipinstruct (StringT (padded))) (l_shipmode (StringT (padded)))
            (l_suppkey (IntT)) (l_tax (FixedT))))))
        lineitem)
       (((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders)))
     (query_relations
      ((((r_name orders)
         (r_schema
          (((o_clerk (StringT (padded))) (o_comment (StringT)) (o_custkey (IntT))
            (o_orderdate (DateT)) (o_orderkey (IntT))
            (o_orderpriority (StringT (padded)))
            (o_orderstatus (StringT (padded))) (o_shippriority (IntT))
            (o_totalprice (FixedT))))))
        orders)))) |}]
