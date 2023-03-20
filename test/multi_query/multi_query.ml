open Core
open Castor
open Option.Let_syntax
open Ast

let%expect_test "" =
  Db.with_conn "postgresql:///tpch_1k" @@ fun conn ->
  let q =
    Abslayout_load.load_string_exn
      ~params:(Set.of_list (module Name) [ Name.create "param0" ])
      conn "select([l_shipdate], filter(l_orderkey = param0, lineitem))"
  in
  let q' =
    Abslayout_load.load_string_exn conn
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
     (((6
        ((((pred (Name ((name l_orderkey)))) (alias l_orderkey) (cast ()))) ())))
      ((6
        ((((pred (Name ((name l_orderkey)))) (alias l_orderkey) (cast ())))
         ((Binop (Eq (Name ((name l_orderkey))) (Int 0)))))))
      ((5
        ((((pred (Name ((name l_comment)))) (alias l_comment) (cast ()))
          ((pred (Name ((name l_commitdate)))) (alias l_commitdate) (cast ()))
          ((pred (Name ((name l_discount)))) (alias l_discount) (cast ()))
          ((pred (Name ((name l_extendedprice)))) (alias l_extendedprice)
           (cast ()))
          ((pred (Name ((name l_linenumber)))) (alias l_linenumber) (cast ()))
          ((pred (Name ((name l_linestatus)))) (alias l_linestatus) (cast ()))
          ((pred (Name ((name l_orderkey)))) (alias l_orderkey) (cast ()))
          ((pred (Name ((name l_partkey)))) (alias l_partkey) (cast ()))
          ((pred (Name ((name l_quantity)))) (alias l_quantity) (cast ()))
          ((pred (Name ((name l_receiptdate)))) (alias l_receiptdate) (cast ()))
          ((pred (Name ((name l_returnflag)))) (alias l_returnflag) (cast ()))
          ((pred (Name ((name l_shipdate)))) (alias l_shipdate) (cast ()))
          ((pred (Name ((name l_shipinstruct)))) (alias l_shipinstruct)
           (cast ()))
          ((pred (Name ((name l_shipmode)))) (alias l_shipmode) (cast ()))
          ((pred (Name ((name l_suppkey)))) (alias l_suppkey) (cast ()))
          ((pred (Name ((name l_tax)))) (alias l_tax) (cast ())))
         ((Binop (Eq (Name ((name l_orderkey))) (Int 0))))))
       (2
        ((((pred (Name ((name l_orderkey)))) (alias l_orderkey) (cast ()))) ()))
       (6
        ((((pred (Name ((name l_orderkey)))) (alias l_orderkey) (cast ())))
         ((Binop (Eq (Name ((name l_orderkey))) (Int 0)))))))
      ()))
    digraph {
    subgraph cluster_4 {
    label="Class 4"
    c4 [shape=point];
    n4_0 [label="( = )"];
    }
    subgraph cluster_5 {
    label="Class 5"
    c5 [shape=point];
    n5_0 [label="filter(, )"];
    }
    subgraph cluster_2 {
    label="Class 2"
    c2 [shape=point];
    n2_0 [label="select([ as l_orderkey], )"];
    }
    subgraph cluster_0 {
    label="Class 0"
    c0 [shape=point];
    n0_0 [label="lineitem"];
    }
    subgraph cluster_1 {
    label="Class 1"
    c1 [shape=point];
    n1_0 [label="l_orderkey"];
    }
    subgraph cluster_3 {
    label="Class 3"
    c3 [shape=point];
    n3_0 [label="0"];
    }
    subgraph cluster_6 {
    label="Class 6"
    c6 [shape=point];
    n6_0 [label="select([ as l_orderkey], )"];
    }
    n4_0 -> {c1 c3};
    n5_0 -> {c4 c0};
    n2_0 -> {c1 c0};
    n0_0 -> {};
    n1_0 -> {};
    n3_0 -> {};
    n6_0 -> {c1 c5};
    }
    ("extra view predicates"
     (extra_view_preds
      ((Binop (Eq (Name ((scope lineitem) (name l_orderkey))) (Int 0))))))("extra view predicates"
     (extra_view_preds
      ((Binop (Eq (Name ((scope lineitem) (name l_orderkey))) (Int 0))))))("no output column matching" (n ((scope lineitem) (name l_comment))))("no output column matching" (n ((scope lineitem) (name l_comment))))("no output column matching" (n ((scope lineitem) (name l_comment))))("extra view predicates"
     (extra_view_preds
      ((Binop (Eq (Name ((scope lineitem) (name l_orderkey))) (Int 0))))))("extra view predicates"
     (extra_view_preds
      ((Binop (Eq (Name ((scope lineitem) (name l_orderkey))) (Int 0)))))) |}]
