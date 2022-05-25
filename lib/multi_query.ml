open Core
open Option.Let_syntax
open Ast

let%expect_test "" =
  Db.with_conn "postgresql:///tpch_1k" @@ fun conn ->
  let q =
    Abslayout_load.load_string_exn conn "select([l_orderkey], lineitem)"
  in
  let q' =
    Abslayout_load.load_string_exn conn
      "select([l_orderkey], filter(l_orderkey = 0, lineitem))"
  in
  let module G = Egraph.AstEGraph in
  let g = G.create () in
  let _ = G.add_annot g q in
  let _ = G.add_annot g q' in
  let sqls =
    G.classes g
    |> Iter.filter_map (fun id ->
           match G.choose_exn g id with
           | `Annot r ->
               let%bind query = Subsumption.of_sql (Sql.of_ralgebra r) in
               return (id, query)
           | _ -> None)
    |> Iter.to_list
  in
  let can_use =
    List.map sqls ~f:(fun (_, q) ->
        List.filter_map sqls ~f:(fun (id', q') ->
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
  [%expect
    {|
    (can_use
     (((5
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
         ()))
       (6
        ((((pred (Name ((name l_orderkey)))) (alias l_orderkey) (cast ()))) ())))
      ((2
        ((((pred (Name ((name l_orderkey)))) (alias l_orderkey) (cast ()))) ()))
       (6
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
       (0
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
         ()))
       (6
        ((((pred (Name ((name l_orderkey)))) (alias l_orderkey) (cast ())))
         ((Binop (Eq (Name ((name l_orderkey))) (Int 0)))))))
      ((6
        ((((pred (Name ((name l_orderkey)))) (alias l_orderkey) (cast ()))) ())))))
    ("extra view predicates"
     (extra_view_preds
      ((Binop (Eq (Name ((scope lineitem) (name l_orderkey))) (Int 0))))))("extra view predicates"
     (extra_view_preds
      ((Binop (Eq (Name ((scope lineitem) (name l_orderkey))) (Int 0))))))("no output column matching" (n ((scope lineitem) (name l_comment))))("no output column matching" (n ((scope lineitem) (name l_comment))))("no output column matching" (n ((scope lineitem) (name l_comment))))("extra view predicates"
     (extra_view_preds
      ((Binop (Eq (Name ((scope lineitem) (name l_orderkey))) (Int 0))))))("extra view predicates"
     (extra_view_preds
      ((Binop (Eq (Name ((scope lineitem) (name l_orderkey))) (Int 0)))))) |}]
