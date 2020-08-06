open Castor_test
open Collections
module A = Abslayout

let () = Logs.Src.set_level Join_opt.src (Some Warning)

module Config = struct
  let cost_conn = Db.create "postgresql:///tpch_1k"

  let conn = cost_conn

  let validate = false

  let param_ctx = Map.empty (module Name)

  let params = Set.empty (module Name)

  let verbose = false

  let simplify = None

  let random = Mcmc.Random_choice.create ()
end

open Ops.Make (Config)

open Join_opt.Make (Config)

module C =
( val Constructors.Annot.with_default
        (object
           method stage : Name.t -> [ `Compile | `Run | `No_scope ] =
             assert false
        end) )

let type_ = Prim_type.IntT { nullable = false }

let c_custkey = Name.create ~type_ "c_custkey"

let c_nationkey = Name.create ~type_ "c_nationkey"

let n_nationkey = Name.create ~type_ "n_nationkey"

let o_custkey = Name.create ~type_ "o_custkey"

let orders = Db.relation Config.cost_conn "orders"

let customer = Db.relation Config.cost_conn "customer"

let nation = Db.relation Config.cost_conn "nation"

let%expect_test "parted-cost" =
  estimate_ntuples_parted (Set.empty (module Name)) (Flat (C.relation orders))
  |> [%sexp_of: int * int * float] |> print_s;
  [%expect {| (1000 1000 1000) |}]

let%expect_test "parted-cost" =
  estimate_ntuples_parted
    (Set.singleton (module Name) o_custkey)
    (Flat (C.relation orders))
  |> [%sexp_of: int * int * float] |> print_s;
  [%expect {| (1 2 1.0060362173038229) |}]

let%expect_test "parted-cost" =
  estimate_ntuples_parted
    (Set.singleton (module Name) c_custkey)
    (Flat (C.relation customer))
  |> [%sexp_of: int * int * float] |> print_s;
  [%expect {| (1 1 1) |}]

let estimate_cost p r = [| size_cost p r; scan_cost p r |]

let%expect_test "cost" =
  estimate_cost
    (Set.empty (module Name))
    (Flat
       C.(
         join
           (Binop (Eq, Name c_custkey, Name o_custkey))
           (relation orders) (relation customer)))
  |> [%sexp_of: float array] |> print_s;
  [%expect {| (257016 68000) |}]

let%expect_test "cost" =
  estimate_cost
    (Set.empty (module Name))
    C.(
      Nest
        {
          pred = Binop (Eq, Name c_custkey, Name o_custkey);
          lhs = Flat (relation customer);
          rhs = Flat (relation orders);
        })
  |> [%sexp_of: float array] |> print_s;
  [%expect {| (272098 67808) |}]

let%expect_test "cost" =
  estimate_cost
    (Set.empty (module Name))
    C.(
      Flat
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (relation nation) (relation customer)))
  |> [%sexp_of: float array] |> print_s;
  estimate_cost
    (Set.empty (module Name))
    C.(
      Nest
        {
          pred = Binop (Eq, Name c_nationkey, Name n_nationkey);
          lhs = Flat (relation nation);
          rhs = Flat (relation customer);
        })
  |> [%sexp_of: float array] |> print_s;
  estimate_cost
    (Set.empty (module Name))
    C.(
      Hash
        {
          lkey = Name c_nationkey;
          rkey = Name n_nationkey;
          lhs = Flat (relation nation);
          rhs = Flat (relation customer);
        })
  |> [%sexp_of: float array] |> print_s;
  [%expect {|
    (193846 47712)
    (138044 32208)
    (137660 32233) |}]

let%expect_test "to-from-ralgebra" =
  let r =
    C.(
      join
        (Binop (Eq, Name c_nationkey, Name n_nationkey))
        (relation nation) (relation customer))
  in
  let s = G.of_abslayout r in
  G.to_ralgebra s#graph |> Format.printf "%a" Abslayout.pp;
  [%expect {|
    join((c_nationkey = n_nationkey), nation, customer) |}]

let%expect_test "to-from-ralgebra" =
  let r =
    C.(
      join
        (Binop (Eq, Name c_custkey, Name o_custkey))
        (relation orders)
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (relation nation) (relation customer)))
  in
  let s = G.of_abslayout r in
  G.to_ralgebra s#graph |> Format.printf "%a" Abslayout.pp;
  [%expect
    {|
    join((c_custkey = o_custkey),
      orders,
      join((c_nationkey = n_nationkey), nation, customer)) |}]

let%expect_test "part-fold" =
  let r =
    C.(
      join
        (Binop (Eq, Name c_custkey, Name o_custkey))
        (relation orders)
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (relation nation) (relation customer)))
  in
  let s = G.of_abslayout r in
  G.partition_fold s#graph ~init:() ~f:(fun () (s1, s2, _) ->
      Format.printf "%a@.%a@.---\n" Abslayout.pp (G.to_ralgebra s1) Abslayout.pp
        (G.to_ralgebra s2));
  [%expect
    {|
    join((c_nationkey = n_nationkey), nation, customer)
    orders
    ---
    join((c_custkey = o_custkey), orders, customer)
    nation
    ---
    nation
    join((c_custkey = o_custkey), orders, customer)
    ---
    orders
    join((c_nationkey = n_nationkey), nation, customer)
    --- |}]

let opt_test r = (opt r)#joins |> [%sexp_of: (float array * t) list] |> print_s

let%expect_test "join-opt" =
  opt_test
  @@ C.join
       (Binop (Eq, Name c_nationkey, Name n_nationkey))
       (C.relation nation) (C.relation customer);
  [%expect
    {|
    (((32233)
      (Hash (lkey (Name ((name n_nationkey) (meta <opaque>))))
       (lhs
        (Flat
         ((node
           (Relation
            ((r_name nation)
             (r_schema
              (((((name n_nationkey) (meta <opaque>)) (IntT))
                (((name n_name) (meta <opaque>)) (StringT (padded)))
                (((name n_regionkey) (meta <opaque>)) (IntT))
                (((name n_comment) (meta <opaque>)) (StringT))))))))
          (meta <opaque>))))
       (rkey (Name ((name c_nationkey) (meta <opaque>))))
       (rhs
        (Flat
         ((node
           (Relation
            ((r_name customer)
             (r_schema
              (((((name c_custkey) (meta <opaque>)) (IntT))
                (((name c_name) (meta <opaque>)) (StringT))
                (((name c_address) (meta <opaque>)) (StringT))
                (((name c_nationkey) (meta <opaque>)) (IntT))
                (((name c_phone) (meta <opaque>)) (StringT (padded)))
                (((name c_acctbal) (meta <opaque>)) (FixedT))
                (((name c_mktsegment) (meta <opaque>)) (StringT (padded)))
                (((name c_comment) (meta <opaque>)) (StringT))))))))
          (meta <opaque>))))))) |}]

let%expect_test "join-opt" =
  opt_test
  @@ C.join (Binop (Eq, Name c_custkey, Name o_custkey)) (C.relation orders)
  @@ C.join (Binop (Eq, Name c_nationkey, Name n_nationkey)) (C.relation nation)
  @@ C.relation customer;
  [%expect
    {|
    (((68425)
      (Hash (lkey (Name ((name n_nationkey) (meta <opaque>))))
       (lhs
        (Flat
         ((node
           (Relation
            ((r_name nation)
             (r_schema
              (((((name n_nationkey) (meta <opaque>)) (IntT))
                (((name n_name) (meta <opaque>)) (StringT (padded)))
                (((name n_regionkey) (meta <opaque>)) (IntT))
                (((name n_comment) (meta <opaque>)) (StringT))))))))
          (meta <opaque>))))
       (rkey (Name ((name c_nationkey) (meta <opaque>))))
       (rhs
        (Flat
         ((node
           (Join
            ((pred
              (Binop Eq (Name ((name c_custkey) (meta <opaque>)))
               (Name ((name o_custkey) (meta <opaque>)))))
             (r1
              ((node
                (Relation
                 ((r_name customer)
                  (r_schema
                   (((((name c_custkey) (meta <opaque>)) (IntT))
                     (((name c_name) (meta <opaque>)) (StringT))
                     (((name c_address) (meta <opaque>)) (StringT))
                     (((name c_nationkey) (meta <opaque>)) (IntT))
                     (((name c_phone) (meta <opaque>)) (StringT (padded)))
                     (((name c_acctbal) (meta <opaque>)) (FixedT))
                     (((name c_mktsegment) (meta <opaque>)) (StringT (padded)))
                     (((name c_comment) (meta <opaque>)) (StringT))))))))
               (meta <opaque>)))
             (r2
              ((node
                (Relation
                 ((r_name orders)
                  (r_schema
                   (((((name o_orderkey) (meta <opaque>)) (IntT))
                     (((name o_custkey) (meta <opaque>)) (IntT))
                     (((name o_orderstatus) (meta <opaque>)) (StringT (padded)))
                     (((name o_totalprice) (meta <opaque>)) (FixedT))
                     (((name o_orderdate) (meta <opaque>)) (DateT))
                     (((name o_orderpriority) (meta <opaque>))
                      (StringT (padded)))
                     (((name o_clerk) (meta <opaque>)) (StringT (padded)))
                     (((name o_shippriority) (meta <opaque>)) (IntT))
                     (((name o_comment) (meta <opaque>)) (StringT))))))))
               (meta <opaque>))))))
          (meta <opaque>))))))) |}]

let%expect_test "" =
  let r =
    Abslayout_load.load_string_exn
      ~params:
        (Set.of_list
           (module Name)
           [
             Name.create ~scope:"k0" ~type_:Prim_type.string_t "n1_name";
             Name.create ~scope:"k0" ~type_:Prim_type.string_t "n2_name";
             Name.create ~scope:"k0" ~type_:Prim_type.date_t "l_year";
           ])
      (Lazy.force Test_util.tpch_conn)
      {|
join(((n1_name = k0.n1_name) &&
     ((n2_name = k0.n2_name) && ((to_year(l_shipdate) = k0.l_year) && (true && (s_suppkey = l_suppkey))))),
  join((o_orderkey = l_orderkey),
    join((c_custkey = o_custkey),
      join((c_nationkey = n2_nationkey), select([n_name as n2_name, n_nationkey as n2_nationkey], nation), customer),
      orders),
    filter(((l_shipdate >= date("1995-01-01")) && (l_shipdate <= date("1996-12-31"))), lineitem)),
  join((s_nationkey = n1_nationkey), select([n_name as n1_name, n_nationkey as n1_nationkey], nation), supplier))
|}
  in
  apply transform Path.root r |> Option.iter ~f:(Fmt.pr "%a" A.pp);
  [%expect
    {|
    filter((true &&
           ((to_year(l_shipdate) = k0.l_year) &&
           ((n2_name = k0.n2_name) && (n1_name = k0.n1_name)))),
      depjoin(alist(select([n_name as n1_name, n_nationkey as n1_nationkey],
                      nation) as s0,
                atuple([ascalar(s0.n1_name), ascalar(s0.n1_nationkey)], cross)) as s2,
        select([s2.n1_name, s2.n1_nationkey, n2_name, n2_nationkey, l_orderkey,
                l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice,
                l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate,
                l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode,
                l_comment, o_orderkey, o_custkey, o_orderstatus, o_totalprice,
                o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment,
                c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
                c_mktsegment, c_comment, s_suppkey, s_name, s_address,
                s_nationkey, s_phone, s_acctbal, s_comment],
          ahashidx(dedup(
                     select([s_nationkey],
                       alist(join((s_suppkey = l_suppkey),
                               join((n2_nationkey = c_nationkey),
                                 select([n_name as n2_name,
                                         n_nationkey as n2_nationkey],
                                   nation),
                                 join((c_custkey = o_custkey),
                                   join((o_orderkey = l_orderkey),
                                     filter(((l_shipdate >= date("1995-01-01"))
                                            &&
                                            (l_shipdate <= date("1996-12-31"))),
                                       lineitem),
                                     orders),
                                   customer)),
                               supplier) as s4,
                         atuple([ascalar(s4.n2_name), ascalar(s4.n2_nationkey),
                                 ascalar(s4.l_orderkey), ascalar(s4.l_partkey),
                                 ascalar(s4.l_suppkey), ascalar(s4.l_linenumber),
                                 ascalar(s4.l_quantity),
                                 ascalar(s4.l_extendedprice),
                                 ascalar(s4.l_discount), ascalar(s4.l_tax),
                                 ascalar(s4.l_returnflag),
                                 ascalar(s4.l_linestatus),
                                 ascalar(s4.l_shipdate),
                                 ascalar(s4.l_commitdate),
                                 ascalar(s4.l_receiptdate),
                                 ascalar(s4.l_shipinstruct),
                                 ascalar(s4.l_shipmode), ascalar(s4.l_comment),
                                 ascalar(s4.o_orderkey), ascalar(s4.o_custkey),
                                 ascalar(s4.o_orderstatus),
                                 ascalar(s4.o_totalprice),
                                 ascalar(s4.o_orderdate),
                                 ascalar(s4.o_orderpriority),
                                 ascalar(s4.o_clerk), ascalar(s4.o_shippriority),
                                 ascalar(s4.o_comment), ascalar(s4.c_custkey),
                                 ascalar(s4.c_name), ascalar(s4.c_address),
                                 ascalar(s4.c_nationkey), ascalar(s4.c_phone),
                                 ascalar(s4.c_acctbal), ascalar(s4.c_mktsegment),
                                 ascalar(s4.c_comment), ascalar(s4.s_suppkey),
                                 ascalar(s4.s_name), ascalar(s4.s_address),
                                 ascalar(s4.s_nationkey), ascalar(s4.s_phone),
                                 ascalar(s4.s_acctbal), ascalar(s4.s_comment)],
                           cross)))) as s3,
            filter((s3.s_nationkey = s_nationkey),
              alist(join((s_suppkey = l_suppkey),
                      join((n2_nationkey = c_nationkey),
                        select([n_name as n2_name, n_nationkey as n2_nationkey],
                          nation),
                        join((c_custkey = o_custkey),
                          join((o_orderkey = l_orderkey),
                            filter(((l_shipdate >= date("1995-01-01")) &&
                                   (l_shipdate <= date("1996-12-31"))),
                              lineitem),
                            orders),
                          customer)),
                      supplier) as s1,
                atuple([ascalar(s1.n2_name), ascalar(s1.n2_nationkey),
                        ascalar(s1.l_orderkey), ascalar(s1.l_partkey),
                        ascalar(s1.l_suppkey), ascalar(s1.l_linenumber),
                        ascalar(s1.l_quantity), ascalar(s1.l_extendedprice),
                        ascalar(s1.l_discount), ascalar(s1.l_tax),
                        ascalar(s1.l_returnflag), ascalar(s1.l_linestatus),
                        ascalar(s1.l_shipdate), ascalar(s1.l_commitdate),
                        ascalar(s1.l_receiptdate), ascalar(s1.l_shipinstruct),
                        ascalar(s1.l_shipmode), ascalar(s1.l_comment),
                        ascalar(s1.o_orderkey), ascalar(s1.o_custkey),
                        ascalar(s1.o_orderstatus), ascalar(s1.o_totalprice),
                        ascalar(s1.o_orderdate), ascalar(s1.o_orderpriority),
                        ascalar(s1.o_clerk), ascalar(s1.o_shippriority),
                        ascalar(s1.o_comment), ascalar(s1.c_custkey),
                        ascalar(s1.c_name), ascalar(s1.c_address),
                        ascalar(s1.c_nationkey), ascalar(s1.c_phone),
                        ascalar(s1.c_acctbal), ascalar(s1.c_mktsegment),
                        ascalar(s1.c_comment), ascalar(s1.s_suppkey),
                        ascalar(s1.s_name), ascalar(s1.s_address),
                        ascalar(s1.s_nationkey), ascalar(s1.s_phone),
                        ascalar(s1.s_acctbal), ascalar(s1.s_comment)],
                  cross))),
            s2.n1_nationkey)))) |}]

(* let%expect_test "" =
 *   Logs.Src.set_level Join_opt.src (Some Info);
 *   let r =
 *     Abslayout_load.load_string_exn
 *       (Lazy.force Test_util.tpch_conn)
 *       {|
 * join((((p_partkey = l_partkey))),
 *   join((s_suppkey = l_suppkey),
 *     join((l_orderkey = o_orderkey),
 *       join((o_custkey = c_custkey),
 *         join((c_nationkey = n1_nationkey),
 *           join((n1_regionkey = r_regionkey),
 *             select([n_regionkey as n1_regionkey, n_nationkey as n1_nationkey], nation),
 *             filter(true, region)),
 *           customer),
 *         filter(((o_orderdate >= date("1995-01-01")) && (o_orderdate <= date("1996-12-31"))), orders)),
 *       lineitem),
 *     join((s_nationkey = n2_nationkey), select([n_nationkey as n2_nationkey, n_name as n2_name], nation), supplier)),
 *   part)
 * |}
 *   in
 *   apply transform Path.root r |> Option.iter ~f:(Fmt.pr "%a" A.pp) *)

let%expect_test "" =
  Logs.Src.set_level Join_opt.src (Some Info);
  let r =
    Abslayout_load.load_string_exn
      (Lazy.force Test_util.tpch_conn)
      {|
        join((c_nationkey = n1_nationkey),
          join((n1_regionkey = r_regionkey),
            select([n_regionkey as n1_regionkey, n_nationkey as n1_nationkey], nation),
            filter(true, region)),
          customer)
|}
  in
  apply transform Path.root r |> Option.iter ~f:(Fmt.pr "%a" A.pp)
