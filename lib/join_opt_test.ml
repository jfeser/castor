open Core
open Castor
open Collections
module A = Abslayout

module Config = struct
  let dbconn = new Postgresql.connection ~conninfo:"postgresql:///tpch_1k" ()

  let conn = Db.create "postgresql:///tpch_1k"

  let validate = false

  let param_ctx = Map.empty (module Name)

  let params = Set.empty (module Name)

  let fresh = Fresh.create ()

  let verbose = false
end

module Join_opt = Join_opt.Make (Config)
open Join_opt
module M = Abslayout_db.Make (Config)

let type_ = Type.PrimType.IntT {nullable= false}

let c_custkey = Name.create ~type_ ~relation:"customer" "c_custkey"

let c_nationkey = Name.create ~type_ ~relation:"customer" "c_nationkey"

let n_nationkey = Name.create ~type_ ~relation:"nation" "n_nationkey"

let o_custkey = Name.create ~type_ ~relation:"orders" "o_custkey"

let orders = Db.relation Config.conn "orders"

let customer = Db.relation Config.conn "customer"

let nation = Db.relation Config.conn "nation"

let%expect_test "parted-cost" =
  estimate_ntuples_parted (Set.empty (module Name)) (Flat (A.relation orders))
  |> [%sexp_of: int * int * float] |> print_s ;
  [%expect {| (1000 1000 1000) |}]

let%expect_test "parted-cost" =
  estimate_ntuples_parted
    (Set.singleton (module Name) o_custkey)
    (Flat (A.relation orders))
  |> [%sexp_of: int * int * float] |> print_s ;
  [%expect {| (1 2 1.002004008016032) |}]

let%expect_test "parted-cost" =
  estimate_ntuples_parted
    (Set.singleton (module Name) c_custkey)
    (Flat (A.relation customer))
  |> [%sexp_of: int * int * float] |> print_s ;
  [%expect {| (1 1 1) |}]

let%expect_test "cost" =
  estimate_cost
    (Set.empty (module Name))
    (Flat
       A.(
         join
           (Binop (Eq, Name c_custkey, Name o_custkey))
           (relation orders) (relation customer)))
  |> [%sexp_of: float array] |> print_s ;
  [%expect {| (257016 68000) |}]

let%expect_test "cost" =
  estimate_cost
    (Set.empty (module Name))
    A.(
      Nest
        { pred= Binop (Eq, Name c_custkey, Name o_custkey)
        ; lhs= Flat (relation customer)
        ; rhs= Flat (relation orders) })
  |> [%sexp_of: float array] |> print_s ;
  [%expect {| (272710 67936) |}]

let%expect_test "cost" =
  estimate_cost
    (Set.empty (module Name))
    A.(
      Flat
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (relation nation) (relation customer)))
  |> [%sexp_of: float array] |> print_s ;
  estimate_cost
    (Set.empty (module Name))
    A.(
      Nest
        { pred= Binop (Eq, Name c_nationkey, Name n_nationkey)
        ; lhs= Flat (relation nation)
        ; rhs= Flat (relation customer) })
  |> [%sexp_of: float array] |> print_s ;
  estimate_cost
    (Set.empty (module Name))
    A.(
      Hash
        { lkey= Name c_nationkey
        ; rkey= Name n_nationkey
        ; lhs= Flat (relation nation)
        ; rhs= Flat (relation customer) })
  |> [%sexp_of: float array] |> print_s ;
  [%expect {|
    (194626 47904)
    (138592 32336)
    (2932 32361) |}]

let%expect_test "to-from-ralgebra" =
  let r =
    A.(
      join
        (Binop (Eq, Name c_nationkey, Name n_nationkey))
        (relation nation) (relation customer))
  in
  JoinSpace.of_abslayout r |> JoinSpace.to_ralgebra
  |> Format.printf "%a" Abslayout.pp ;
  [%expect
    {|
    join((customer.c_nationkey = nation.n_nationkey), nation, customer) |}]

let%expect_test "to-from-ralgebra" =
  let r =
    A.(
      join
        (Binop (Eq, Name c_custkey, Name o_custkey))
        (relation orders)
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (relation nation) (relation customer)))
  in
  JoinSpace.of_abslayout r |> JoinSpace.to_ralgebra
  |> Format.printf "%a" Abslayout.pp ;
  [%expect
    {|
    join((customer.c_custkey = orders.o_custkey),
      orders,
      join((customer.c_nationkey = nation.n_nationkey), nation, customer)) |}]

let%expect_test "part-fold" =
  let r =
    A.(
      join
        (Binop (Eq, Name c_custkey, Name o_custkey))
        (relation orders)
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (relation nation) (relation customer)))
  in
  let open JoinSpace in
  of_abslayout r
  |> partition_fold ~init:() ~f:(fun () (s1, s2, _) ->
         Format.printf "%a@.%a@.---\n" Abslayout.pp (to_ralgebra s1)
           Abslayout.pp (to_ralgebra s2) ) ;
  [%expect
    {|
    join((customer.c_nationkey = nation.n_nationkey), nation, customer)
    orders
    ---
    join((customer.c_custkey = orders.o_custkey), orders, customer)
    nation
    ---
    nation
    join((customer.c_custkey = orders.o_custkey), orders, customer)
    ---
    orders
    join((customer.c_nationkey = nation.n_nationkey), nation, customer)
    --- |}]

let%expect_test "join-opt" =
  opt
    A.(
      join
        (Binop (Eq, Name c_nationkey, Name n_nationkey))
        (relation nation) (relation customer))
  |> [%sexp_of: (float array * t) list] |> print_s ;
  [%expect
    {|
    (((138592 32336)
      (Nest
       (lhs
        (Flat
         ((node
           (Relation
            ((r_name nation)
             (r_schema
              (((relation (nation)) (name n_nationkey))
               ((relation (nation)) (name n_name))
               ((relation (nation)) (name n_regionkey))
               ((relation (nation)) (name n_comment)))))))
          (meta ()))))
       (rhs
        (Flat
         ((node
           (Relation
            ((r_name customer)
             (r_schema
              (((relation (customer)) (name c_custkey))
               ((relation (customer)) (name c_name))
               ((relation (customer)) (name c_address))
               ((relation (customer)) (name c_nationkey))
               ((relation (customer)) (name c_phone))
               ((relation (customer)) (name c_acctbal))
               ((relation (customer)) (name c_mktsegment))
               ((relation (customer)) (name c_comment)))))))
          (meta ()))))
       (pred
        (Binop
         (Eq (Name ((relation (customer)) (name c_nationkey)))
          (Name ((relation (nation)) (name n_nationkey))))))))) |}]

let%expect_test "join-opt" =
  opt
    A.(
      join
        (Binop (Eq, Name c_custkey, Name o_custkey))
        (relation orders)
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (relation nation) (relation customer)))
  |> [%sexp_of: (float array * t) list] |> print_s ;
  [%expect
    {|
    (((258866 68400)
      (Nest
       (lhs
        (Flat
         ((node
           (Relation
            ((r_name nation)
             (r_schema
              (((relation (nation)) (name n_nationkey))
               ((relation (nation)) (name n_name))
               ((relation (nation)) (name n_regionkey))
               ((relation (nation)) (name n_comment)))))))
          (meta ()))))
       (rhs
        (Flat
         ((node
           (Join
            (pred
             (Binop
              (Eq (Name ((relation (customer)) (name c_custkey)))
               (Name ((relation (orders)) (name o_custkey))))))
            (r1
             ((node
               (Relation
                ((r_name customer)
                 (r_schema
                  (((relation (customer)) (name c_custkey))
                   ((relation (customer)) (name c_name))
                   ((relation (customer)) (name c_address))
                   ((relation (customer)) (name c_nationkey))
                   ((relation (customer)) (name c_phone))
                   ((relation (customer)) (name c_acctbal))
                   ((relation (customer)) (name c_mktsegment))
                   ((relation (customer)) (name c_comment)))))))
              (meta ())))
            (r2
             ((node
               (Relation
                ((r_name orders)
                 (r_schema
                  (((relation (orders)) (name o_orderkey))
                   ((relation (orders)) (name o_custkey))
                   ((relation (orders)) (name o_orderstatus))
                   ((relation (orders)) (name o_totalprice))
                   ((relation (orders)) (name o_orderdate))
                   ((relation (orders)) (name o_orderpriority))
                   ((relation (orders)) (name o_clerk))
                   ((relation (orders)) (name o_shippriority))
                   ((relation (orders)) (name o_comment)))))))
              (meta ())))))
          (meta ()))))
       (pred
        (Binop
         (Eq (Name ((relation (customer)) (name c_nationkey)))
          (Name ((relation (nation)) (name n_nationkey))))))))
     ((274559.99999999994 68335.999999999985)
      (Nest
       (lhs
        (Flat
         ((node
           (Relation
            ((r_name nation)
             (r_schema
              (((relation (nation)) (name n_nationkey))
               ((relation (nation)) (name n_name))
               ((relation (nation)) (name n_regionkey))
               ((relation (nation)) (name n_comment)))))))
          (meta ()))))
       (rhs
        (Nest
         (lhs
          (Flat
           ((node
             (Relation
              ((r_name customer)
               (r_schema
                (((relation (customer)) (name c_custkey))
                 ((relation (customer)) (name c_name))
                 ((relation (customer)) (name c_address))
                 ((relation (customer)) (name c_nationkey))
                 ((relation (customer)) (name c_phone))
                 ((relation (customer)) (name c_acctbal))
                 ((relation (customer)) (name c_mktsegment))
                 ((relation (customer)) (name c_comment)))))))
            (meta ()))))
         (rhs
          (Flat
           ((node
             (Relation
              ((r_name orders)
               (r_schema
                (((relation (orders)) (name o_orderkey))
                 ((relation (orders)) (name o_custkey))
                 ((relation (orders)) (name o_orderstatus))
                 ((relation (orders)) (name o_totalprice))
                 ((relation (orders)) (name o_orderdate))
                 ((relation (orders)) (name o_orderpriority))
                 ((relation (orders)) (name o_clerk))
                 ((relation (orders)) (name o_shippriority))
                 ((relation (orders)) (name o_comment)))))))
            (meta ()))))
         (pred
          (Binop
           (Eq (Name ((relation (customer)) (name c_custkey)))
            (Name ((relation (orders)) (name o_custkey))))))))
       (pred
        (Binop
         (Eq (Name ((relation (customer)) (name c_nationkey)))
          (Name ((relation (nation)) (name n_nationkey))))))))) |}]
