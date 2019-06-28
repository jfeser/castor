open Core
open Castor
open Collections
module A = Abslayout
open Test_util

module Config = struct
  let cost_conn = Db.create "postgresql:///tpch_1k"

  let conn = cost_conn

  let validate = false

  let param_ctx = Map.empty (module Name)

  let params = Set.empty (module Name)

  let verbose = false

  let simplify = None
end

module Join_opt = Join_opt.Make (Config)
open Join_opt
module M = Abslayout_db.Make (Config)

let type_ = Type.PrimType.IntT {nullable= false}

let c_custkey = Name.create ~type_ "c_custkey"

let c_nationkey = Name.create ~type_ "c_nationkey"

let n_nationkey = Name.create ~type_ "n_nationkey"

let o_custkey = Name.create ~type_ "o_custkey"

let orders = Db.relation Config.cost_conn "orders"

let customer = Db.relation Config.cost_conn "customer"

let nation = Db.relation Config.cost_conn "nation"

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

let estimate_cost p r = [|size_cost p r; scan_cost p r|]

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
    (138208 32361) |}]

let%expect_test "to-from-ralgebra" =
  let r =
    A.(
      join
        (Binop (Eq, Name c_nationkey, Name n_nationkey))
        (relation nation) (relation customer))
  in
  JoinSpace.of_abslayout r |> JoinSpace.to_ralgebra
  |> Format.printf "%a" Abslayout.pp ;
  [%expect {|
    join((c_nationkey = n_nationkey), nation, customer) |}]

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
    join((c_custkey = o_custkey),
      orders,
      join((c_nationkey = n_nationkey), nation, customer)) |}]

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
           Abslayout.pp (to_ralgebra s2)) ;
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

let%expect_test "join-opt" =
  opt
    A.(
      join
        (Binop (Eq, Name c_nationkey, Name n_nationkey))
        (relation nation) (relation customer))
  |> [%sexp_of: (float array * t) list] |> print_s ;
  [%expect
    {|
    (((47904)
      (Flat
       ((node
         (Join
          ((pred
            (Binop
             (Eq (Name ((scope ()) (name c_nationkey)))
              (Name ((scope ()) (name n_nationkey))))))
           (r1
            ((node
              (Relation
               ((r_name customer)
                (r_schema
                 ((((scope ()) (name c_custkey)) ((scope ()) (name c_name))
                   ((scope ()) (name c_address)) ((scope ()) (name c_nationkey))
                   ((scope ()) (name c_phone)) ((scope ()) (name c_acctbal))
                   ((scope ()) (name c_mktsegment))
                   ((scope ()) (name c_comment))))))))
             (meta ())))
           (r2
            ((node
              (Relation
               ((r_name nation)
                (r_schema
                 ((((scope ()) (name n_nationkey)) ((scope ()) (name n_name))
                   ((scope ()) (name n_regionkey)) ((scope ()) (name n_comment))))))))
             (meta ()))))))
        (meta ()))))) |}]

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
    (((84000)
      (Flat
       ((node
         (Join
          ((pred
            (Binop
             (Eq (Name ((scope ()) (name c_custkey)))
              (Name ((scope ()) (name o_custkey))))))
           (r1
            ((node
              (Join
               ((pred
                 (Binop
                  (Eq (Name ((scope ()) (name c_nationkey)))
                   (Name ((scope ()) (name n_nationkey))))))
                (r1
                 ((node
                   (Relation
                    ((r_name customer)
                     (r_schema
                      ((((scope ()) (name c_custkey)) ((scope ()) (name c_name))
                        ((scope ()) (name c_address))
                        ((scope ()) (name c_nationkey))
                        ((scope ()) (name c_phone)) ((scope ()) (name c_acctbal))
                        ((scope ()) (name c_mktsegment))
                        ((scope ()) (name c_comment))))))))
                  (meta ())))
                (r2
                 ((node
                   (Relation
                    ((r_name nation)
                     (r_schema
                      ((((scope ()) (name n_nationkey))
                        ((scope ()) (name n_name))
                        ((scope ()) (name n_regionkey))
                        ((scope ()) (name n_comment))))))))
                  (meta ()))))))
             (meta ())))
           (r2
            ((node
              (Relation
               ((r_name orders)
                (r_schema
                 ((((scope ()) (name o_orderkey)) ((scope ()) (name o_custkey))
                   ((scope ()) (name o_orderstatus))
                   ((scope ()) (name o_totalprice))
                   ((scope ()) (name o_orderdate))
                   ((scope ()) (name o_orderpriority))
                   ((scope ()) (name o_clerk)) ((scope ()) (name o_shippriority))
                   ((scope ()) (name o_comment))))))))
             (meta ()))))))
        (meta ()))))) |}]
