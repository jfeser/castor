open Core
open Castor
module A = Abslayout

module Config = struct
  let dbconn = new Postgresql.connection ~conninfo:"postgresql:///tpch_1k" ()

  let conn = Db.create "postgresql:///tpch_1k"

  let validate = false

  let param_ctx = Map.empty (module Name)
end

module Join_opt = Join_opt.Make (Config)
open Join_opt
module M = Abslayout_db.Make (Config)

let%expect_test "parted-cost" =
  estimate_ntuples_parted [] (Flat (A.scan "orders"))
  |> [%sexp_of: int * int * float] |> print_s ;
  [%expect {| (1000 1000 1000) |}]

let type_ = Type.PrimType.IntT {nullable= false}

let c_custkey = Name.create ~type_ ~relation:"customer" "c_custkey"

let c_nationkey = Name.create ~type_ ~relation:"customer" "c_nationkey"

let n_nationkey = Name.create ~type_ ~relation:"nation" "n_nationkey"

let o_custkey = Name.create ~type_ ~relation:"orders" "o_custkey"

let%expect_test "parted-cost" =
  estimate_ntuples_parted
    [ ( A.scan "customer"
      , Set.singleton (module Name) c_custkey
      , Set.singleton (module Name) o_custkey
      , Abslayout.(Binop (Eq, Name c_custkey, Name o_custkey)) ) ]
    (Flat (A.scan "orders"))
  |> [%sexp_of: int * int * float] |> print_s ;
  [%expect {| (1 2 1.002004008016032) |}]

let%expect_test "parted-cost" =
  estimate_ntuples_parted
    [ ( A.scan "customer"
      , Set.singleton (module Name) c_custkey
      , Set.singleton (module Name) c_custkey
      , Abslayout.(Binop (Eq, Name c_custkey, Name c_custkey)) ) ]
    (Flat (A.scan "customer"))
  |> [%sexp_of: int * int * float] |> print_s ;
  [%expect {| (1 1 1) |}]

let%expect_test "cost" =
  estimate_cost []
    (Flat
       A.(
         join
           (Binop (Eq, Name c_custkey, Name o_custkey))
           (scan "orders") (scan "customer")))
  |> [%sexp_of: float array] |> print_s ;
  [%expect {| (257016 68000) |}]

let%expect_test "cost" =
  estimate_cost []
    A.(
      Nest
        { pred= Binop (Eq, Name c_custkey, Name o_custkey)
        ; lhs= Flat (scan "customer")
        ; rhs= Flat (scan "orders") })
  |> [%sexp_of: float array] |> print_s ;
  [%expect {| (272710 67936) |}]

let%expect_test "cost" =
  estimate_cost []
    A.(
      Flat
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (scan "nation") (scan "customer")))
  |> [%sexp_of: float array] |> print_s ;
  estimate_cost []
    A.(
      Nest
        { pred= Binop (Eq, Name c_nationkey, Name n_nationkey)
        ; lhs= Flat (scan "nation")
        ; rhs= Flat (scan "customer") })
  |> [%sexp_of: float array] |> print_s ;
  estimate_cost []
    A.(
      Hash
        { lkey= Name c_nationkey
        ; rkey= Name n_nationkey
        ; lhs= Flat (scan "nation")
        ; rhs= Flat (scan "customer") })
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
        (scan "nation") (scan "customer"))
  in
  M.annotate_schema r ;
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
        (scan "orders")
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (scan "nation") (scan "customer")))
  in
  M.annotate_schema r ;
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
        (scan "orders")
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (scan "nation") (scan "customer")))
  in
  M.annotate_schema r ;
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
        (scan "nation") (scan "customer"))
  |> [%sexp_of: (float array * t) list] |> print_s

let%expect_test "join-opt" =
  opt
    A.(
      join
        (Binop (Eq, Name c_custkey, Name o_custkey))
        (scan "orders")
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (scan "nation") (scan "customer")))
  |> [%sexp_of: (float array * t) list] |> print_s
