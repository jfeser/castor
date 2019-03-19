open Core
open Castor
open Join_opt

let ctx =
  { conn= new Postgresql.connection ~conninfo:"postgresql:///tpch_1k" ()
  ; dbconn= Db.create "postgresql:///tpch_1k"
  ; sql= Sql.create_ctx () }

let%expect_test "parted-cost" =
  estimate_cost_parted ctx [] (Flat (A.scan "orders"))
  |> [%sexp_of: int * int * float] |> print_s ;
  [%expect {| (1000 1000 1000) |}]

let type_ = Type.PrimType.IntT {nullable= false}

let c_custkey = Name.create ~type_ ~relation:"customer" "c_custkey"

let c_nationkey = Name.create ~type_ ~relation:"customer" "c_nationkey"

let n_nationkey = Name.create ~type_ ~relation:"nation" "n_nationkey"

let o_custkey = Name.create ~type_ ~relation:"orders" "o_custkey"

let%expect_test "parted-cost" =
  estimate_cost_parted ctx
    [ ( A.scan "customer"
      , Set.singleton (module Name) c_custkey
      , Set.singleton (module Name) o_custkey
      , Abslayout.(Binop (Eq, Name c_custkey, Name o_custkey)) ) ]
    (Flat (A.scan "orders"))
  |> [%sexp_of: int * int * float] |> print_s ;
  [%expect {| (1 2 1.002004008016032) |}]

let%expect_test "cost" =
  estimate_cost ctx []
    (Flat
       A.(
         join
           (Binop (Eq, Name c_custkey, Name o_custkey))
           (scan "orders") (scan "customer")))
  |> [%sexp_of: float array] |> print_s ;
  [%expect {| (1 2 1.002004008016032) |}]

let%expect_test "cost" =
  estimate_cost ctx []
    A.(
      Nest
        { pred= Binop (Eq, Name c_custkey, Name o_custkey)
        ; lhs= Flat (scan "customer")
        ; rhs= Flat (scan "orders") })
  |> [%sexp_of: float array] |> print_s ;
  [%expect {| (1 2 1.002004008016032) |}]

let%expect_test "cost" =
  estimate_cost ctx []
    A.(
      Flat
        (join
           (Binop (Eq, Name c_nationkey, Name n_nationkey))
           (scan "nation") (scan "customer")))
  |> [%sexp_of: float array] |> print_s ;
  estimate_cost ctx []
    A.(
      Nest
        { pred= Binop (Eq, Name c_nationkey, Name n_nationkey)
        ; lhs= Flat (scan "nation")
        ; rhs= Flat (scan "customer") })
  |> [%sexp_of: float array] |> print_s ;
  estimate_cost ctx []
    A.(
      Hash
        { lkey= Name c_nationkey
        ; rkey= Name n_nationkey
        ; lhs= Flat (scan "nation")
        ; rhs= Flat (scan "customer") })
  |> [%sexp_of: float array] |> print_s ;
  [%expect {| (1 2 1.002004008016032) |}]
