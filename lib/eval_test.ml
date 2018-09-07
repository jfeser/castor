open Core
open Base
open Collections
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

module Eval = Eval.Make_mock (struct
  let rels = rels
end)

let _ =
  Test_util.create rels "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

let _ =
  Test_util.create rels "log" ["id"; "succ"; "counter"]
    [[1; 4; 1]; [2; 3; 2]; [3; 4; 3]; [4; 6; 1]; [5; 6; 3]]

let%expect_test "eval-select" =
  let query = "select([log.id as k], log)" |> of_string_exn in
  Eval.eval Ctx.empty query |> [%sexp_of : Ctx.t Seq.t] |> print_s;
  [%expect {|
    (((((relation ()) (name k) (type_ ())) (Int 1)))
     ((((relation ()) (name k) (type_ ())) (Int 2)))
     ((((relation ()) (name k) (type_ ())) (Int 3)))
     ((((relation ()) (name k) (type_ ())) (Int 4)))
     ((((relation ()) (name k) (type_ ())) (Int 5)))) |}]

let%expect_test "eval-select" =
  let query =
    "join(k = log.id && log.counter < log.succ, select([log.id as k], log), log)"
    |> of_string_exn
  in
  Eval.eval Ctx.empty query |> [%sexp_of : Ctx.t Seq.t] |> print_s;
  [%expect {|
    (((((relation ()) (name k) (type_ ())) (Int 1))
      (((relation (log)) (name counter) (type_ ())) (Int 1))
      (((relation (log)) (name id) (type_ ())) (Int 1))
      (((relation (log)) (name succ) (type_ ())) (Int 4)))
     ((((relation ()) (name k) (type_ ())) (Int 2))
      (((relation (log)) (name counter) (type_ ())) (Int 2))
      (((relation (log)) (name id) (type_ ())) (Int 2))
      (((relation (log)) (name succ) (type_ ())) (Int 3)))
     ((((relation ()) (name k) (type_ ())) (Int 3))
      (((relation (log)) (name counter) (type_ ())) (Int 3))
      (((relation (log)) (name id) (type_ ())) (Int 3))
      (((relation (log)) (name succ) (type_ ())) (Int 4)))
     ((((relation ()) (name k) (type_ ())) (Int 4))
      (((relation (log)) (name counter) (type_ ())) (Int 1))
      (((relation (log)) (name id) (type_ ())) (Int 4))
      (((relation (log)) (name succ) (type_ ())) (Int 6)))
     ((((relation ()) (name k) (type_ ())) (Int 5))
      (((relation (log)) (name counter) (type_ ())) (Int 3))
      (((relation (log)) (name id) (type_ ())) (Int 5))
      (((relation (log)) (name succ) (type_ ())) (Int 6)))) |}]
