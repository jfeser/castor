open Core
open Base
open Collections
open Abslayout
open Test_util

let rels = Hashtbl.create (module Db.Relation)

let _ = create rels "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

let _ =
  create rels "log" ["id"; "succ"; "counter"]
    [[1; 4; 1]; [2; 3; 2]; [3; 4; 3]; [4; 6; 1]; [5; 6; 3]]

module E = Eval.Make_mock (struct
  let rels = rels
end)

let%expect_test "eval-select" =
  let query = "select([log.id as k], log)" |> of_string_exn in
  E.eval Ctx.empty query |> [%sexp_of: Ctx.t Seq.t] |> print_s ;
  [%expect
    {|
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
  E.eval Ctx.empty query |> [%sexp_of: Ctx.t Seq.t] |> print_s ;
  [%expect
    {|
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

let%expect_test "eval-foreach" =
  let module M = Abslayout_db.Make (E) in
  let q1 = "select([id as i1, counter as c1], log)" |> of_string_exn |> M.resolve in
  M.annotate_schema q1 ;
  let q2 =
    "select([log.id as i2, log.counter as c2], filter(log.succ = i1, log))"
    |> of_string_exn
  in
  M.annotate_schema q2 ;
  E.eval_foreach Ctx.empty q1 q2
  |> Seq.map ~f:(fun (c, s) -> (c, Seq.to_list s))
  |> Seq.to_list |> [%sexp_of: (Ctx.t * Ctx.t list) list] |> print_s ;
  [%expect
    {|
    ((((((relation ()) (name c1) (type_ ((IntT (nullable false))))) (Int 3))
       (((relation ()) (name i1) (type_ ((IntT (nullable false))))) (Int 3)))
      (((((relation ()) (name c1) (type_ ((IntT (nullable false))))) (Int 3))
        (((relation ()) (name c2) (type_ ())) (Int 2))
        (((relation ()) (name i1) (type_ ((IntT (nullable false))))) (Int 3))
        (((relation ()) (name i2) (type_ ())) (Int 2)))))
     (((((relation ()) (name c1) (type_ ((IntT (nullable false))))) (Int 1))
       (((relation ()) (name i1) (type_ ((IntT (nullable false))))) (Int 4)))
      (((((relation ()) (name c1) (type_ ((IntT (nullable false))))) (Int 1))
        (((relation ()) (name c2) (type_ ())) (Int 1))
        (((relation ()) (name i1) (type_ ((IntT (nullable false))))) (Int 4))
        (((relation ()) (name i2) (type_ ())) (Int 1)))
       ((((relation ()) (name c1) (type_ ((IntT (nullable false))))) (Int 1))
        (((relation ()) (name c2) (type_ ())) (Int 3))
        (((relation ()) (name i1) (type_ ((IntT (nullable false))))) (Int 4))
        (((relation ()) (name i2) (type_ ())) (Int 3)))))) |}]

let make_module_db () =
  let module E = Eval.Make (struct
    let conn = create_db "postgresql://localhost:5433/demomatch"
  end) in
  let module A = Abslayout_db.Make (E) in
  ((module E : Eval.S), (module A : Abslayout_db.S))
