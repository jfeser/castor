open Core
open Base
open Collections
open Abslayout
open Test_util

let rels = Hashtbl.create (module Db.Relation)

let create rels name fs xs =
  let rel =
    Db.Relation.
      { rname= name
      ; fields=
          List.map fs ~f:(fun f -> {Db.Field.fname= f; type_= IntT {nullable= false}}
          ) }
  in
  let data =
    List.map xs ~f:(fun data ->
        List.map2_exn fs data ~f:(fun fname value -> (fname, Value.Int value)) )
  in
  Hashtbl.set rels ~key:rel ~data ;
  ( name
  , List.map fs ~f:(fun f ->
        let open Name in
        { name= f
        ; relation= Some name
        ; type_= Some (Type0.PrimType.IntT {nullable= false}) } ) )

let create_val rels name fs xs =
  let rel =
    Db.Relation.
      { rname= name
      ; fields= List.map fs ~f:(fun (f, t) -> Db.Field.{fname= f; type_= t}) }
  in
  let data =
    List.map xs ~f:(fun data ->
        List.map2_exn fs data ~f:(fun (fname, _) value -> (fname, value)) )
  in
  Hashtbl.set rels ~key:rel ~data ;
  ( name
  , List.map fs ~f:(fun (f, t) ->
        let open Name in
        {name= f; relation= Some name; type_= Some t} ) )

let _ = create rels "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

let _ =
  create rels "log" ["id"; "succ"; "counter"]
    [[1; 4; 1]; [2; 3; 2]; [3; 4; 3]; [4; 6; 1]; [5; 6; 3]]

module E = Eval.Make_mock (struct
  let rels = rels
end)

let%expect_test "eval-select" =
  let query = "select([log.id as k], log)" |> of_string_exn in
  E.eval Ctx.empty query |> [%sexp_of: Ctx.t Gen.t] |> print_s ;
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
  E.eval Ctx.empty query |> [%sexp_of: Ctx.t Gen.t] |> print_s ;
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

(* let%expect_test "eval-foreach" =
 *   let module M = Abslayout_db.Make (E) in
 *   let q1 = "select([id as i1, counter as c1], log)" |> of_string_exn |> M.resolve in
 *   M.annotate_schema q1 ;
 *   let q2 =
 *     "select([log.id as i2, log.counter as c2], filter(log.succ = i1, log))"
 *     |> of_string_exn
 *   in
 *   M.annotate_schema q2 ;
 *   E.eval_foreach Ctx.empty q1 q2
 *   |> Seq.map ~f:(fun (c, s) -> (c, Seq.to_list s))
 *   |> Seq.to_list |> [%sexp_of: (Ctx.t * Ctx.t list) list] |> print_s ;
 *   [%expect
 *     {|
 *     ((((((relation ()) (name c1) (type_ ((IntT (nullable false))))) (Int 3))
 *        (((relation ()) (name i1) (type_ ((IntT (nullable false))))) (Int 3)))
 *       (((((relation ()) (name c1) (type_ ((IntT (nullable false))))) (Int 3))
 *         (((relation ()) (name c2) (type_ ())) (Int 2))
 *         (((relation ()) (name i1) (type_ ((IntT (nullable false))))) (Int 3))
 *         (((relation ()) (name i2) (type_ ())) (Int 2)))))
 *      (((((relation ()) (name c1) (type_ ((IntT (nullable false))))) (Int 1))
 *        (((relation ()) (name i1) (type_ ((IntT (nullable false))))) (Int 4)))
 *       (((((relation ()) (name c1) (type_ ((IntT (nullable false))))) (Int 1))
 *         (((relation ()) (name c2) (type_ ())) (Int 1))
 *         (((relation ()) (name i1) (type_ ((IntT (nullable false))))) (Int 4))
 *         (((relation ()) (name i2) (type_ ())) (Int 1)))
 *        ((((relation ()) (name c1) (type_ ((IntT (nullable false))))) (Int 1))
 *         (((relation ()) (name c2) (type_ ())) (Int 3))
 *         (((relation ()) (name i1) (type_ ((IntT (nullable false))))) (Int 4))
 *         (((relation ()) (name i2) (type_ ())) (Int 3)))))) |}] *)

(* let%expect_test "eval-foreach" =
 *   let (module E), (module A) = make_module_db () in
 *   let q1 =
 *     {|dedup(select([lp_id as lp_k, lc_id as lc_k], 
 *     join(true,
 *       select([id as lp_id], log_bench),
 *              select([id as lc_id], log_bench))))|}
 *     |> of_string_exn |> A.annotate_schema
 *   in
 *   let q2 =
 *     {|select([lp_counter, lc_counter], 
 *     join(lp_counter < lc_counter && 
 *          lc_counter < lp_succ, 
 *       select([counter as lp_counter, succ as lp_succ],
 *         filter(log_bench.id = lp_k, log_bench)), 
 *       select([counter as lc_counter],
 *              filter(log_bench.id = lc_k, log_bench))))|}
 *     |> of_string_exn |> A.annotate_schema
 *   in
 *   let summarize s =
 *     let s = Seq.map ~f:(fun (t, ts) -> (t, Seq.take ts 5)) s in
 *     Seq.take s 100
 *   in
 *   E.eval_foreach Ctx.empty q1 q2
 *   |> summarize |> [%sexp_of: (Ctx.t * Ctx.t Seq.t) Seq.t] |> print_s *)
