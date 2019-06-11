open! Core
open Castor
open Abslayout
open Filter_tactics
open Test_util

module C = struct
  let params =
    Set.singleton
      (module Name)
      (Name.create ~type_:Type.PrimType.int_t "param")

  let fresh = Fresh.create ()

  let verbose = false

  let validate = false

  let param_ctx = Map.empty (module Name)

  let conn = Lazy.force test_db_conn

  let simplify = None
end

module T = Make (C)
open T
module O = Ops.Make (C)
open O
module M = Abslayout_db.Make (C)

let%expect_test "push-filter-comptime" =
  let r =
    M.load_string
      "alist(r as r1, filter(r1.f = f, alist(r as r2, ascalar(r2.f))))"
  in
  Option.iter
    (apply
       (at_ push_filter Path.(all >>? is_filter >>| shallowest))
       Path.root r)
    ~f:(Format.printf "%a\n" pp) ;
  [%expect
    {| alist(r as r1, alist(filter((r1.f = f), r) as r2, ascalar(r2.f))) |}]

let%expect_test "push-filter-runtime" =
  let r =
    M.load_string
      "depjoin(r as r1, filter(r1.f = f, alist(r as r2, ascalar(r2.f))))"
  in
  Option.iter
    (apply
       (at_ push_filter Path.(all >>? is_filter >>| shallowest))
       Path.root r)
    ~f:(Format.printf "%a\n" pp) ;
  [%expect
    {| depjoin(r as r1, alist(r as r2, filter((r1.f = r2.f), ascalar(r2.f)))) |}]

let%expect_test "push-filter-support" =
  let r =
    M.load_string ~params:C.params
      "filter(f > param, ahashidx(select([f], r) as k, ascalar(0), 0))"
  in
  Option.iter
    (apply
       (at_ push_filter Path.(all >>? is_filter >>| shallowest))
       Path.root r)
    ~f:(Format.printf "%a\n" pp) ;
  [%expect
    {| ahashidx(select([f], r) as k, filter((k.f > param), ascalar(0)), 0) |}]

let with_log src f =
  Logs.Src.set_level src (Some Debug) ;
  Exn.protect ~f ~finally:(fun () -> Logs.Src.set_level src None)

let%expect_test "elim-eq-filter" =
  let r =
    M.load_string ~params:C.params
      "filter(fresh = param, select([f as fresh], r))"
  in
  with_log elim_eq_filter_src (fun () ->
      Option.iter
        (apply elim_eq_filter Path.root r)
        ~f:(Format.printf "%a\n" pp) ;
      [%expect
        {|
          ahashidx(dedup(select([fresh], select([f as fresh], r))) as s0,
            select([], filter((fresh = s0.fresh), select([f as fresh], r))),
            param) |}]
  )

let%expect_test "elim-eq-filter-approx" =
  let r =
    M.load_string ~params:C.params
      "filter(fresh = param, select([f as fresh], filter(g = param, r)))"
  in
  with_log elim_eq_filter_src (fun () ->
      Option.iter
        (apply elim_eq_filter Path.root r)
        ~f:(Format.printf "%a\n" pp) ;
      [%expect
        {|
          ahashidx(select([f as fresh], dedup(select([f], r))) as s0,
            select([],
              filter((fresh = s0.fresh), select([f as fresh], filter((g = param), r)))),
            param) |}]
  )
