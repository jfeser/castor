open! Core
open Castor
open Abslayout
open Filter_tactics

module C = struct
  let params = Set.empty (module Name)

  let fresh = Fresh.create ()

  let verbose = false

  let validate = false

  let param_ctx = Map.empty (module Name)

  let conn = Lazy.force Test_util.test_db_conn

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
    (apply (at_ push_filter Path.(all >>? is_filter >>| shallowest)) r)
    ~f:(Format.printf "%a\n" pp) ;
  [%expect
    {| alist(r as r1, alist(filter((r1.f = f), r) as r2, ascalar(r2.f))) |}]

let%expect_test "push-filter-runtime" =
  let r =
    M.load_string
      "depjoin(r as r1, filter(r1.f = f, alist(r as r2, ascalar(r2.f))))"
  in
  Option.iter
    (apply (at_ push_filter Path.(all >>? is_filter >>| shallowest)) r)
    ~f:(Format.printf "%a\n" pp) ;
  [%expect
    {| depjoin(r as r1, alist(r as r2, filter((r1.f = f), ascalar(r2.f)))) |}]
