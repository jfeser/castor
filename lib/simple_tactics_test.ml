open! Core
open Castor
open Abslayout
open Simple_tactics
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
open Ops
module M = Abslayout_db.Make (C)

let%expect_test "row-store-comptime" =
  let r = M.load_string "alist(r as r1, filter(r1.f = f, r))" in
  Option.iter
    (apply (at_ row_store Path.(all >>? is_filter >>| shallowest)) r)
    ~f:(Format.printf "%a\n" pp) ;
  [%expect {|
    alist(r as r1,
      alist(filter((r1.f = f), r) as s0,
        atuple([ascalar(s0.f), ascalar(s0.g)], cross))) |}]

let%expect_test "row-store-runtime" =
  let r = M.load_string "depjoin(r as r1, filter(r1.f = f, r))" in
  Option.iter
    (apply (at_ row_store Path.(all >>? is_filter >>| shallowest)) r)
    ~f:(Format.printf "%a\n" pp) ;
  [%expect {| |}]
