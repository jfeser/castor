open Abslayout
open Simple_tactics
open Test_util

module C = struct
  let params =
    Set.singleton (module Name) (Name.create ~type_:Prim_type.int_t "param")

  let fresh = Fresh.create ()
  let verbose = false
  let validate = false
  let param_ctx = Map.empty (module Name)
  let conn = Lazy.force test_db_conn
  let cost_conn = Lazy.force test_db_conn
  let simplify = None
end

open Make (C)
open Ops.Make (C)

let load_string ?params s = Abslayout_load.load_string_exn ?params C.conn s

let%expect_test "row-store-comptime" =
  let r = load_string "alist(r as r1, filter(r1.f = f, r))" in
  Option.iter
    (apply (at_ row_store Path.(all >>? is_filter >>| shallowest)) Path.root r)
    ~f:(Format.printf "%a\n" pp);
  [%expect
    {|
    alist(r as r1,
      alist(filter((r1.f = f), r) as s0,
        atuple([ascalar(s0.f), ascalar(s0.g)], cross))) |}]

let%expect_test "row-store-runtime" =
  let r = load_string "depjoin(r as r1, filter(r1.f = f, r))" in
  Option.iter
    (apply (at_ row_store Path.(all >>? is_filter >>| shallowest)) Path.root r)
    ~f:(Format.printf "%a\n" pp);
  [%expect {| |}]
