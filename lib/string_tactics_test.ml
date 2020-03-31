open Abslayout
open Collections
open String_tactics
open Castor_test.Test_util

module C = struct
  let params =
    Set.singleton (module Name) (Name.create ~type_:Prim_type.string_t "param")

  let fresh = Fresh.create ()

  let verbose = false

  let validate = false

  let param_ctx = Map.empty (module Name)

  let conn = Lazy.force test_db_conn

  let cost_conn = Lazy.force test_db_conn

  let simplify = None
end

open Ops.Make (C)

open Make (C)

let load_string ?params s = Abslayout_load.load_string ?params C.conn s

(* let%expect_test "" =
 *   load_string ~params:C.params "filter(str_field = param, unique_str)"
 *   |> Branching.apply dictionary_encode Path.root
 *   |> Seq.iter ~f:(Format.printf "%a.@\n" pp);
 *   [%expect.unreachable]
 * [@@expect.uncaught_exn {|
 *   (\* CR expect_test_collector: This test expectation appears to contain a backtrace.
 *      This is strongly discouraged as backtraces are fragile.
 *      Please change this test to not include a backtrace. *\)
 * 
 *   (Failure "Not schema invariant")
 *   Raised at file "stdlib.ml", line 29, characters 17-33
 *   Called from file "castor/lib/ops.ml", line 357, characters 14-43
 *   Called from file "src/sequence.ml", line 189, characters 33-36
 *   Called from file "src/sequence.ml", line 186, characters 14-23
 *   Called from file "src/sequence.ml", line 351, characters 10-19
 *   Called from file "castor-opt/lib/string_tactics_test.ml" (inlined), line 34, characters 5-44
 *   Called from file "castor-opt/lib/string_tactics_test.ml", line 32, characters 2-164
 *   Called from file "castor/test/test_util.ml", line 8, characters 4-12
 *   Called from file "collector/expect_test_collector.ml", line 253, characters 12-19 |}] *)
