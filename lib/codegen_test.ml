open Core
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

module Eval = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (Eval)

module I =
  Implang.IRGen.Make (struct
      let code_only = false
    end)
    (Eval)
    ()

module C =
  Codegen.Make (struct
      let debug = false
    end)
    (I)
    ()

[@@@warning "-8"]

let _, [f; _] =
  Test_util.create rels "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

[@@@warning "+8"]

let%expect_test "ordered-idx" =
  let layout =
    of_string_exn
      "AOrderedIdx(OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc) as k, \
       AScalar(k.f), 1, 3)"
    |> M.resolve |> M.annotate_schema |> M.annotate_key_layouts
  in
  let out_dir = Filename.temp_dir "bin" "" in
  let exe_fn, data_fn = C.compile ~out_dir ~gprof:false ~params:[] layout in
  Sys.command_exn (sprintf "%s %s" exe_fn data_fn)
