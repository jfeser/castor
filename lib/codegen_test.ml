open Core
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

module Eval = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (Eval)

module S =
  Serialize.Make (struct
      let layout_map_channel = Some Out_channel.stdout
    end)
    (Eval)

module I =
  Implang.IRGen.Make (struct
      let code_only = false
    end)
    (Eval)
    (S)
    ()

module C =
  Codegen.Make (struct
      let debug = false
    end)
    (I)
    ()

let run_in_fork thunk =
  match Unix.fork () with
  | `In_the_child ->
      Backtrace.elide := false ;
      Logs.set_reporter (Logs.format_reporter ()) ;
      Logs.set_level (Some Logs.Debug) ;
      thunk () ;
      exit 0
  | `In_the_parent pid ->
      let _, err = Unix.wait (`Pid pid) in
      Unix.Exit_or_signal.to_string_hum err |> print_endline

[@@@warning "-8"]

let _, [f; _] =
  Test_util.create rels "r1" ["f"; "g"]
    [[0; 5]; [1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]; [4; 6]]

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
  Unix.system (sprintf "%s -p %s" exe_fn data_fn) |> ignore ;
  [%expect {||}]
