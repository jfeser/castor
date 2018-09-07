open Core
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

[@@@warning "-8"]

let _, [f; _] =
  Test_util.create rels "r1" ["f"; "g"]
    [[0; 5]; [1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]; [4; 6]]

[@@@warning "+8"]

module Eval = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (Eval)

let make_modules layout_file =
  let module S =
    Serialize.Make (struct
        let layout_map_channel = Some (Out_channel.create layout_file)
      end)
      (Eval)
  in
  let module I =
    Implang.IRGen.Make (struct
        let code_only = false
      end)
      (Eval)
      (S)
      ()
  in
  let module C =
    Codegen.Make (struct
        let debug = false
      end)
      (I)
      ()
  in
  ((module S : Serialize.S), (module I : Implang.IRGen.S), (module C : Codegen.S))

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

let%expect_test "ordered-idx" =
  let layout_file = Filename.temp_file "layout" "txt" in
  let (module S), (module I), (module C) = make_modules layout_file in
  let layout =
    of_string_exn
      "AOrderedIdx(OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc) as k, \
       AScalar(k.f), 1, 3)"
    |> M.resolve |> M.annotate_schema |> M.annotate_key_layouts
  in
  let out_dir = Filename.temp_dir "bin" "" in
  let exe_fn, data_fn = C.compile ~out_dir ~gprof:false ~params:[] layout in
  In_channel.input_all (In_channel.create layout_file) |> print_endline ;
  [%expect {|
    0:8 Ordered idx len (=66)
    8:8 Ordered idx index len (=45)
    16:1 Scalar (=(Int 0))
    16:1 Ordered idx key
    17:8 Ordered idx value ptr (=0)
    25:1 Scalar (=(Int 1))
    25:1 Ordered idx key
    26:8 Ordered idx value ptr (=1)
    34:1 Scalar (=(Int 2))
    34:1 Ordered idx key
    35:8 Ordered idx value ptr (=2)
    43:1 Scalar (=(Int 3))
    43:1 Ordered idx key
    44:8 Ordered idx value ptr (=3)
    52:1 Scalar (=(Int 4))
    52:1 Ordered idx key
    53:8 Ordered idx value ptr (=4)
    61:1 Scalar (=(Int 0))
    62:1 Scalar (=(Int 1))
    63:1 Scalar (=(Int 2))
    64:1 Scalar (=(Int 3))
    65:1 Scalar (=(Int 4)) |}] ;
  Unix.system (sprintf "%s -p %s" exe_fn data_fn) |> ignore ;
  [%expect {|
    1,1,
    2,2, |}]
