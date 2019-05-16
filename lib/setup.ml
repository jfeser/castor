open! Core
open Test_util

let make_modules ?layout_file ?(irgen_debug = false) ?(code_only = false) () =
  let module Config = struct
    let conn = Lazy.force test_db_conn
  end in
  let module M = Abslayout_db.Make (Config) in
  let module S =
    Serialize.Make (struct
        let layout_map_channel = Option.map layout_file ~f:Out_channel.create
      end)
      (M)
  in
  let module I =
    Irgen.Make (struct
        let code_only = code_only

        let debug = irgen_debug
      end)
      (M)
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
  ( (module M : Abslayout_db.S)
  , (module S : Serialize.S)
  , (module I : Irgen.S)
  , (module C : Codegen.S) )
