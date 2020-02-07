open! Core
open Test_util

let make_modules ?layout_file ?(irgen_debug = false) ?(code_only = false) () =
  let module S = Serialize.Make (struct
    let conn = Lazy.force test_db_conn

    let layout_file = layout_file
  end) in
  let module I =
    Irgen.Make
      (struct
        let code_only = code_only

        let debug = irgen_debug
      end)
      (S)
      ()
  in
  let module C =
    Codegen.Make
      (struct
        let debug = false
      end)
      (I)
      ()
  in
  ((module S : Serialize.S), (module I : Irgen.S), (module C : Codegen.S))
