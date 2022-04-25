let make_modules ?(irgen_debug = false) ?(code_only = false) () =
  let module I =
    Irgen.Make
      (struct
        let code_only = code_only
        let debug = irgen_debug
      end)
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
  ((module I : Irgen.S), (module C : Codegen.S))
