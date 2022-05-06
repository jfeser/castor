let make_modules () =
  let module C =
    Codegen.Make
      (struct
        let debug = false
      end)
      ()
  in
  (module C : Codegen.S)
