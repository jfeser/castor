open Base
open Base.Polymorphic_compare
open Base.Printf
open Stdio
open Postgresql
open Dblayout
open Layout
open Locality
open Collections
open Serialize
open Ralgebra
open Implang

let main () =
  Logs.set_reporter (Logs.format_reporter ());
  Logs.set_level (Some Logs.Debug);

  let prog = ["main", Infix.(
      fun_ ["x", IntT] IntT [] [
        loop (int 10) [
            Yield (Var "x");
          ]
      ])]
  in
  let module Gen = Codegen.Make(struct
      open Llvm
      let global_ctx = global_context ()
      let module_ = create_module global_ctx "scanner"
      let builder = builder global_ctx
      let values = Hashtbl.create (module String) ()
      let iters = Hashtbl.create (module String) ()
      let funcs = Hashtbl.create (module String) ()
    end) ()
  in
  Gen.codegen prog

let () = Exn.handle_uncaught ~exit:true main
