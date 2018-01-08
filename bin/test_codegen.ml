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

module Printexc = Caml.Printexc

let main () =
  Logs.set_reporter (Logs.format_reporter ());
  Logs.set_level (Some Logs.Debug);

  Printexc.register_printer (function
      | Postgresql.Error e -> Some (Postgresql.string_of_error e)
      | _ -> None);

  let conn = new connection ~dbname:"sam_analytics_small" () in
  Ctx.global.conn <- Some (conn);
  Ctx.global.testctx <- Some (PredCtx.of_vars ["xv", `Int 10; "yv", `Int 10]);
  let taxi = relation_from_db conn "taxi" in
  let l = row_layout taxi in
  let t = Type.of_layout_exn l in

  let module IGen = IRGen.Make () in
  let prog = IGen.gen_layout t in

  let module CGen = Codegen.Make(struct
      open Llvm
      let global_ctx = global_context ()
      let module_ = create_module global_ctx "scanner"
      let builder = builder global_ctx
      let values = Hashtbl.create (module String) ()
      let iters = Hashtbl.create (module String) ()
      let funcs = Hashtbl.create (module String) ()
    end) ()
  in
  CGen.codegen (serialize l) prog

let () = Exn.handle_uncaught ~exit:true main
