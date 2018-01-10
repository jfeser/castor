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

  let ir_file, buf_file = match Caml.Sys.argv with
    | [|_; x; y|] -> (x, y)
    | _ -> printf "Usage: test_codegen.exe IR BUF"; Caml.exit 1
  in

  let conn = new connection ~dbname:"sam_analytics_small" () in
  Ctx.global.conn <- Some (conn);
  Ctx.global.testctx <- Some (PredCtx.of_vars ["xv", `Int 10; "yv", `Int 10]);
  let taxi = relation_from_db conn "taxi" in
  let l = row_layout taxi in
  let t = Type.of_layout_exn l in

  let module IGen = IRGen.Make () in
  let ir_module = IGen.gen_layout t in

  let module_ = Llvm.create_module (Llvm.global_context ()) "scanner" in
  let module CGen = Codegen.Make(struct
      open Llvm
      let ctx = global_context ()
      let module_ = module_
      let builder = builder ctx
    end) ()
  in
  let buf = serialize l in
  CGen.codegen buf ir_module;

  Out_channel.with_file buf_file ~f:(fun ch -> Out_channel.output_bytes ch buf);
  Llvm.print_module ir_file module_;

  Caml.Sys.command (sprintf "llc -filetype=obj \"%s\"" ir_file) |> ignore
  (* Caml.Sys.command (sprintf "clang %s.o -o %s.exe" ir_file ir_file) |> ignore *)

let () = Exn.handle_uncaught ~exit:true main
