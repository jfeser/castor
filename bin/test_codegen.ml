open Base
open Base.Polymorphic_compare
open Base.Printf
open Stdio
open Postgresql

open Dblayout
open Eval
open Layout
open Collections
open Serialize
open Ralgebra
open Implang
open Db

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
  let taxi = Relation.from_db conn "taxi" in
  let layout = Transform.row_layout taxi in

  Logs.debug (fun m -> m "Relation schema %a." Sexp.pp_hum ([%sexp_of:Schema.t] (Schema.of_relation taxi)));
  Logs.debug (fun m -> m "Layout schema %a." Sexp.pp_hum ([%sexp_of:Schema.t] (Layout.to_schema_exn layout)));

  let l = Ralgebra0.(Filter (Binop (Gt, Field (Relation.field_exn taxi "xpos"), Var ("xv", IntT)), Scan layout)) in

  let module IGen = IRGen.Make () in
  let ir_module = IGen.irgen l in

  let module_ = Llvm.create_module (Llvm.global_context ()) "scanner" in
  let module CGen = Codegen.Make(struct
      open Llvm
      let ctx = global_context ()
      let module_ = module_
      let builder = builder ctx
    end) ()
  in
  CGen.codegen ir_module.buffer ir_module;
  Out_channel.with_file buf_file ~f:(fun ch -> Out_channel.output_bytes ch ir_module.buffer);
  Out_channel.with_file "scanner.h" ~f:CGen.write_header;

  Llvm.print_module ir_file module_;


  Caml.Sys.command (sprintf "llc -O0 -filetype=obj \"%s\"" ir_file) |> ignore;
  Caml.Sys.command (sprintf "clang -g -O0 scanner.o main.c -o scanner.exe") |> ignore

let () = Exn.handle_uncaught ~exit:true main
