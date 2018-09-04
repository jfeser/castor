open Core
open Dblayout
open Postgresql
open Collections

let main ~debug ~gprof ~params ~db ~port ~code_only fn =
  let module CConfig = struct
    let conn = new connection ~dbname:db ?port ()

    let debug = debug

    let code_only = code_only
  end in
  let module Eval = Eval.Make (CConfig) in
  let module Codegen = Codegen.Make (CConfig) () in
  let module IRGen = Implang.IRGen.Make (CConfig) (Eval) () in
  let module Abslayout_db = Abslayout_db.Make (Eval) in
  let ir_module =
    Logs.debug (fun m -> m "Loading ralgebra from %s." fn) ;
    let ralgebra =
      let params =
        List.map params ~f:(fun (n, t) -> Abslayout.Name.create ~type_:t n)
        |> Set.of_list (module Abslayout.Name.Compare_no_type)
      in
      In_channel.with_file fn ~f:Abslayout.of_channel_exn
      |> Abslayout_db.resolve ~params |> Abslayout_db.annotate_schema
    in
    Logs.debug (fun m -> m "Generating IR.") ;
    let ir_module = IRGen.irgen_abstract ~data_fn:"db.buf" ralgebra in
    Logs.debug (fun m -> m "Generating IR complete.") ;
    ir_module
  in
  (* Dump IR. *)
  Out_channel.with_file "scanner.ir" ~f:(fun ch ->
      IRGen.pp (Format.formatter_of_out_channel ch) ir_module ) ;
  (* Codegen *)
  Logs.debug (fun m -> m "Codegen.") ;
  Codegen.compile ~gprof ~params ir_module

let () =
  (* Turn on some llvm error handling. *)
  Llvm.enable_pretty_stacktrace () ;
  Llvm.install_fatal_error_handler (fun err ->
      let ocaml_trace = Backtrace.get () in
      print_endline (Backtrace.to_string ocaml_trace) ;
      print_endline "" ;
      print_endline err ) ;
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ()) ;
  let open Command in
  let open Let_syntax in
  let param =
    Arg_type.create (fun s ->
        let k, v = String.lsplit2_exn ~on:':' s in
        let v = Sexp.of_string v |> [%of_sexp : Type.PrimType.t] in
        (k, v) )
  in
  basic ~summary:"Compile a query."
    (let%map_open verbose =
       flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
     and db = flag "db" (required string) ~doc:"the database to connect to"
     and port = flag "port" (optional string) ~doc:"the port to connect to"
     and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
     and debug = flag "debug" ~aliases:["g"] no_arg ~doc:"enable debug mode"
     and gprof = flag "prof" ~aliases:["pg"] no_arg ~doc:"enable profiling"
     and params =
       flag "param" ~aliases:["p"] (listed param)
         ~doc:"query parameters (passed as key:value)"
     and code_only = flag "code-only" no_arg ~doc:"only emit code"
     and query = anon ("query" %: file) in
     fun () ->
       if verbose then Logs.set_level (Some Logs.Debug)
       else if quiet then Logs.set_level (Some Logs.Error)
       else Logs.set_level (Some Logs.Info) ;
       main ~debug ~gprof ~params ~db ~port ~code_only query)
  |> run
