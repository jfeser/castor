open Core
open Postgresql
open Dblayout
open Collections

let c_template : string -> (string * string) list -> string =
 fun fn args ->
  let args_str =
    List.map args ~f:(fun (n, x) -> sprintf "-D%s=%s" n x) |> String.concat ~sep:" "
  in
  let cmd = sprintf "clang -E %s %s" args_str fn in
  Unix.open_process_in cmd |> In_channel.input_all

let main ~debug ~gprof ~params ~db ~port ~code_only fn =
  let module CConfig = struct
    let conn = new connection ~dbname:db ?port ()

    let debug = debug

    let ctx = Llvm.create_context ()

    let module_ = Llvm.create_module ctx "scanner"

    let builder = Llvm.builder ctx

    let code_only = code_only

    let layout_map = true
  end in
  let module Codegen = Codegen.Make (CConfig) () in
  let module IRGen = Implang.IRGen.Make (CConfig) () in
  let module Abslayout = Abslayout.Make_db (CConfig) () in
  let ir_module =
    Logs.debug (fun m -> m "Loading ralgebra from %s." fn) ;
    let ralgebra =
      In_channel.with_file fn ~f:Abslayout.of_channel_exn
      |> Abslayout.resolve CConfig.conn
      |> Abslayout.annotate_schema
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
  Codegen.codegen ir_module ;
  Llvm.print_module "scanner.ll" CConfig.module_ ;
  Out_channel.with_file "scanner.h" ~f:Codegen.write_header ;
  (* Compile and link *)
  let funcs, calls =
    List.filter_mapi params ~f:(fun i (n, t) ->
        if List.exists ir_module.params ~f:(fun (n', _) -> String.(n = n')) then
          let open Type.PrimType in
          let from_fn fn =
            let template = Config.project_root ^ "/bin/templates/" ^ fn in
            let func =
              c_template template [("PARAM_NAME", n); ("PARAM_IDX", Int.to_string i)]
            in
            let call = sprintf "set_%s(params, input_%s(argv, optind));" n n in
            Some (func, call)
          in
          match t with
          | NullT -> failwith "No null parameters."
          | IntT -> from_fn "load_int.c"
          | BoolT -> from_fn "load_bool.c"
          | StringT -> from_fn "load_string.c"
        else None )
    |> List.unzip
  in
  let funcs_str = String.concat (["#include \"scanner.h\""] @ funcs) ~sep:"\n" in
  let calls_str = String.concat calls ~sep:"\n" in
  let perf_template = Config.project_root ^ "/bin/templates/perf.c" in
  let perf_c =
    let open In_channel in
    with_file perf_template ~f:(fun ch ->
        String.template (input_all ch) [funcs_str; calls_str] )
  in
  Out_channel.(with_file "main.c" ~f:(fun ch -> output_string ch perf_c)) ;
  let command_exn : string list -> unit = function
    | [] -> Error.of_string "Empty command" |> Error.raise
    | args ->
        let cmd = String.concat args ~sep:" " in
        Logs.info (fun m -> m "%s" cmd) ;
        let ret = Sys.command cmd in
        if ret = 0 then ()
        else Error.create "Non-zero exit code" ret [%sexp_of : int] |> Error.raise
  in
  let clang = Config.llvm_root ^ "/bin/clang" in
  let opt = Config.llvm_root ^ "/bin/opt" in
  let cflags = ["-g"; "-lcmph"] in
  let cflags = (if debug then ["-O0"] else []) @ cflags in
  let cflags = (if gprof then ["-pg"] else []) @ cflags in
  if debug then
    command_exn ((clang :: cflags) @ ["scanner.ll"; "main.c"; "-o"; "scanner.exe"])
  else (
    command_exn
      [ opt
      ; "-S -pass-remarks-output=remarks.yaml -globalopt -simplifycfg -dce -inline \
         -dce -simplifycfg -sroa -instcombine -simplifycfg -sroa -instcombine \
         -jump-threading -instcombine -reassociate -early-cse -mem2reg -loop-idiom \
         -loop-rotate -licm -loop-unswitch -loop-deletion -loop-unroll -sroa \
         -instcombine -gvn -memcpyopt -sccp -sink -instsimplify -instcombine \
         -jump-threading -dse -simplifycfg -loop-idiom -loop-deletion \
         -jump-threading -slp-vectorizer -load-store-vectorizer -adce \
         -loop-vectorize -instcombine -simplifycfg -loop-load-elim scanner.ll > \
         scanner-opt.ll" ] ;
    command_exn
      ((clang :: cflags) @ ["scanner-opt.ll"; "main.c"; "-o"; "scanner.exe"]) ) ;
  Llvm.dispose_module CConfig.module_ ;
  Llvm.dispose_context CConfig.ctx

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
