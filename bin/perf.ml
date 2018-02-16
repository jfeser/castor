open Core
open Stdio
open Printf
open Postgresql
open Dblayout
open Collections

let in_dir : string -> f:(unit -> 'a) -> 'a = fun dir ~f ->
  let cur_dir = Unix.getcwd () in
  Unix.chdir dir;
  let ret = try f () with e -> Unix.chdir cur_dir; raise e in
  Unix.chdir cur_dir; ret

let benchmark : debug:bool -> params:(string * string) list -> gprof:bool -> Ralgebra.t -> unit =
fun ~debug ~params ~gprof ralgebra ->
  Logs.info (fun m -> m "Benchmarking %s." (Ralgebra.to_string ralgebra));

  let module IGen = Implang.IRGen.Make () in
  let ir_module = IGen.irgen ralgebra in
  let llctx = Llvm.create_context () in
  let module_ = Llvm.create_module llctx "scanner" in
  let module CGen = Codegen.Make(struct
      open Llvm
      let debug = debug
      let ctx = llctx
      let module_ = module_
      let builder = builder ctx
    end) ()
  in

  (* Dump IR. *)
  CGen.codegen ir_module.buffer ir_module;
  Llvm.print_module "scanner.ll" module_;
  Out_channel.with_file "scanner.h" ~f:CGen.write_header;

  Out_channel.with_file "db.buf" ~f:(fun ch ->
      let w = Bitstring.Writer.with_channel ch in
      Bitstring.Writer.write w ir_module.buffer);
  Out_channel.with_file "db.txt" ~f:(fun ch ->
      let fmt = Format.formatter_of_out_channel ch in
      Bitstring.pp fmt ir_module.buffer);

  (* Generate and dump main.c *)
  let params_str = List.map params ~f:(fun (n, v) ->
      let val_str =
        match [%of_sexp:Db.primvalue] (Sexp.of_string v) with
        | `Int x -> sprintf "%d" x
        | `Bool true -> "true"
        | `Bool false -> "false"
        | `String x -> sprintf "\"%s\"" x
      in
      sprintf "set_%s(params, %s);" n val_str) |> String.concat ~sep:"\n"
  in

  let perf_template = Config.project_root ^ "/bin/templates/perf.c" in
  In_channel.(with_file perf_template ~f:(fun ch ->
      let out = String.template (input_all ch)
          [ "#include \"scanner.h\""; params_str ] in
      let () =
        Out_channel.(with_file "main.c" ~f:(fun ch -> output_string ch out)) in
      ()));

  let command_exn : string list -> unit = function
    | [] -> Error.of_string "Empty command" |> Error.raise
    | args ->
      let cmd = String.concat args ~sep:" " in
      let ret = Sys.command cmd in
      if ret = 0 then () else
        Error.create "Non-zero exit code" ret [%sexp_of:int] |> Error.raise
  in

  Logs.debug (fun m -> m "LLVM root: %s" Config.llvm_root);

  let clang = Config.llvm_root ^ "/bin/clang" in
  let opt = Config.llvm_root ^ "/bin/opt" in
  let cflags = ["-lcmph"] in
  let cflags = (if debug then ["-g"; "-O0"] else []) @ cflags in
  let cflags = (if gprof then ["-pg"] else []) @ cflags in
  if debug then
    command_exn (clang :: cflags @ ["scanner.ll"; "main.c"; "-o"; "scanner.exe"])
  else begin
    command_exn (opt :: ["-S -pass-remarks-output=remarks.yaml -mergereturn -always-inline scanner.ll > scanner-opt.ll"]);
    command_exn (clang :: cflags @ ["scanner-opt.ll"; "main.c"; "-o"; "scanner.exe"])
  end;
  Llvm.dispose_module module_;
  Llvm.dispose_context llctx;

  (* Calibrate CPU counter. *)
  let calibrator = Time_stamp_counter.Calibrator.create () in
  Time_stamp_counter.Calibrator.calibrate ~t:calibrator ();
  let start = Time_stamp_counter.now () in
  if Caml.Sys.command ("./scanner.exe -c db.buf") > 0 then
    Logs.err (fun m -> m "Scanner failed.");
  let stop = Time_stamp_counter.now () in
  let runtime =
    Time_ns.(diff (Time_stamp_counter.to_time_ns ~calibrator stop)
               (Time_stamp_counter.to_time_ns ~calibrator start)
             |> Span.to_short_string)
  in
  Logs.info (fun m -> m "Total time: %s" runtime)

  let main : params:(string * string) list -> db:string -> ralgebra:_ ->
    verbose:bool -> quiet:bool -> transforms:Transform.t list option ->
    dir:string option -> debug:bool -> gprof:bool -> unit =
    fun ~params ~db ~ralgebra ~verbose ~quiet ~transforms ~dir ~debug ~gprof ->
      Logs.set_reporter (Logs.format_reporter ());
      if verbose then Logs.set_level (Some Logs.Debug)
      else if quiet then Logs.set_level (Some Logs.Error)
      else Logs.set_level (Some Logs.Info);

      let conn = new connection ~dbname:db () in
      Eval.Ctx.global.conn <- Some (conn);

      let predctx = List.map params ~f:(fun (n, v) ->
          (n, [%of_sexp:Db.primvalue] (Sexp.of_string v)))
                    |> Layout.PredCtx.of_vars
      in
      Eval.Ctx.global.testctx <- Some (predctx);

      let ralgebra = Ralgebra.resolve conn ralgebra in
      let candidates = match transforms with
        | Some tfs -> Transform.run_chain tfs ralgebra
        | None -> Transform.search conn ralgebra
      in
      let dir = match dir with
        | Some x -> x
        | None -> Filename.temp_dir "perf" ""
      in
      Logs.info (fun m -> m "Generating files in %s." dir);

      in_dir dir ~f:(fun () -> Seq.iter candidates ~f:(benchmark ~debug ~params ~gprof))

let () =
  let open Command in
  let kv = Arg_type.create (fun s -> String.lsplit2_exn ~on:':' s) in
  let ralgebra = Arg_type.create Ralgebra.of_string_exn in
  let transform = Arg_type.create Transform.of_name_exn in
  let open Let_syntax in
  basic ~summary:"Benchmark tool." [%map_open
    let db = flag "db" (required string) ~doc:"DB the database to connect to"
    and params = flag "param" ~aliases:["p"] (listed kv) ~doc:"query parameters (passed as key:value)"
    and verbose = flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
    and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
    and debug = flag "debug" ~aliases:["g"] no_arg ~doc:"enable debug mode"
    and gprof = flag "prof" ~aliases:["pg"] no_arg ~doc:"enable profiling"
    and transforms = flag "transform" ~aliases:["t"]
        (optional (Arg_type.comma_separated transform)) ~doc:"transforms to run"
    and dir = flag "dir" ~aliases:["d"] (optional file) ~doc:"where to write intermediate files"
    and ralgebra = anon ("ralgebra" %: ralgebra)
    in fun () -> main ~db ~ralgebra ~params ~verbose ~quiet ~transforms ~dir ~debug ~gprof
  ] |> run
