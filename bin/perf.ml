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

let benchmark : debug:bool -> params:(string * string) list -> gprof:bool -> _ -> Ralgebra.t -> unit =
fun ~debug ~params ~gprof (module IConfig : Implang.Config.S) ralgebra ->
  Logs.info (fun m -> m "Benchmarking %s." (Ralgebra.to_string ralgebra));

  let module CConfig = struct
    include IConfig

    let debug = debug
    let ctx = Llvm.create_context ()
    let module_ = Llvm.create_module ctx "scanner"
    let builder = Llvm.builder ctx
  end in

  let module Implang = Implang.Make(CConfig) in
  let module Codegen = Codegen.Make(CConfig) () in
  let module IRGen = Implang.IRGen.Make () in

  let ir_module = IRGen.irgen ralgebra in

  (* Dump IR. *)
  Out_channel.with_file "scanner.ir" ~f:(fun ch ->
      IRGen.pp (Format.formatter_of_out_channel ch) ir_module);

  (* Codegen *)
  Codegen.codegen ir_module.buffer ir_module;
  Llvm.print_module "scanner.ll" CConfig.module_;
  Out_channel.with_file "scanner.h" ~f:Codegen.write_header;

  (* Serialize. *)
  Out_channel.with_file "db.buf" ~f:(fun ch ->
      let w = Bitstring.Writer.with_channel ch in
      Bitstring.Writer.write w ir_module.buffer);
  Out_channel.with_file "db.txt" ~f:(fun ch ->
      let fmt = Format.formatter_of_out_channel ch in
      Bitstring.pp fmt ir_module.buffer);

  (* Compile and link *)
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
  Llvm.dispose_module CConfig.module_;
  Llvm.dispose_context CConfig.ctx;

  (* Collect runtime information. *)
  if Caml.Sys.command ("./scanner.exe -c db.buf") > 0 then
    Logs.err (fun m -> m "Scanner failed.")

let main :
  ?transforms:string list ->
  ?dir:string ->
  ?sample:int ->
  debug:bool ->
  gprof:bool ->
  params:(string * string) list ->
  db:string ->
  ralgebra:_ ->
  verbose:bool ->
  quiet:bool ->
  unit =
  fun ?transforms ?dir ?sample ~debug ~gprof ~params ~db ~ralgebra ~verbose ~quiet ->
    if verbose then Logs.set_level (Some Logs.Debug)
    else if quiet then Logs.set_level (Some Logs.Error)
    else Logs.set_level (Some Logs.Info);

    let module Config = struct
      let conn = new connection ~dbname:db ()
      let testctx = List.map params ~f:(fun (n, v) ->
          (n, [%of_sexp:Db.primvalue] (Sexp.of_string v)))
                    |> Layout.PredCtx.of_vars
    end in

    let module Transform = Transform.Make(Config) in
    let ralgebra = Ralgebra.resolve Config.conn ralgebra in

    (* If we need to sample, generate sample tables and swap them in the
       expression. *)
    let ralgebra = match sample with
      | Some s ->
        Ralgebra.relations ralgebra
        |> List.map ~f:(fun r -> r, Db.Relation.sample Config.conn s r)
        |> List.fold_left ~init:ralgebra ~f:(fun ra (r1, r2) ->
            Ralgebra.replace_relation r1 r2 ra)
      | None -> ralgebra
    in

    let candidates = match transforms with
      | Some tfs ->
        Transform.run_chain (List.map ~f:Transform.of_name_exn tfs) ralgebra
      | None -> Transform.search ralgebra
    in
    let dir = match dir with
      | Some x -> x
      | None -> Filename.temp_dir "perf" ""
    in
    Logs.info (fun m -> m "Generating files in %s." dir);

    in_dir dir ~f:(fun () ->
        match Seq.next candidates with
        | Some (x, _) -> benchmark ~debug ~params ~gprof (module Config) x
        | _ -> failwith ""
      );

    if Logs.err_count () > 0 then exit 1 else exit 0

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ());

  let open Command in
  let kv = Arg_type.create (fun s -> String.lsplit2_exn ~on:':' s) in
  let ralgebra = Arg_type.create Ralgebra.of_string_exn in
  let open Let_syntax in
  basic ~summary:"Benchmark tool." [%map_open
    let db = flag "db" (required string) ~doc:"DB the database to connect to"
    and params = flag "param" ~aliases:["p"] (listed kv) ~doc:"query parameters (passed as key:value)"
    and verbose = flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
    and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
    and debug = flag "debug" ~aliases:["g"] no_arg ~doc:"enable debug mode"
    and gprof = flag "prof" ~aliases:["pg"] no_arg ~doc:"enable profiling"
    and transforms = flag "transform" ~aliases:["t"]
        (optional (Arg_type.comma_separated string)) ~doc:"transforms to run"
    and dir = flag "dir" ~aliases:["d"] (optional file) ~doc:"where to write intermediate files"
    and sample = flag "sample" ~aliases:["s"] (optional int) ~doc:"the number of rows to sample from large tables"
    and ralgebra = anon ("ralgebra" %: ralgebra)
    in fun () ->
      main ~db ~ralgebra ~params ~verbose ~quiet ?transforms ?dir ~debug ~gprof ?sample
  ] |> run
