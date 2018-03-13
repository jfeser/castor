open Core

open Dblayout
open Collections

let main = fun ~debug ~gprof ~params fn ->
  let fd = Unix.openfile ~mode:[O_RDWR; O_CREAT] fn in
  let size = (Unix.Native_file.stat fn).st_size in
  let buf = Bigstring.create size in
  let ralgebra, _ =
    Bigstring.read_bin_prot buf Ralgebra.Binable.bin_reader_t
    |> Or_error.ok_exn
  in
  Unix.close fd;

  let ralgebra = Ralgebra.Binable.to_ralgebra ralgebra in

  let module CConfig = struct
    let debug = debug
    let ctx = Llvm.create_context ()
    let module_ = Llvm.create_module ctx "scanner"
    let builder = Llvm.builder ctx
  end in

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
      let val_str = match v with
        | `Int x -> sprintf "%d" x
        | `Bool true -> "true"
        | `Bool false -> "false"
        | `String x -> sprintf "\"%s\"" x
        | _ -> Error.of_string "Unexpected param type." |> Error.raise
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

  (* Collect runtime information. Most of this requires parsing the query
     output, so it doesn't work when debug info is turned on. *)
  if not debug then begin
    Caml.Sys.command "sh -c \"./scanner.exe -p db.buf > output.csv\"" |> ignore;
    let runs_per_sec =
      try
        Unix.open_process_in "./scanner.exe -t 10 db.buf" |> In_channel.input_all
        |> String.strip |> Float.of_string
      with _ -> 0.0
    in
    let db_size = (Unix.stat "db.buf").st_size in
    let exe_size = (Unix.stat "scanner.exe").st_size in

    let scanner_crashed = Caml.Sys.command "./scanner.exe -c db.buf" > 0 in
    if scanner_crashed then Logs.err (fun m -> m "Scanner crashed.");

    let outputs_differ =
      Caml.Sys.command "bash -c 'diff -q <(sort ../golden.csv) <(sort output.csv)'" > 0
    in
    if outputs_differ then Logs.err (fun m -> m "Outputs differ.");

    (runs_per_sec, db_size, exe_size, scanner_crashed || outputs_differ)
  end else (0.0, 0L, 0L, false)

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ());

  let open Command in
  let open Let_syntax in

  let param = Arg_type.create (fun s ->
      let k, v = String.lsplit2_exn ~on:':' s in
      let v = Sexp.of_string v |> [%of_sexp:Db.primvalue] in
      (k, v))
  in

  basic ~summary:"Compile a query." [%map_open
    let verbose = flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
    and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
    and debug = flag "debug" ~aliases:["g"] no_arg ~doc:"enable debug mode"
    and gprof = flag "prof" ~aliases:["pg"] no_arg ~doc:"enable profiling"
    and params = flag "param" ~aliases:["p"] (listed param) ~doc:"query parameters (passed as key:value)"
    and query = anon ("query" %: file)
    in fun () ->
      if verbose then Logs.set_level (Some Logs.Debug)
      else if quiet then Logs.set_level (Some Logs.Error)
      else Logs.set_level (Some Logs.Info);

      main ~debug ~gprof ~params query |> ignore
  ] |> run
