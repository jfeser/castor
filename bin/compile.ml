open Core
open Postgresql
open Dblayout
open Collections

let main = fun ~debug ~gprof ~params ~abs_layout ~db ~port fn ->
  let module CConfig = struct
    let conn = new connection ~dbname:db ?port ()
    let debug = debug
    let ctx = Llvm.create_context ()
    let module_ = Llvm.create_module ctx "scanner"
    let builder = Llvm.builder ctx
  end in
  let module Codegen = Codegen.Make(CConfig) () in
  let module IRGen = Implang.IRGen.Make(CConfig) () in

  let ir_module =
    if abs_layout then begin
      Logs.debug (fun m -> m "Loading ralgebra from %s." fn);
      let ralgebra =
        In_channel.with_file fn ~f:Abslayout.of_channel_exn
        |> Abslayout.resolve CConfig.conn
      in

      Logs.debug (fun m -> m "Generating IR.");
      let ir_module = IRGen.irgen_abstract ralgebra in
      Logs.debug (fun m -> m "Generating IR complete.");
      ir_module
    end else begin
      Logs.debug (fun m -> m "Loading ralgebra from %s." fn);
      let fd = Unix.openfile ~mode:[O_RDWR] fn in
      let size = (Unix.Native_file.stat fn).st_size in
      let buf = Bigstring.map_file ~shared:false fd size in
      let Candidate.Binable.({ ralgebra; transforms }), _ = 
        Bigstring.read_bin_prot buf Candidate.Binable.bin_reader_t
        |> Or_error.ok_exn
      in
      Unix.close fd;
      Logs.debug (fun m -> m "Loading complete.");

      let ralgebra = Ralgebra.Binable.to_ralgebra ralgebra in

      (* Dump type. *)
      Out_channel.with_file "ralgebra" ~f:(fun ch ->
          Ralgebra.to_string ralgebra |> Out_channel.output_string ch);

      Logs.debug (fun m -> m "Generating IR.");
      let ir_module = IRGen.irgen ralgebra in
      Logs.debug (fun m -> m "Generating IR complete.");
      ir_module
    end
  in

  (* Dump IR. *)
  Out_channel.with_file "scanner.ir" ~f:(fun ch ->
      IRGen.pp (Format.formatter_of_out_channel ch) ir_module);

  (* Codegen *)
  Logs.debug (fun m -> m "Codegen.");
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
  let params_str =
    List.filter params ~f:(fun (n, _) ->
        List.exists ir_module.params ~f:(fun (n', _) -> String.(n = n')))
    |> List.map ~f:(fun (n, v) ->
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
  Llvm.dispose_context CConfig.ctx

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
    and db = flag "db" (required string) ~doc:"the database to connect to"
    and port = flag "port" (optional string) ~doc:"the port to connect to"
    and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
    and debug = flag "debug" ~aliases:["g"] no_arg ~doc:"enable debug mode"
    and gprof = flag "prof" ~aliases:["pg"] no_arg ~doc:"enable profiling"
    and abs_layout = flag "abs" ~aliases:["a"] no_arg ~doc:"parse an abstract query"
    and params = flag "param" ~aliases:["p"] (listed param) ~doc:"query parameters (passed as key:value)"
    and query = anon ("query" %: file)
    in fun () ->
      if verbose then Logs.set_level (Some Logs.Debug)
      else if quiet then Logs.set_level (Some Logs.Error)
      else Logs.set_level (Some Logs.Info);

      main ~debug ~gprof ~params ~abs_layout ~db ~port query
  ] |> run
