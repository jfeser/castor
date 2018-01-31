open Core
open Stdio
open Printf
open Postgresql
open Dblayout
open Collections

let main : params:(string * string) list -> db:string -> ralgebra:_ -> verbose:bool -> quiet:bool -> unit =
  fun ~params ~db ~ralgebra ~verbose ~quiet ->
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
    let module IGen = Implang.IRGen.Make () in

    Transform.search conn ralgebra
    |> Seq.iter ~f:(fun ralgebra ->
        Logs.info (fun m -> m "Benchmarking %s." (Ralgebra.to_string ralgebra));

        let ir_module = IGen.irgen ralgebra in
        let llctx = Llvm.create_context () in
        let module_ = Llvm.create_module llctx "scanner" in
        let module CGen = Codegen.Make(struct
            open Llvm
            let ctx = llctx
            let module_ = module_
            let builder = builder ctx
          end) ()
        in

        let tmpdir = Unix.mkdtemp "perf" in
        Logs.info (fun m -> m "Generating files in %s." tmpdir);
        Unix.chdir tmpdir;

        (* Dump IR. *)
        CGen.codegen ir_module.buffer ir_module;
        Llvm.print_module "scanner.ll" module_;
        Out_channel.with_file "scanner.h" ~f:CGen.write_header;

        Out_channel.with_file "db.buf" ~f:(fun ch ->
            let w = Bitstring.Writer.with_channel ch in
            Bitstring.Writer.write w ir_module.buffer);

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
        In_channel.(with_file "/Users/jack/work/fastdb/bin/templates/perf.c" ~f:(fun ch ->
            let out = String.template (input_all ch)
                [ "#include \"scanner.h\""; params_str ] in
            let () =
              Out_channel.(with_file "main.c" ~f:(fun ch -> output_string ch out)) in
            ()));

        Caml.Sys.command ("llc -O2 -filetype=obj scanner.ll") |> ignore;
        Caml.Sys.command ("clang -g -O0 scanner.o main.c -o scanner.exe") |> ignore;
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
        Logs.info (fun m -> m "Total time: %s" runtime))

let () =
  let kv = Command.Arg_type.create (fun s -> String.lsplit2_exn ~on:':' s) in
  let ralgebra = Command.Arg_type.create Ralgebra.of_string_exn in
  let open Command.Let_syntax in
  Command.basic ~summary:"Benchmark tool." [%map_open
    let db = flag "db" (required string) ~doc:"DB the database to connect to"
    and params = flag "param" ~aliases:["p"] (listed kv) ~doc:"query parameters (passed as key:value)"
    and verbose = flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
    and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
    and ralgebra = anon ("ralgebra" %: ralgebra)
    in fun () -> main ~db ~ralgebra ~params ~verbose ~quiet
  ] |> Command.run
