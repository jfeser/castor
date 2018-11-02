open Core
open Castor
open Collections

let main ~debug ~gprof ~params ~db ~port ~code_only ?out_dir ch =
  let module CConfig = struct
    let conn = Db.create db ?port

    let debug = debug

    let code_only = code_only

    let layout_map_channel =
      if debug then
        let layout_file =
          match out_dir with Some d -> d ^ "/layout.txt" | None -> "layout.txt"
        in
        Some (Out_channel.create layout_file)
      else None
  end in
  let module E = Eval.Make (CConfig) in
  let module S = Serialize.Make (CConfig) (E) in
  let module I = Irgen.Make (CConfig) (E) (S) () in
  let module C = Codegen.Make (CConfig) (I) () in
  let module A = Abslayout_db.Make (E) in
  let params = List.map params ~f:(fun (n, t) -> Name.create ~type_:t n) in
  (* Codegen *)
  Logs.debug (fun m -> m "Codegen.") ;
  let ralgebra =
    let params = Set.of_list (module Name.Compare_no_type) params in
    Abslayout.of_channel_exn ch |> A.resolve ~params |> A.annotate_schema
    |> A.annotate_key_layouts
  in
  A.annotate_subquery_types ralgebra ;
  C.compile ~gprof ~params ?out_dir ralgebra |> ignore

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ()) ;
  let open Command in
  let open Let_syntax in
  Logs.info (fun m -> m "%s" (Sys.argv |> Array.to_list |> String.concat ~sep:" ")) ;
  basic ~summary:"Compile a query."
    (let%map_open verbose =
       flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
     and db = flag "db" (required string) ~doc:"DBNAME the database to connect to"
     and port = flag "port" (optional string) ~doc:"PORT the port to connect to"
     and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
     and debug = flag "debug" ~aliases:["g"] no_arg ~doc:"enable debug mode"
     and gprof = flag "prof" ~aliases:["pg"] no_arg ~doc:"enable profiling"
     and out_dir =
       flag "output" ~aliases:["o"] (optional string)
         ~doc:"DIR directory to write compiler output in"
     and params =
       flag "param" ~aliases:["p"] (listed Util.param)
         ~doc:"NAME:TYPE query parameters"
     and code_only = flag "code-only" no_arg ~doc:"only emit code"
     and ch =
       anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
     in
     fun () ->
       if verbose then Logs.set_level (Some Logs.Debug)
       else if quiet then Logs.set_level (Some Logs.Error)
       else Logs.set_level (Some Logs.Info) ;
       Logs.info (fun m ->
           m "%s" (Sys.argv |> Array.to_list |> String.concat ~sep:" ") ) ;
       main ~debug ~gprof ~params ~db ~port ~code_only ?out_dir ch)
  |> run
