open! Core
open Castor
open Collections
open Abslayout_load

let main ~debug ~gprof ~params ~db ~code_only ?out_dir ch =
  Logs.info (fun m ->
      m "%s" (Sys.get_argv () |> Array.to_list |> String.concat ~sep:" "));
  let module CConfig = struct
    let conn = db

    let debug = debug

    let code_only = code_only
  end in
  let layout_file =
    if debug then
      let layout_file =
        match out_dir with Some d -> d ^ "/layout.txt" | None -> "layout.txt"
      in
      Some layout_file
    else None
  in
  let module I = Irgen.Make (CConfig) () in
  let module C = Codegen.Make (CConfig) (I) () in
  let params = List.map params ~f:(fun (n, t) -> Name.create ~type_:t n) in
  load_string
    ~params:(Set.of_list (module Name) params)
    db (In_channel.input_all ch)
  |> Type.annotate db
  |> C.compile ~gprof ~params ?out_dir ?layout_log:layout_file CConfig.conn
  |> ignore

let () =
  let open Command.Let_syntax in
  Command.basic ~summary:"Compile a query."
    [%map_open
      let () = Log.param
      and db = Db.param
      and debug = flag "debug" ~aliases:[ "g" ] no_arg ~doc:"enable debug mode"
      and gprof = flag "prof" ~aliases:[ "pg" ] no_arg ~doc:"enable profiling"
      and out_dir =
        flag "output" ~aliases:[ "o" ] (optional string)
          ~doc:"DIR directory to write compiler output in"
      and params =
        flag "param" ~aliases:[ "p" ] (listed Util.param)
          ~doc:"NAME:TYPE query parameters"
      and code_only = flag "code-only" no_arg ~doc:"only emit code"
      and ch =
        anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
      in
      fun () -> main ~debug ~gprof ~params ~db ~code_only ?out_dir ch]
  |> Command.run
