open! Core
open Castor
open Collections
open Abslayout_load

let main ~debug ~gprof ~params ~code_only ~query ~enable_redshift_dates ?out_dir
    fn =
  let open Result.Let_syntax in
  Global.enable_redshift_dates := enable_redshift_dates;
  Log.info (fun m ->
      m "Command: %a" Fmt.(array ~sep:sp string) (Sys.get_argv ()));
  let module Config = struct
    let conn = Db.create (Sys.getenv_exn "CASTOR_DB")

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
  let module I = Irgen.Make (Config) () in
  let module C = Codegen.Make (Config) (I) () in
  let open Config in
  let ch =
    match fn with Some fn -> In_channel.create fn | None -> In_channel.stdin
  in
  let filename = match fn with Some fn -> fn | None -> "<stdin>" in
  let ralgebra, params =
    let out =
      if query then
        let%map Query.{ body; args; _ } = Query.of_channel ch in
        (body, args)
      else
        let%map body = Abslayout.of_channel ch in
        (body, params)
    in
    match out with
    | Ok x -> x
    | Error e ->
        failwith
        @@ Fmt.str "Failed to parse %s: %a" filename (Abslayout.pp_err Fmt.nop)
             e
  in

  let ralgebra =
    let load_params =
      List.map params ~f:(fun (n, t) -> Name.create ~type_:t n)
      |> Set.of_list (module Name)
    in
    load_layout_exn ~params:load_params conn ralgebra
  in
  let params = List.map params ~f:(fun (n, t) -> (Name.create n, t)) in

  (* Attach the streams needed for folding over the ast. *)
  let ralgebra = Abslayout_fold.Data.annotate ?dir:out_dir conn ralgebra in

  (* Annotate with types. *)
  let ralgebra = Type.annotate ralgebra in

  let ralgebra =
    V.map_meta
      (fun m ->
        object
          method fold_stream = m#fold_stream

          method resolved = m#meta#meta#resolved

          method type_ = m#type_
        end)
      ralgebra
  in

  C.compile ~gprof ~params ?out_dir ?layout_log:layout_file ralgebra |> ignore

let () =
  let open Command.Let_syntax in
  Command.basic ~summary:"Compile a query."
    [%map_open
      let () = Log.param
      and () = Db.param
      and () = Join_elim.param
      and debug = flag "debug" ~aliases:[ "g" ] no_arg ~doc:"enable debug mode"
      and gprof = flag "prof" ~aliases:[ "pg" ] no_arg ~doc:"enable profiling"
      and enable_redshift_dates =
        flag "enable-redshift-dates" no_arg ~doc:"enable redshift date syntax"
      and out_dir =
        flag "output" ~aliases:[ "o" ] (optional string)
          ~doc:"DIR directory to write compiler output in"
      and params =
        flag "param" ~aliases:[ "p" ] (listed Util.param)
          ~doc:"NAME:TYPE query parameters"
      and code_only = flag "code-only" no_arg ~doc:"only emit code"
      and query =
        flag "query" ~aliases:[ "r" ] no_arg ~doc:"parse input as a query"
      and fn = anon (maybe ("query" %: string)) in
      fun () ->
        main ~debug ~gprof ~params ~code_only ~query ~enable_redshift_dates
          ?out_dir fn]
  |> Command.run
