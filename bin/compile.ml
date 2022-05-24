open! Core
open Castor
open Collections
open Abslayout_load

let ( let* ) x y = Db.with_conn x y

let main ~debug ~gprof ~params ~query ~enable_redshift_dates ~output_layout ?db
    ?out_dir fn =
  let open Result.Let_syntax in
  Global.enable_redshift_dates := enable_redshift_dates;
  Log.info (fun m ->
      m "Command: %a" Fmt.(array ~sep:sp string) (Sys.get_argv ()));

  let* conn =
    Option.value_or_thunk db ~default:(fun () -> Sys.getenv_exn "CASTOR_DB")
  in

  let layout_file =
    if debug || output_layout then
      let layout_file =
        match out_dir with Some d -> d ^ "/layout.txt" | None -> "layout.txt"
      in
      Some layout_file
    else None
  in

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

  (* Attach the streams needed for folding over the ast. *)
  let ralgebra =
    Abslayout_fold.Data.annotate ?dir:out_dir conn @@ Equiv.annotate ralgebra
  in

  (* Annotate with types. *)
  let ralgebra = Type.annotate ralgebra in

  let ralgebra =
    V.map_meta
      (fun m ->
        object
          method fold_stream = m#fold_stream
          method resolved = m#meta#meta#meta#resolved
          method type_ = m#type_
          method eq = m#meta#meta#eq
        end)
      ralgebra
  in

  Codegen.compile ~gprof ~params ?out_dir ?layout_log:layout_file ralgebra
  |> ignore

let spec =
  let open Command.Let_syntax in
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
    and query =
      flag "query" ~aliases:[ "r" ] no_arg ~doc:"parse input as a query"
    and db = flag "db" (optional string) ~doc:" database url"
    and output_layout =
      flag "output-layout" no_arg ~doc:" output layout description"
    and fn = anon (maybe ("query" %: string)) in
    fun () ->
      main ~debug ~gprof ~params ~query ~enable_redshift_dates ?out_dir ?db
        ~output_layout fn]

let () = Command.basic spec ~summary:"Compile a query." |> Command_unix.run
