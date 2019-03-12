open Core
open Castor
open Castor_opt

let main ~params:all_params ~db ~validate ch =
  let params =
    List.map all_params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
    |> Set.of_list (module Name.Compare_no_type)
  in
  let param_ctx =
    List.map all_params ~f:(fun (n, t, v) -> (Name.create ~type_:t n, v))
    |> Map.of_alist_exn (module Name.Compare_no_type)
  in
  let module Config = struct
    let conn = Db.create db

    let dbconn = new Postgresql.connection ~conninfo:db ()

    let params = params

    let param_ctx = param_ctx

    let validate = validate
  end in
  let module A = Abslayout_db.Make (Config) in
  let module T = Transform.Make (Config) () in
  let query_str = In_channel.input_all ch in
  let query = Abslayout.of_string_exn query_str |> A.resolve ~params in
  match T.(apply opt Path.root query) with
  | Some query' ->
      Or_error.iter_error (T.is_serializable query') ~f:(fun err ->
          Logs.warn (fun m -> m "Query is not serializable: %a" Error.pp err)
      ) ;
      Format.printf "%a" Abslayout.pp query'
  | None -> ()

let reporter ppf =
  let report _ level ~over k msgf =
    let k _ = over () ; k () in
    let with_time h _ k ppf fmt =
      let time = Core.Time.now () in
      Format.kfprintf k ppf
        ("%a [%s] @[" ^^ fmt ^^ "@]@.")
        Logs.pp_header (level, h) (Core.Time.to_string time)
    in
    msgf @@ fun ?header ?tags fmt -> with_time header tags k ppf fmt
  in
  {Logs.report}

let pp_level (level : Logs.level) =
  let style =
    match level with
    | Logs.App -> Logs_fmt.app_style
    | Error -> Logs_fmt.err_style
    | Warning -> Logs_fmt.warn_style
    | Info -> Logs_fmt.info_style
    | Debug -> Logs_fmt.debug_style
  in
  Fmt.(styled style string)

let setup_log level =
  Fmt_tty.setup_std_outputs () ;
  Logs.set_level (Some level) ;
  Logs.set_reporter
    (Logs_fmt.reporter
       ~pp_header:(fun fmt (level, hdr) ->
         let hdr = Option.value hdr ~default:"" in
         Fmt.pf fmt "[%a] [%a] %s" (pp_level level)
           (Logs.level_to_string (Some level))
           Time.pp (Time.now ()) hdr )
       ()) ;
  ()

let () =
  let open Command in
  let open Let_syntax in
  basic ~summary:"Compile a query."
    (let%map_open verbose =
       flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
     and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
     and validate =
       flag "validate" ~aliases:["c"] no_arg ~doc:"validate transforms"
     and db =
       flag "db" (required string) ~doc:"CONNINFO the database to connect to"
     and params =
       flag "param" ~aliases:["p"]
         (listed Util.param_and_value)
         ~doc:"NAME:TYPE query parameters"
     and ch =
       anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
     in
     fun () ->
       let level =
         if verbose then Logs.Debug
         else if quiet then Logs.Error
         else Logs.Info
       in
       setup_log level ;
       Logs.info (fun m ->
           m "%s" (Sys.argv |> Array.to_list |> String.concat ~sep:" ") ) ;
       main ~params ~db ~validate ch)
  |> run
