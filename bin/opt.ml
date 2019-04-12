open Core
open Castor
open Collections
open Castor_opt

let main ~params:all_params ~db ~validate ~verbose ch =
  let params =
    List.map all_params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
    |> Set.of_list (module Name)
  in
  let param_ctx =
    List.map all_params ~f:(fun (n, t, v) -> (Name.create ~type_:t n, v))
    |> Map.of_alist_exn (module Name)
  in
  let module Config = struct
    let conn = Db.create db

    let dbconn = new Postgresql.connection ~conninfo:db ()

    let params = params

    let param_ctx = param_ctx

    let validate = validate

    let fresh = Fresh.create ()

    let verbose = verbose
  end in
  let module A = Abslayout_db.Make (Config) in
  let module T = Transform.Make (Config) () in
  let module O = Ops.Make (Config) in
  let query_str = In_channel.input_all ch in
  let query = Abslayout.of_string_exn query_str |> A.resolve ~params in
  match Transform.optimize (module Config) query with
  | Some query' ->
      Or_error.iter_error (T.is_serializable query') ~f:(fun err ->
          Logs.warn (fun m -> m "Query is not serializable: %a" Error.pp err)
      ) ;
      Format.printf "%a" Abslayout.pp query'
  | None -> ()

let setup_log level =
  let format_reporter ppf =
    let open Logs in
    let report src level ~over k msgf =
      let style =
        match level with
        | Logs.App -> `White
        | Debug -> `Green
        | Info -> `Blue
        | Warning -> `Yellow
        | Error -> `Red
      in
      let k _ = over () ; k () in
      let format ?header:_ ?tags:_ fmt =
        Fmt.kpf k ppf
          ("[%a] [%a] [%s] @[" ^^ fmt ^^ "@]@.")
          Fmt.(styled style Logs.pp_level)
          level Time.pp (Time.now ()) (Src.name src)
      in
      msgf format
    in
    {report}
  in
  let ppf = Fmt_tty.setup Out_channel.stderr in
  Logs.set_level (Some level) ;
  Logs.set_reporter (format_reporter ppf)

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
       main ~params ~db ~validate ~verbose ch)
  |> run
