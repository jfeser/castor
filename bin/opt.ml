open Core
open Castor
open Castor_opt

let main ~params ~db ch =
  let params =
    List.map params ~f:(fun (n, t) -> Name.create ~type_:t n)
    |> Set.of_list (module Name.Compare_no_type)
  in
  let module Config = struct
    let conn = Db.create db

    let dbconn = new Postgresql.connection ~conninfo:db ()

    let params = params
  end in
  let module A = Abslayout_db.Make (Config) in
  let module T = Transform.Make (Config) () in
  let query_str = In_channel.input_all ch in
  let query = Abslayout.of_string_exn query_str |> A.resolve ~params in
  match T.opt query with
  | Some query' -> Format.printf "%a" Abslayout.pp query'
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

let () =
  Logs.set_reporter (reporter Format.err_formatter) ;
  let open Command in
  let open Let_syntax in
  Logs.info (fun m ->
      m "%s" (Sys.argv |> Array.to_list |> String.concat ~sep:" ") ) ;
  basic ~summary:"Compile a query."
    (let%map_open verbose =
       flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
     and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
     and db =
       flag "db" (required string) ~doc:"CONNINFO the database to connect to"
     and params =
       flag "param" ~aliases:["p"] (listed Util.param)
         ~doc:"NAME:TYPE query parameters"
     and ch =
       anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
     in
     fun () ->
       if verbose then Logs.set_level (Some Logs.Debug)
       else if quiet then Logs.set_level (Some Logs.Error)
       else Logs.set_level (Some Logs.Info) ;
       Logs.info (fun m ->
           m "%s" (Sys.argv |> Array.to_list |> String.concat ~sep:" ") ) ;
       main ~params ~db ch)
  |> run
