open Core
open Postgresql
open Dblayout
open Db

let main ?(seed = 0) ~db_in ~db_out ~sample =
  let conn = new connection ~dbname:db_in () in
  Logs.info (fun m -> m "Creating new database %s." db_out) ;
  exec conn ~params:[db_out] "drop database if exists $0" |> ignore ;
  exec conn ~params:[db_out; db_in] "create database $0 with template $1" |> ignore ;
  let conn = new connection ~dbname:db_out () in
  exec conn ~params:[Int.to_string seed] "set seed to $0" |> ignore ;
  List.iter (Db.Relation.all_from_db conn) ~f:(fun r ->
      Logs.info (fun m -> m "Sampling from table %s." r.rname) ;
      exec conn ~params:[r.rname] "alter table $0 rename to old_$0" |> ignore ;
      exec conn
        ~params:[r.rname; Int.to_string sample]
        "create table $0 as (select * from old_$0 order by random() limit $1)"
      |> ignore ;
      exec conn ~params:[r.rname] "drop table old_$0 cascade" |> ignore )

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ()) ;
  let open Command in
  let open Let_syntax in
  basic ~summary:"Generate a sample database for a benchmark."
    (let%map_open verbose =
       flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
     and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
     and db_in = flag "i" (required string) ~doc:"the input database"
     and db_out = flag "o" (required string) ~doc:"the output database"
     and sample = flag "n" (required int) ~doc:"the sample size for each relation"
     and seed = flag "s" (optional int) ~doc:"the random seed" in
     fun () ->
       if verbose then Logs.set_level (Some Logs.Debug)
       else if quiet then Logs.set_level (Some Logs.Error)
       else Logs.set_level (Some Logs.Info) ;
       main ?seed ~db_in ~db_out ~sample)
  |> run
