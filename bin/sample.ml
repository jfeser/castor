open Core
open Postgresql
open Dblayout
open Db

let main : ?seed:int -> db_in:string -> db_out:string -> sample:int -> Bench.t -> unit =
 fun ?(seed= 0) ~db_in ~db_out ~sample {name; params; query} ->
  (* FIXME: Use the first parameter value for test params. Should use multiple
       choices and average. *)
  (* let params = List.map params ~f:(fun (pname, values) ->
     *     match values with
     *     | [] -> Error.create "Empty parameter list." (name, pname)
     *               [%sexp_of:string * string] |> Error.raise
     *     | v::_ -> (pname, v))
     *                 |> Map.of_alist_exn (module String)
     * in *)
  let conn = new connection ~dbname:db_in () in
  let ralgebra = Ralgebra.of_string_exn query |> Ralgebra.resolve conn in
  let preds =
    Ralgebra.required_predicates ralgebra
    (* |> Map.map ~f:(fun ps -> List.map ps ~f:(Ralgebra.pred_subst params)) *)
    |> Map.map ~f:(fun ps -> Ralgebra0.Varop (And, ps) |> Ralgebra.pred_to_sql_exn)
  in
  exec conn ~params:[db_out] "drop database if exists $0" |> ignore ;
  exec conn ~params:[db_out; db_in] "create database $0 with template $1" |> ignore ;
  let conn = new connection ~dbname:db_out () in
  exec conn ~params:[Int.to_string seed] "set seed to $0" |> ignore ;
  List.iter (Ralgebra.relations ralgebra) ~f:(fun r ->
      exec conn ~params:[r.Relation.name] "alter table $0 rename to old_$0" |> ignore ;
      ( match Map.find preds r with
      | Some p_sql ->
          exec conn ~params:[r.Relation.name; Int.to_string sample; p_sql]
            "create table $0 as (select * from old_$0 where $2 order by random() limit \
             $1)"
          |> ignore
      | None ->
          exec conn ~params:[r.Relation.name; Int.to_string sample]
            "create table $0 as (select * from old_$0 order by random() limit $1)"
          |> ignore ) ;
      exec conn ~params:[r.Relation.name] "drop table old_$0" |> ignore )

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ()) ;
  let open Command in
  let bench = Arg_type.create (fun s -> Sexp.load_sexp s |> [%of_sexp : Bench.t]) in
  let open Let_syntax in
  basic ~summary:"Generate a sample database for a benchmark."
    (let%map_open verbose =
       flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
     and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
     and db_in = flag "i" (required string) ~doc:"the input database"
     and db_out = flag "o" (required string) ~doc:"the output database"
     and sample = flag "n" (required int) ~doc:"the sample size for each relation"
     and seed = flag "s" (optional int) ~doc:"the random seed"
     and bench = anon ("bench" %: bench) in
     fun () ->
       if verbose then Logs.set_level (Some Logs.Debug)
       else if quiet then Logs.set_level (Some Logs.Error)
       else Logs.set_level (Some Logs.Info) ;
       main ?seed ~db_in ~db_out ~sample bench)
  |> run
