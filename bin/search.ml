open Core
open Async
open Printf
open Postgresql
open Sexplib

open Dblayout
open Collections

let ensure_dir : string -> unit Deferred.t = fun dir -> 
  match%bind Sys.file_exists dir with
  | `No -> try_with (fun () -> Unix.mkdir dir) |> Deferred.ignore
  | _ -> return ()

let in_dir : string -> f:(unit -> 'a Deferred.t) -> 'a Deferred.t = fun dir ~f ->
  let%bind cur_dir = Unix.getcwd () in
  ensure_dir dir >>= fun () -> Unix.chdir dir >>= fun () ->
  match%bind try_with f with
  | Ok x -> Unix.chdir cur_dir >>| fun () -> x
  | Error e -> Unix.chdir cur_dir >>| fun () -> raise e

let validate : Ralgebra.t -> bool = fun r ->
  (* Check that there are no relation references. *)
  let has_refs = List.length (Ralgebra.relations r) > 0 in

  (* Check that all the layouts type check. *)
  let types_check = List.for_all (Ralgebra.layouts r) ~f:(fun l ->
      try Type.of_layout_exn l |> ignore; true with Type.TypeError _ -> false)
  in

  not has_refs && types_check

let main : ?num:int -> ?sample:int -> queue:string -> db:string -> Bench.t -> string -> unit Deferred.t =
  fun ?num ?sample ~queue ~db { name; sql; query; params } dir ->
    (* FIXME: Use the first parameter value for test params. Should use multiple
       choices and average. *)
    let test_params = List.map params ~f:(fun (pname, values) ->
        match values with
        | [] -> Error.create "Empty parameter list." (name, pname)
                  [%sexp_of:string * string] |> Error.raise
        | v::_ -> (pname, v))
    in

    (* Create search queue. *)
    let qconn = new connection ~dbname:"benchmarks" () in
    let () = try
      Db.exec qconn ~params:[name]
        "create table if not exists $0 (like template including all)" |> ignore
      with _ -> ()
    in

    let serialize : Candidate.t -> unit Or_error.t Deferred.t = fun cand ->
      let open Deferred.Or_error in
      let open Let_syntax in
      let open Candidate.Binable in
      let cand_bin = of_candidate cand in

      (* Update the search queue. *)
      let is_valid = validate cand.ralgebra |> Bool.to_string in
      let hash = Ralgebra.Binable.hash cand_bin.ralgebra |> Int.to_string in
      let size = bin_size_t cand_bin |> Int.to_string in
      let tf_string = Candidate.transforms_to_string cand.transforms in

      Logs.debug (fun m -> m "Serializing %s." hash);
      Logs.info (fun m -> m "'%s'" tf_string);

      let%bind do_write =
        let params = [queue; hash; size; is_valid; tf_string] in
        let ret = qconn#exec
            (Db.subst_params params
               "insert into $0 (hash,program_size,valid,search_state,search_transforms) values ($1,$2,$3,'searching','$4')")
        in
        match ret#status with
        | Command_ok -> return true
        | Fatal_error ->
          begin try match ret#error_code with
            | UNIQUE_VIOLATION -> return false
            | e ->
              let e = Error_code.to_string e in
              fail (Error.create "Postgres error" e [%sexp_of:string])
            with Failure _ -> return true
          end
        | e ->
          let e = result_status e in
          fail (Error.create "Postgres error" e [%sexp_of:string])

      in

      if do_write then
        Logs.debug (fun m -> m "Won the race to write %s." hash)
      else
        Logs.debug (fun m -> m "Lost the race to write %s." hash);

      (* Write out the candidate. *)
      let fn = sprintf "%s.bin" hash in
      let%map () = if do_write then Writer.with_file fn ~f:(fun w ->
          Writer.write_bin_prot w bin_writer_t cand_bin |> return)
        else return ()
      in

      (* Unlock the candidate in the search queue. *)
      if do_write then
        Db.exec qconn ~params:[queue; hash]
          "update $0 set search_state = 'unsearched' where hash = $1"
        |> Pervasives.ignore
    in

    let deserialize : string -> Candidate.t Or_error.t Deferred.t = fun hash ->
      let open Candidate.Binable in
      let fn = sprintf "%s.bin" hash in
      let%bind reader = Reader.open_file fn in
      try_with (fun () ->
          match%map Reader.read_bin_prot reader bin_reader_t with
          | `Ok r -> Result.Ok (to_candidate r)
          | `Eof -> Result.Error (Error.create "EOF when reading." fn [%sexp_of:string]))
        >>| Result.map_error ~f:(fun e -> Error.(of_exn e |> tag ~tag:fn))
        >>| Result.join
    in

    let with_candidate : (string -> unit Or_error.t Deferred.t) -> unit Deferred.t = fun f ->
      (* Grab a candidate from the frontier. *)
      Db.exec qconn "begin" |> ignore;
      let hash = Db.exec1 qconn ~params:[queue]
          "select hash from $0 where search_state = 'unsearched' order by search_time asc limit 1"
      in
      match hash with
      | [] -> Db.exec qconn "end" |> ignore; Clock.after (Time.Span.of_int_sec 3)
      | [hash] ->
        Db.exec qconn ~params:[queue; hash]
          "update $0 set search_state = 'searching' where hash = $1; end" |> ignore;
        let%map result = f hash in
        Db.exec qconn ~params:[queue; hash; Bool.to_string (Or_error.is_error result)]
          "update $0 set (search_state, search_failed) = ('searched', $2) where hash = $1"
        |> ignore
      | r -> Error.create "Unexpected db results." r [%sexp_of:string list] |> Error.raise
    in

    let is_done : unit -> bool = fun () ->
      match num with
      | Some n ->
        begin match Db.exec1 qconn ~params:[queue] "select count(*) from $0 where valid" with
          | [ct] -> Int.of_string ct >= n
          | _ -> false
        end
      | None -> false
    in

    let module Config = struct
      let conn = new connection ~dbname:db ()
      let testctx = Layout.PredCtx.of_vars test_params
      let check_transforms = false
    end in

    let module Transform = Transform.Make(Config) in
    let module Candidate = Candidate.Make(Config) in

    let search : unit -> _ = fun () ->
      (* Grab a candidate from the frontier. *)
      let%map () = with_candidate (fun hash ->
          let module List = Deferred.List in
          let open Deferred.Or_error in
          deserialize hash >>= fun r ->
          Logs.info (fun m -> m "Transforming candidate %s." hash);

          (* Generate children of candidate and write to frontier. *)
          List.iter Transform.transforms ~f:(fun t ->
              let tf = {Transform.(compose required t) with name = t.name} in
              List.iter (Candidate.run tf r) ~f:serialize))
      in
      if is_done () then `Finished () else `Repeat ()
    in

    let cand = Candidate.({
        ralgebra = Ralgebra.of_string_exn query |> Ralgebra.resolve Config.conn;
        transforms = [];
      }) in

    (* If we need to sample, generate sample tables and swap them in the
       expression. *)
    begin match sample with
      | Some s ->
        Ralgebra.relations cand.ralgebra
        |> List.iter ~f:(Db.Relation.sample Config.conn s);
      | None -> ()
    end;

    ensure_dir dir >>= fun () ->
    in_dir dir ~f:(fun () ->
        let%bind ret = serialize cand in
        begin match ret with
          | Ok () -> ()
          | Error e -> Logs.err (fun m -> m "%s" (Error.to_string_hum e))
        end;
        Deferred.repeat_until_finished () search)

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ());

  let open Command in
  let bench = Arg_type.create (fun s -> Sexp.load_sexp s |> [%of_sexp:Bench.t]) in
  let open Let_syntax in
  async ~summary:"Generate candidates from a benchmark file." [%map_open
    let db = flag "d" (required string) ~doc:"DB the database to connect to"
    and queue = flag "b" (required string) ~doc:"TABLE the name of the benchmark table"
    and verbose = flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
    and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
    and sample = flag "sample" ~aliases:["s"] (optional int) ~doc:"N the number of rows to sample from large tables"
    and num = flag "num" ~aliases:["n"] (optional int) ~doc:"N the number of candidates to enumerate"
    and bench = anon ("bench" %: bench)
    and dir = anon ("dir" %: file)
    in fun () ->
      if verbose then Logs.set_level (Some Logs.Debug)
      else if quiet then Logs.set_level (Some Logs.Error)
      else Logs.set_level (Some Logs.Info);

      main ?sample ~db ~queue bench dir
  ] |> run
