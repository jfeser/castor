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

  if has_refs then
    Logs.info (fun m -> m "Discarding candidate: still has relation references.");

  (* Check that all the layouts type check. *)
  let types_check = List.for_all (Ralgebra.layouts r) ~f:(fun l ->
      try Type.of_layout_exn l |> ignore; true with Type.TypeError _ -> false)
  in

  if not types_check then
    Logs.info (fun m -> m "Discarding candidate: some layouts don't type check.");

  not has_refs && types_check

let main : ?num:int -> ?sample:int -> db:string -> Bench.t -> string -> unit Deferred.t =
  fun ?num ?sample ~db { name; sql; query; params } dir ->
    (* FIXME: Use the first parameter value for test params. Should use multiple
       choices and average. *)
    let test_params = List.map params ~f:(fun (pname, values) ->
        match values with
        | [] -> Error.create "Empty parameter list." (name, pname)
                  [%sexp_of:string * string] |> Error.raise
        | v::_ -> (pname, v))
    in

    let module Config = struct
      let conn = new connection ~dbname:db ()
      let testctx = Layout.PredCtx.of_vars test_params
      let check_transforms = false
    end in

    let module Transform = Transform.Make(Config) in
    let module Candidate = Candidate.Make(Config) in

    let serialize : Candidate.t -> unit Deferred.t = fun c ->
      let open Unix in

      let r = c.ralgebra in
      let c = Candidate.Binable.of_candidate c in
      let name = sprintf "%d.bin" (Ralgebra.Binable.hash c.ralgebra) in
      let fn = sprintf "frontier/%s" name in

      (* If the candidate exists on the frontier or in the collected set, don't add
         it.*)
      match%bind Sys.file_exists fn with
      | `Yes | `Unknown -> return ()
      | `No -> begin match%bind Sys.file_exists name with
          | `Yes | `Unknown -> return ()
          | `No -> begin match%bind Sys.file_exists (sprintf "dead/%s" name) with
              | `Yes | `Unknown -> return ()
              | `No ->
                let%bind () = Writer.with_file fn ~f:(fun w ->
                Writer.write_bin_prot w Candidate.Binable.bin_writer_t c |> return)
            in
            if validate r then link ~target:fn ~link_name:name () else return ()
            end
        end
    in

    let deserialize : string -> f:(Candidate.t -> 'a Deferred.t) -> 'a Deferred.t = fun fn ~f ->
      let open Unix in
      with_file ~exclusive:`Write fn ~mode:[`Rdwr] ~f:(fun fd ->
          let reader = Reader.create fd in
          try_with (fun () -> Reader.read_bin_prot reader Candidate.Binable.bin_reader_t) >>= function
          | Ok (`Ok x) -> f (Candidate.Binable.to_candidate x)
          | Ok `Eof -> Logs.err (fun m -> m "Reading candidate failed: %s" fn); return ()
          | Error e -> Logs.err (fun m -> m "Reading candidate failed: %s %s" fn (Exn.to_string e)); return ())
    in

    let search : unit -> _ = fun () ->
      let%bind should_stop = match num with
        | Some n ->
          let%map dir = Sys.readdir "." in
          Array.length dir >= n
        | None -> return false
      in
      if should_stop then return (`Finished ()) else
        (* Grab a candidate from the frontier. *)
        let%bind dir_entries = Sys.readdir "frontier" in
        if Array.length dir_entries = 0 then return (`Finished ()) else
          let name = dir_entries.(Random.int (Array.length dir_entries)) in
          let fn = (sprintf "frontier/%s" name) in
          let%bind () = deserialize fn ~f:(fun r ->
              Logs.info (fun m -> m "Transforming candidate %s." name);

              (* Move candidate out of frontier. *)
              try_with (fun () -> Unix.link ~force:true ~target:fn ~link_name:(sprintf "dead/%s" name) ())
                ~rest:`Log |> ignore;
              try_with (fun () -> Unix.unlink fn) ~rest:`Log |> ignore;

              (* Generate children of candidate and write to frontier. *)
              Deferred.List.iter Transform.transforms ~f:(fun t ->
                  let tf = Transform.(compose required t) in
                  Deferred.List.iter (Candidate.run tf r) ~f:(fun r' ->
                      serialize r')))
          in
          return (`Repeat ())
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
        let%bind () = ensure_dir "frontier" in
        let%bind () = ensure_dir "dead" in
        let%bind _ = serialize cand in
        Deferred.repeat_until_finished () search)

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ());

  let open Command in
  let bench = Arg_type.create (fun s -> Sexp.load_sexp s |> [%of_sexp:Bench.t]) in
  let open Let_syntax in
  async ~summary:"Generate candidates from a benchmark file." [%map_open
    let db = flag "db" (required string) ~doc:"the database to connect to"
    and verbose = flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
    and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
    and sample = flag "sample" ~aliases:["s"] (optional int) ~doc:"the number of rows to sample from large tables"
    and num = flag "num" ~aliases:["n"] (optional int) ~doc:"the number of candidates to enumerate"
    and bench = anon ("bench" %: bench)
    and dir = anon ("dir" %: file)
    in fun () ->
      if verbose then Logs.set_level (Some Logs.Debug)
      else if quiet then Logs.set_level (Some Logs.Error)
      else Logs.set_level (Some Logs.Info);

      main ?sample ~db bench dir
  ] |> run
