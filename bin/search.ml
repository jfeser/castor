open Core
open Stdio
open Printf
open Postgresql
open Sexplib

open Dblayout
open Collections

let in_dir : string -> f:(unit -> 'a) -> 'a = fun dir ~f ->
  let cur_dir = Unix.getcwd () in
  if Sys.file_exists dir = `No then Unix.mkdir dir;
  Unix.chdir dir;
  let ret = try f () with e -> Unix.chdir cur_dir; raise e in
  Unix.chdir cur_dir; ret

let finally : f:(unit -> 'a) -> (unit -> unit) -> 'a = fun ~f g ->
  try let ret = f () in g (); ret with e -> g (); raise e

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

let main : ?sample:int -> db:string -> Bench.t -> string -> unit =
  fun ?sample ~db { name; sql; query; params } dir ->
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

    let serialize : Candidate.t -> bool = fun c ->
      let open Unix in

      let r = c.ralgebra in
      let c = Candidate.Binable.of_candidate c in
      let name = sprintf "%d.bin" (Ralgebra.Binable.hash c.ralgebra) in
      let fn = sprintf "frontier/%s" name in

      (* If the candidate exists on the frontier or in the collected set, don't add
         it.*)
      if Sys.file_exists fn = `Yes || Sys.file_exists name = `Yes then false else
        let size =
          Candidate.Binable.bin_size_t c + Bin_prot.Utils.size_header_length
        in
        try
          let fd = openfile ~mode:[O_RDWR; O_CREAT; O_EXCL] fn in
          finally (fun () -> close fd) ~f:(fun () ->
              if flock fd Flock_command.lock_exclusive then begin
                finally (fun () ->
                    if not (flock fd Flock_command.unlock) then
                      Logs.warn (fun m -> m "Failed to release file lock on %s." fn))
                  ~f:(fun () ->
                      let buf = Bigstring.create size in
                      Bigstring.write_bin_prot buf Candidate.Binable.bin_writer_t c |> ignore;
                      Bigstring.really_write fd buf;

                      if validate r then link ~target:fn ~link_name:name ()); true
              end else false)
        with Unix_error _ -> false
    in

    let deserialize : string -> f:(Candidate.t -> 'a) -> 'a option = fun fn ~f ->
      let open Unix in
      try
        let fd = openfile ~mode:[O_RDWR] fn in
        finally (fun () -> close fd) ~f:(fun () ->
            if Unix.flock fd Unix.Flock_command.lock_exclusive then
              finally (fun () ->
                  if not (Unix.flock fd Unix.Flock_command.unlock) then
                    Logs.warn (fun m -> m "Failed to release file lock on %s." fn))
                ~f:(fun () ->
                    let size = (Native_file.stat fn).st_size in
                    let buf = Bigstring.create size in
                    Bigstring.really_read fd buf;
                    let r, _ = Bigstring.read_bin_prot buf Candidate.Binable.bin_reader_t
                               |> Or_error.ok_exn
                    in
                    Some (Candidate.Binable.to_candidate r |> f))
            else None)
      with Unix_error _ -> None
    in

    let rec search : unit -> unit = fun () ->
      (* Grab a candidate from the frontier. *)
      let dir_entries = Sys.readdir "frontier" in
      if Array.length dir_entries = 0 then () else
        let name = dir_entries.(Random.int (Array.length dir_entries)) in
        let fn = (sprintf "frontier/%s" name) in
        deserialize fn ~f:(fun r ->
            Logs.info (fun m -> m "Transforming candidate %s." name);

            (* Move candidate out of frontier. *)
            Unix.unlink fn;

            (* Generate children of candidate and write to frontier. *)
            List.iter Transform.transforms ~f:(fun t ->
                let tf = Transform.(compose required t) in
                List.iter (Candidate.run tf r) ~f:(fun r' ->
                    serialize r' |> ignore))
          ) |> ignore;
        search ()
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

    in_dir dir ~f:(fun () -> 
        if Sys.file_exists "frontier" = `No then Unix.mkdir "frontier";
        serialize cand |> ignore;
        search ())

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ());

  let open Command in
  let bench = Arg_type.create (fun s -> Sexp.load_sexp s |> [%of_sexp:Bench.t]) in
  let open Let_syntax in
  basic ~summary:"Generate candidates from a benchmark file." [%map_open
    let db = flag "db" (required string) ~doc:"the database to connect to"
    and verbose = flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
    and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
    and sample = flag "sample" ~aliases:["s"] (optional int) ~doc:"the number of rows to sample from large tables"
    and bench = anon ("bench" %: bench)
    and dir = anon ("dir" %: file)
    in fun () ->
      if verbose then Logs.set_level (Some Logs.Debug)
      else if quiet then Logs.set_level (Some Logs.Error)
      else Logs.set_level (Some Logs.Info);

      main ?sample ~db bench dir
  ] |> run
