open Core
open Stdio
open Printf
open Postgresql
open Sexplib

open Dblayout
open Collections

let in_dir : string -> f:(unit -> 'a) -> 'a = fun dir ~f ->
  let cur_dir = Unix.getcwd () in
  Unix.chdir dir;
  let ret = try f () with e -> Unix.chdir cur_dir; raise e in
  Unix.chdir cur_dir; ret

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
    end in

    let module Transform = Transform.Make(Config) in
    let ralgebra =
      Ralgebra.of_string_exn query |> Ralgebra.resolve Config.conn
    in

            (* If we need to sample, generate sample tables and swap them in the
       expression. *)
    begin match sample with
      | Some s ->
          Ralgebra.relations ralgebra
          |> List.iter ~f:(Db.Relation.sample Config.conn s);
      | None -> ()
    end;


    let candidates = Transform.search ralgebra in

    in_dir dir ~f:(fun () -> Seq.iter candidates ~f:(fun cand ->
        let cand = Ralgebra.Binable.of_ralgebra cand in
        let hash = Ralgebra.Binable.hash cand in
        let fn = sprintf "%d.bin" hash in
        if Sys.file_exists fn = `No then
          let fd = Unix.openfile ~mode:[O_RDWR; O_CREAT] fn in
          let size =
            Ralgebra.Binable.bin_size_t cand + Bin_prot.Utils.size_header_length
          in
          let buf = Bigstring.create size in
          Bigstring.write_bin_prot buf Ralgebra.Binable.bin_writer_t cand |> ignore;
          Bigstring.really_write fd buf;
          Unix.close fd;
      ))

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
