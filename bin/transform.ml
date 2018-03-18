open Core
open Dblayout
open Postgresql
open Collections

type transform = (string * int option)

let main : ?sample:int -> ?transforms:transform list -> ?output:string -> db:string -> Bench.t -> unit =
  fun ?sample ?(transforms = []) ?output ~db { name; params; sql; query } ->
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

    let candidates = List.fold_left transforms ~init:[ralgebra] ~f:(fun rs (t, i) ->
        let tf = Transform.of_name t |> Or_error.ok_exn in
        let rs' = List.concat_map rs ~f:(fun r ->
            let r' = Transform.(run_checked (compose required tf) r) in
            match i with
            | Some idx -> [List.nth_exn r' idx]
            | None -> r')
        in
        rs')
    in

    List.iteri candidates ~f:(fun i r ->
        printf "Candidate #%d:\n%s\n\n" i (Ralgebra.to_string r));

    match output, candidates with
    | Some f, [r] ->
      let cand = Ralgebra.Binable.of_ralgebra r in
      let size =
        Ralgebra.Binable.bin_size_t cand + Bin_prot.Utils.size_header_length
      in
      let buf = Bigstring.create size in
      Bigstring.write_bin_prot buf Ralgebra.Binable.bin_writer_t cand |> ignore;
      let fd = Unix.openfile ~mode:[O_RDWR; O_CREAT] f in
      Bigstring.really_write fd buf;
      Unix.close fd
    | Some _, [] -> Error.of_string "No candidates to output." |> Error.raise
    | Some _, _ -> Error.of_string "More than one candidate to output." |> Error.raise
    | None, _ -> ()

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ());

  let open Command in
  let bench = Arg_type.create (fun s -> Sexp.load_sexp s |> [%of_sexp:Bench.t]) in
  let transform = Arg_type.create (fun s ->  match String.split s ~on:':' with
      | [] -> Error.of_string "Unexpected empty string." |> Error.raise
      | [t] -> (t, None)
      | [t; i] -> (t, Some (Int.of_string i))
      | _ -> Error.create "Malformed transform." s [%sexp_of:string] |> Error.raise)
  in
  let open Let_syntax in
  basic ~summary:"Explore transformations by hand." [%map_open
    let db = flag "db" (required string) ~doc:"the database to connect to"
    and transforms = flag "transform" ~aliases:["t"]
        (optional (Arg_type.comma_separated transform)) ~doc:"transforms to run"
    and verbose = flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
    and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
    and sample = flag "sample" ~aliases:["s"] (optional int) ~doc:"the number of rows to sample from large tables"
    and output = flag "output" ~aliases:["o"] (optional string) ~doc:"where to write the final expression"
    and bench = anon ("bench" %: bench)
    in fun () ->
      if verbose then Logs.set_level (Some Logs.Debug)
      else if quiet then Logs.set_level (Some Logs.Error)
      else Logs.set_level (Some Logs.Info);

      main ?sample ?transforms ?output ~db bench
  ] |> run
