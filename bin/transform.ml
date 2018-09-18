open Core
open Dblayout
open Postgresql
open Collections

type transform = string * int option

let main ?(debug = false) ?sample:_ ?(transforms = []) ~db query_str =
  (* (\* FIXME: Use the first parameter value for test params. Should use multiple
   *      choices and average. *\)
   * let test_params =
   *   List.map params ~f:(fun (pname, values) ->
   *       match values with
   *       | [] ->
   *           Error.create "Empty parameter list." (name, pname)
   *             [%sexp_of: string * string]
   *           |> Error.raise
   *       | v :: _ -> (pname, v) )
   * in *)
  let module Config = struct
    let conn = new connection ~dbname:db ()

    (* let testctx = Layout.PredCtx.of_vars test_params *)

    let check_transforms = debug
  end in
  let module E = Eval.Make (Config) in
  let module M = Abslayout_db.Make (E) in
  let module Transform = Transform.Make (Config) (M) () in
  let query = Abslayout.of_string_exn query_str in
  (* If we need to sample, generate sample tables and swap them in the
       expression. *)
  (* ( match sample with
   * | Some s ->
   *     Ralgebra.relations cand.ralgebra
   *     |> List.iter ~f:(Db.Relation.sample Config.conn s)
   * | None -> () ) ; *)
  let candidates =
    List.fold_left transforms ~init:[query] ~f:(fun rs (t, i) ->
        let tf = Transform.of_name t |> Or_error.ok_exn in
        List.concat_map rs ~f:(fun r ->
            let r' = Transform.run tf r in
            match i with Some idx -> [List.nth_exn r' idx] | None -> r' ) )
  in
  List.iteri candidates ~f:(fun i r ->
      Format.eprintf "Candidate #%d:\n" i ;
      Format.eprintf "%a\n" Abslayout.pp r ;
      Format.(pp_print_newline err_formatter ()) ) ;
  match candidates with
  | [] -> Error.of_string "No candidates to output." |> Error.raise
  | [r] -> Format.printf "%a" Abslayout.pp r
  | _ :: _ -> Error.of_string "More than one candidate to output." |> Error.raise

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ()) ;
  let open Command in
  let transform =
    Arg_type.create (fun s ->
        match String.split s ~on:':' with
        | [] -> Error.of_string "Unexpected empty string." |> Error.raise
        | [t] -> (t, None)
        | [t; i] -> (t, Some (Int.of_string i))
        | _ ->
            Error.create "Malformed transform." s [%sexp_of: string] |> Error.raise
    )
  in
  let channel = Arg_type.create In_channel.create in
  let open Let_syntax in
  basic ~summary:"Explore transformations by hand."
    (let%map_open db =
       flag "db" (required string) ~doc:"DB the database to connect to"
     and transforms =
       flag "transform" ~aliases:["t"]
         (optional (Arg_type.comma_separated transform))
         ~doc:"transforms to run"
     and verbose = flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
     and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
     and sample =
       flag "sample" ~aliases:["s"] (optional int)
         ~doc:"N the number of rows to sample from large tables"
     and debug =
       flag "debug" ~aliases:["g"] no_arg
         ~doc:"turn on error checking for transforms"
     and ch = anon (maybe_with_default In_channel.stdin ("query" %: channel)) in
     fun () ->
       if verbose then Logs.set_level (Some Logs.Debug)
       else if quiet then Logs.set_level (Some Logs.Error)
       else Logs.set_level (Some Logs.Info) ;
       let query = In_channel.input_all ch in
       main ~debug ?sample ?transforms ~db query)
  |> run
