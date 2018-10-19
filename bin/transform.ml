open Core
open Dblayout
open Postgresql
open Collections
module A = Abslayout

let main ?(debug = false) ?sample:_ ?(transforms = "") ~db ~params query_str =
  let params =
    List.map params ~f:(fun (n, t) -> Name.create ~type_:t n)
    |> Set.of_list (module Name.Compare_no_type)
  in
  let module Config = struct
    let conn = new connection ~dbname:db ()

    let params = params

    let check_transforms = debug
  end in
  let module E = Eval.Make (Config) in
  let module M = Abslayout_db.Make (E) in
  let module Transform = Transform.Make (Config) (M) () in
  let query = Abslayout.of_string_exn query_str |> M.resolve ~params in
  let candidates =
    Transform.of_string_exn transforms
    |> List.fold_left ~init:[query] ~f:(fun rs tf ->
           List.concat_map rs ~f:tf.Transform.f )
  in
  List.iteri candidates ~f:(fun i r ->
      let r = M.annotate_schema r in
      Format.eprintf "Candidate #%d (serializable=%b):\n" i (A.is_serializeable r) ;
      Abslayout.pp Format.str_formatter r ;
      Format.eprintf "%s\n\n" (Format.flush_str_formatter ()) ;
      Out_channel.flush stderr ) ;
  match candidates with
  | [] -> Error.of_string "No candidates to output." |> Error.raise
  | [r] -> Format.printf "%a" Abslayout.pp r
  | _ :: _ -> Error.of_string "More than one candidate to output." |> Error.raise

let config_format () =
  Format.(pp_set_margin str_formatter) 80 ;
  Format.(pp_set_margin std_formatter) 80 ;
  Format.(pp_set_margin err_formatter) 80

let () =
  config_format () ;
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ()) ;
  let open Command in
  let open Let_syntax in
  basic ~summary:"Explore transformations by hand."
    (let%map_open db =
       flag "db" (required string) ~doc:"DB the database to connect to"
     and transforms =
       flag "transform" ~aliases:["t"] (optional string) ~doc:"transforms to run"
     and verbose = flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
     and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
     and sample =
       flag "sample" ~aliases:["s"] (optional int)
         ~doc:"N the number of rows to sample from large tables"
     and debug =
       flag "debug" ~aliases:["g"] no_arg
         ~doc:"turn on error checking for transforms"
     and params =
       flag "param" ~aliases:["p"] (listed Util.param)
         ~doc:"query parameters (passed as key:value)"
     and ch =
       anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
     in
     fun () ->
       if verbose then Logs.set_level (Some Logs.Debug)
       else if quiet then Logs.set_level (Some Logs.Error)
       else Logs.set_level (Some Logs.Info) ;
       let query = In_channel.input_all ch in
       Logs.info (fun m ->
           m "%s" (Sys.argv |> Array.to_list |> String.concat ~sep:" ") ) ;
       main ~debug ?sample ?transforms ~db ~params query)
  |> run
