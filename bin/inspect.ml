open Core
open Dblayout

let main : query:string -> unit =
 fun ~query ->
  let cand = Util.deserialize query in
  print_endline (Ralgebra.to_string cand.Candidate.ralgebra)

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ());
  let open Command in
  let open Let_syntax in
  basic ~summary:"Compile a query."
    (let%map_open verbose =
       flag "verbose" ~aliases:[ "v" ] no_arg ~doc:"increase verbosity"
     and quiet = flag "quiet" ~aliases:[ "q" ] no_arg ~doc:"decrease verbosity"
     and query = anon ("query" %: file) in
     fun () ->
       if verbose then Logs.set_level (Some Logs.Debug)
       else if quiet then Logs.set_level (Some Logs.Error)
       else Logs.set_level (Some Logs.Info);
       main ~query)
  |> run
