open Core
open Stdio
open Dblayout

let main ~in_place file =
  let open Format in
  let ralgebra = In_channel.with_file file ~f:Abslayout.of_channel_exn in
  let write fmt = pp_set_max_indent fmt 80 ; Abslayout.pp fmt ralgebra in
  if in_place then
    Out_channel.with_file file ~f:(fun ch ->
        let fmt = Format.formatter_of_out_channel ch in
        write fmt )
  else write std_formatter

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ()) ;
  let open Command in
  let open Let_syntax in
  basic ~summary:"Format a relational algebra expression."
    (let%map_open file = anon ("file" %: file)
     and in_place = flag "i" no_arg ~doc:"write output in place" in
     fun () ->
       Logs.set_level (Some Logs.Info) ;
       main ~in_place file)
  |> run
