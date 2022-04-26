open! Core
open Castor

let main ~in_place file =
  let open Format in
  let ralgebra = In_channel.with_file file ~f:Abslayout.of_channel_exn in
  let write fmt =
    pp_set_max_indent fmt 80;
    Abslayout.pp fmt ralgebra
  in
  if in_place then
    Out_channel.with_file file ~f:(fun ch ->
        let fmt = Format.formatter_of_out_channel ch in
        write fmt)
  else write std_formatter

let spec =
  let open Command.Let_syntax in
  let%map_open file = anon ("file" %: string)
  and in_place = flag "i" no_arg ~doc:"write output in place" in
  fun () -> main ~in_place file

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ());
  Logs.set_level (Some Logs.Info);
  Command.basic spec ~summary:"Format a relational algebra expression."
  |> Command_unix.run
