open! Core
open Castor

let main queries =
  let conn = Db.create (Sys.getenv_exn "CASTOR_DB") in
  List.map queries ~f:(fun fn ->
      match In_channel.with_file fn ~f:Query.of_channel with
      | Ok q -> Query.annotate conn q
      | Error e ->
          failwith
          @@ Fmt.str "Failed to parse %s: %a" fn (Abslayout.pp_err Fmt.nop) e)
  |> Query.of_many
  |> Format.printf "%a" Query.pp

let () =
  let open Command.Let_syntax in
  Command.basic ~summary:"Fuse multiple queries into a single query."
    [%map_open
      let () = Log.param
      and () = Db.param
      and fns = anon @@ non_empty_sequence_as_list ("query" %: string) in
      fun () -> main fns]
  |> Command.run
