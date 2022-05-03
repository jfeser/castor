open Castor_test.Test_util

let run_test ~params ~db ?parallel ?serial () =
  let conn = Db.create db in

  let layout = Abslayout_load.load_stdin_nostrip_exn params conn in

  Option.iter parallel ~f:(fun fn ->
      Out_channel.with_file fn ~f:(fun ch ->
          Type.Parallel.type_of conn layout
          |> Result.map_error ~f:(fun _ -> Failure "")
          |> Result.ok_exn |> [%sexp_of: Type.t] |> Sexp.output_hum ch));

  Option.iter serial ~f:(fun fn ->
      Out_channel.with_file fn ~f:(fun ch ->
          Abslayout_fold.Data.annotate conn layout
          |> Type.type_of |> [%sexp_of: Type.t] |> Sexp.output_hum ch))

let spec =
  let open Command in
  let open Let_syntax in
  let%map_open params =
    flag "param" ~aliases:[ "p" ] (listed Util.param)
      ~doc:"NAME:TYPE query parameters"
  and db = flag "db" (required string) ~doc:"database url"
  and parallel = flag "parallel" (optional string) ~doc:"write parallel type"
  and serial = flag "serial" (optional string) ~doc:"write serial type" in
  run_test ~params ~db ?parallel ?serial

let () = Command.basic ~summary:"Generate type." spec |> Command_unix.run
