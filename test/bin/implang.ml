open Abslayout_load
open Castor_test.Test_util

let run_test ?(params = []) ?implang ?type_ ?opt () =
  let (module I), (module C) = Setup.make_modules ~code_only:true () in
  let conn = Lazy.force test_db_conn in
  let layout =
    load_stdin_nostrip_exn params conn
    |> Abslayout_fold.Data.annotate conn
    |> Type.annotate
  in

  let layout, len = Serialize.serialize "/tmp/buf" layout in
  let layout =
    V.map_meta
      (fun m ->
        object
          method pos = m#pos
          method type_ = m#type_
          method resolved = m#meta#meta#meta#resolved
        end)
      layout
  in
  let ir = I.irgen ~params ~len layout in

  Option.iter implang ~f:(fun fn ->
      Out_channel.with_file fn ~f:(fun ch ->
          Fmt.pf (Format.formatter_of_out_channel ch) "%a" I.pp ir));

  Option.iter opt ~f:(fun fn ->
      Out_channel.with_file fn ~f:(fun ch ->
          Fmt.pf (Format.formatter_of_out_channel ch) "%a" I.pp
          @@ Implang_opt.opt ir));

  Option.iter type_ ~f:(fun fn ->
      Out_channel.with_file fn ~f:(fun ch ->
          Fmt.pf
            (Format.formatter_of_out_channel ch)
            "%a" Sexp.pp_hum
            ([%sexp_of: Type.t] layout.meta#type_)))

let spec =
  let open Command in
  let open Let_syntax in
  let%map_open params =
    flag "param" ~aliases:[ "p" ] (listed Util.param)
      ~doc:"NAME:TYPE query parameters"
  and implang = flag "implang" (optional string) ~doc:"write intermediate"
  and opt = flag "opt" (optional string) ~doc:"write optimized intermediate"
  and type_ = flag "type" (optional string) ~doc:"write type" in
  run_test ~params ?implang ?type_ ?opt

let () =
  Command.basic ~summary:"Generate intermediate language." spec |> Command.run
