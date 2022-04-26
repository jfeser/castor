open! Core
open Castor
open Abslayout_load

let main params file =
  let conn = Db.create (Sys.getenv_exn "CASTOR_DB") in
  let bench_name = Filename.(basename file |> chop_extension) in
  let params = List.map params ~f:(fun (n, t) -> Name.create ~type_:t n) in
  let ralgebra =
    let params = Set.of_list (module Name) params in
    load_string_exn ~params conn
      (In_channel.with_file file ~f:In_channel.input_all)
  in
  Cozy.to_string bench_name params ralgebra |> print_string

let spec =
  let open Command.Let_syntax in
  let%map_open () = Log.param
  and () = Db.param
  and params =
    flag "param" ~aliases:[ "p" ] (listed Util.param)
      ~doc:"NAME:TYPE query parameters"
  and file = anon ("file" %: string) in
  fun () -> main params file

let () =
  Command.basic spec
    ~summary:"Convert a relational algebra spec to a Cozy spec."
  |> Command_unix.run
