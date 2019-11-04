open! Core
open Castor

let main conn params file =
  let module CConfig = struct
    let conn = conn

    let simplify = None
  end in
  let module A = Abslayout_db.Make (CConfig) in
  let bench_name = Filename.(basename file |> chop_extension) in
  let params = List.map params ~f:(fun (n, t) -> Name.create ~type_:t n) in
  let ralgebra =
    let params = Set.of_list (module Name) params in
    A.load_string ~params (In_channel.with_file file ~f:In_channel.input_all)
  in
  Cozy.to_string bench_name params ralgebra |> print_string

let () =
  let open Command in
  let open Let_syntax in
  basic ~summary:"Convert a relational algebra spec to a Cozy spec."
    (let%map_open () = Log.param
     and db = Db.param
     and params =
       flag "param" ~aliases:[ "p" ] (listed Util.param)
         ~doc:"NAME:TYPE query parameters"
     and file = anon ("file" %: string) in
     fun () -> main db params file)
  |> run
