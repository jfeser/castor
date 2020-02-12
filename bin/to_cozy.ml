open! Core
open Castor
open Abslayout_load
open Abslayout_visitors

let main conn params file =
  let bench_name = Filename.(basename file |> chop_extension) in
  let params = List.map params ~f:(fun (n, t) -> Name.create ~type_:t n) in
  let ralgebra =
    let params = Set.of_list (module Name) params in
    load_string ~params conn (In_channel.with_file file ~f:In_channel.input_all)
    |> map_meta (fun _ -> Meta.empty ())
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
