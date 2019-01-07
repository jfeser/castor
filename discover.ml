open Core
open Stdio
open Config_defaults

let main kind =
  let llvm_root =
    let configs = ["llvm-config"; "llvm-config-6.0"] in
    Option.value_exn ~message:"No LLVM root found."
      (List.find_map configs ~f:(fun c ->
           try
             Some
               ( Unix.open_process_in (sprintf "%s --prefix" c)
               |> In_channel.input_all |> String.strip |> Filename.realpath )
           with Unix.Unix_error _ -> None ))
  in
  let build_root = Filename.realpath (Sys.getcwd ()) in
  let formatter =
    match kind with
    | "ML" ->
        printf
          {|
    let build_root = "%s"
    let llvm_root = "%s"
    let tpch_db = "%s"
    let demomatch_db = "%s"
|}
    | "ENV" ->
        printf
          {|
[default]
build_root = %s
llvm_root = %s
tpch_db = %s
demomatch_db = %s
|}
    | _ -> failwith "Unexpected kind."
  in
  formatter build_root llvm_root tpch_db demomatch_db

let () =
  let open Command in
  let open Let_syntax in
  basic ~summary:"Generate a config file."
    (let%map_open kind = flag "kind" (required string) ~doc:"ML|ENV" in
     fun () -> main kind)
  |> run
