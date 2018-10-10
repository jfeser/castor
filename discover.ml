open Core
open Stdio

let main kind project_root workspace_root =
  let project_root = Filename.realpath project_root in
  let workspace_root = Filename.realpath workspace_root in
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
  let formatter =
    match kind with
    | "ML" ->
        printf
          {|
    let project_root = "%s"
    let workspace_root = "%s"
    let llvm_root = "%s"
|}
    | "ENV" ->
        printf {|
[default]
project_root = %s
workspace_root = %s
llvm_root = %s
|}
    | _ -> failwith "Unexpected kind."
  in
  formatter project_root workspace_root llvm_root

let () =
  let open Command in
  let open Let_syntax in
  basic ~summary:"Generate a config file."
    (let%map_open kind = flag "kind" (required string) ~doc:"ML|ENV"
     and project_root = anon ("project_root" %: string)
     and workspace_root = anon ("workspace_root" %: string) in
     fun () -> main kind project_root workspace_root)
  |> run
