open Core
open Stdio

let main project_root workspace_root =
  let project_root = Filename.realpath project_root in
  let workspace_root = Filename.realpath workspace_root in
  let llvm_root =
    Unix.open_process_in "llvm-config --obj-root"
    |> In_channel.input_all |> String.strip |> Filename.realpath
  in
  printf
    {|
    let project_root = "%s"
    let workspace_root = "%s"
    let llvm_root = "%s"
|}
    project_root workspace_root llvm_root

let () =
  let open Command in
  let open Let_syntax in
  basic ~summary:"Generate a config file."
    (let%map_open project_root = anon ("project_root" %: string)
     and workspace_root = anon ("workspace_root" %: string) in
     fun () -> main project_root workspace_root)
  |> run
