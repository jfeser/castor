open! Core

let main kind =
  let llvm_root =
    match Config_defaults.llvm_root with
    | Some r -> r
    | None ->
        let configs = ["llvm-config"; "llvm-config-6.0"] in
        let dir =
          List.find_map configs ~f:(fun c ->
              try
                Unix.open_process_in (sprintf "%s --prefix" c)
                |> In_channel.input_all |> String.strip |> Filename.realpath
                |> Option.some
              with Unix.Unix_error _ -> None)
        in
        Option.value_exn ~message:"No LLVM root found." dir
  in
  let build_root =
    match Config_defaults.build_root with
    | Some r -> r
    | None -> Filename.realpath (Sys.getcwd ())
  in
  let formatter, option_to_str =
    match kind with
    | "ML" ->
        let option_to_str = function
          | Some s -> sprintf "(Some \"%s\")" s
          | None -> "None"
        in
        ( printf
            {|
let build_root = "%s"
let llvm_root = "%s"
let tpch_db = %s
let demomatch_db = %s
let tpcds_db = %s
|}
        , option_to_str )
    | "ENV" ->
        let option_to_str = function Some s -> s | None -> "" in
        ( printf
            {|
[default]
build_root = %s
llvm_root = %s
tpch_db = %s
demomatch_db = %s
tpcds_db = %s
|}
        , option_to_str )
    | _ -> failwith "Unexpected kind."
  in
  formatter build_root llvm_root
    (option_to_str Config_defaults.tpch_db)
    (option_to_str Config_defaults.demomatch_db)
    (option_to_str Config_defaults.tpcds_db)

let () =
  let open Command in
  let open Let_syntax in
  basic ~summary:"Generate a config file."
    (let%map_open kind = flag "kind" (required string) ~doc:"ML|ENV" in
     fun () -> main kind)
  |> run
