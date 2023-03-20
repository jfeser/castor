#!/usr/bin/env ocaml

#load "unix.cma";;

let () =
  let sys_str =
    Unix.open_process_in "uname -s" |> input_line
    |> String.lowercase_ascii |> String.trim
  in
  let flags = match sys_str with
    | "darwin" -> "()"
    | "linux" -> "(-Wl,--no-as-needed)"
    | _ -> failwith "Unexpected system string " ^ sys_str
  in
  print_endline flags
;;
