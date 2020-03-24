open! Core
open Castor

let main fn =
  Log.info (fun m -> m "Converting %s." fn);
  let cozy =
    In_channel.with_file fn ~f:(fun ch ->
        let lexbuf = Lexing.from_channel ch in
        Cozy_parser.query_eof Cozy_lexer.token lexbuf)
  in
  print_s ([%sexp_of: Cozy.query] cozy);
  print_s ([%sexp_of: Big_o.t] (Cozy.cost cozy))

let () =
  let open Command in
  let open Let_syntax in
  basic ~summary:""
    [%map_open
      let () = Log.param and file = anon ("file" %: string) in
      fun () -> main file]
  |> run
