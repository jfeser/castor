open! Base
open! Stdio
open Printf

exception Error of exn * (int * int * string * string)

let parse_buf_exn lexbuf =
  try Sql_parser.input Sql_lexer.parse_rule lexbuf
  with exn ->
    let curr = lexbuf.Lexing.lex_curr_p in
    let line = curr.Lexing.pos_lnum in
    let cnum = curr.Lexing.pos_cnum - curr.Lexing.pos_bol in
    let tok = Lexing.lexeme lexbuf in
    let tail = Sql_lexer.ruleTail "" lexbuf in
    raise (Error (exn, (line, cnum, tok, tail)))

let parse_buf lexbuf = try Some (parse_buf_exn lexbuf) with _ -> None

let parse_stdin () = parse_buf (Lexing.from_channel stdin)

let parse_string str =
  (*Error.log "Parsing : %s" str; *)
  parse_buf (Lexing.from_string str)

let parse_file filename =
  In_channel.(with_file filename ~f:input_all) |> parse_string

let parse_stmt stmt = parse_buf_exn (Lexing.from_string stmt)

type token =
  [ `Comment of string
  | `Token of string
  | `Char of char
  | `Space of string
  | `Prop of string * string
  | `Semicolon ]

let get_statements ch =
  let lexbuf = Lexing.from_channel ch in
  let tokens =
    Sequence.unfold ~init:() ~f:(fun () ->
        if lexbuf.Lexing.lex_eof_reached then None
        else
          match Sql_lexer.ruleStatement lexbuf with
          | `Eof -> None
          | #token as x -> Some (x, ()))
  in
  let extract () =
    let b = Buffer.create 1024 in
    let answer () = Buffer.contents b in
    let rec loop tokens smth =
      match Sequence.next tokens with
      | None -> if smth then Some (answer ()) else None
      | Some (x, tokens) -> (
          match x with
          | `Comment s ->
              ignore s;
              loop tokens smth (* do not include comments (option?) *)
          | `Char c ->
              Buffer.add_char b c;
              loop tokens true
          | `Space _ when not smth ->
              loop tokens smth (* drop leading whitespaces *)
          | `Token s | `Space s ->
              Buffer.add_string b s;
              loop tokens true
          | `Prop _ -> loop tokens smth
          | `Semicolon -> Some (answer ()) )
    in
    loop tokens false
  in
  Sequence.unfold ~init:() ~f:(fun () ->
      match extract () with
      | None -> None
      | Some sql -> Some (parse_stmt sql, ())
      | exception e -> failwith (sprintf "lexer failed (%s)" (Exn.to_string e)))
  |> Sequence.force_eagerly
