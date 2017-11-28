open Base
open Base.Printf

include Expr0

exception ParseError of string

let of_string_exn : string -> t = fun s ->
  let lexbuf = Lexing.from_string s in
  let error s =
    sprintf "Parse error: %s (col: %d)" s (lexbuf.lex_curr_p.pos_cnum)
  in
  try Parser.expr_eof Lexer.token lexbuf with
  | Parser.Error -> raise (ParseError (error s))
  | Lexer.SyntaxError _ -> raise (ParseError (error s))
  | Caml.Parsing.Parse_error -> raise (ParseError (error s))
