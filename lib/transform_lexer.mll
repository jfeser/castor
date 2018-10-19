{
open Base
open Transform_parser
open Parser_utils
}

let white = [' ' '\t' '\r']+
let alpha = ['a'-'z' 'A'-'Z']
let digit = ['0'-'9']
let id = (alpha | '_') (alpha | digit | '_' | '.' | '-')*
let int = digit+

rule token = parse
  | white      { token lexbuf }
  | "("        { LPAREN }
  | ")"        { RPAREN }
  | ":"        { COLON }
  | ","        { COMMA }
  | int as x   { INT (Int.of_string x) }
  | id as x    { ID x }
  | eof        { EOF }
  | _          { lex_error lexbuf "unexpected character" }
