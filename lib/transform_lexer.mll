{
open Transform_parser
open Parser_utils
}

let white = [' ' '\t' '\r']+
let id = ['a'-'z' 'A'-'Z' '0'-'9' '.' '"' '_' '-']+

rule token = parse
  | white      { token lexbuf }
  | "("        { LPAREN }
  | ")"        { RPAREN }
  | ":"        { COLON }
  | ","        { COMMA }
  | id as x    { ID x }
  | eof        { EOF }
  | _          { lex_error lexbuf "unexpected character" }
