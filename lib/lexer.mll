{
open Parser

exception SyntaxError of string
let syntax_error lexbuf =
  raise (SyntaxError ("Unexpected character: " ^ (Lexing.lexeme lexbuf)))
}

let white = [' ' '\t' '\n' '\r']+
let id = ['a'-'z' 'A'-'Z' '_'] ['a'-'z' 'A'-'Z' '0'-'9' '_' ''']*

rule token = parse
  | white      { token lexbuf }
  | "["        { LBRACKET }
  | "]"        { RBRACKET }
  | "("        { LPAREN }
  | ")"        { RPAREN }
  | "|"        { BAR }
  | "<-"       { LARROW }
  | ","        { COMMA }
  | "*"        { STAR }
  | id as text { ID text }
  | eof        { EOF }
  | _          { syntax_error lexbuf }
