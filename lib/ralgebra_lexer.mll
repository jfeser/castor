{
open Base
open Ralgebra_parser

let syntax_error lexbuf =
  Error.(create "Unexpected character." (Lexing.lexeme lexbuf) [%sexp_of:string]
         |> raise)
}

let white = [' ' '\t' '\n' '\r']+
let id = ['a'-'z' 'A'-'Z' '_'] ['a'-'z' 'A'-'Z' '0'-'9' '_' ''']*

rule token = parse
  | white      { token lexbuf }
  | "Project"  { PROJECT }
  | "Filter"   { FILTER }
  | "EqJoin"   { EQJOIN }
  | "Concat"   { CONCAT }
  | "int"      { INT }
  | "bool"     { BOOL }
  | "string"   { STRING }
  | "("        { LPAREN }
  | ")"        { RPAREN }
  | "["        { LSBRAC }
  | "]"        { RSBRAC }
  | ":"        { COLON }
  | "."        { DOT }
  | ","        { COMMA }
  | "<="       { LE }
  | ">="       { GE }
  | "<"        { LT }
  | ">"        { GT }
  | "="        { EQ }
  | "&&"       { AND }
  | "||"       { OR }
  | id as text { ID text }
  | eof        { EOF }
  | _          { syntax_error lexbuf }
