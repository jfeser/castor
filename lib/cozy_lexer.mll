{
open! Core
open Cozy_parser
open Parser_utils

let keyword_tbl = Hashtbl.of_alist_exn (module String) [
    "filter", FILTER;
    "map", MAP;
    "flatmap", FLATMAP;
    "distinct", DISTINCT;
    "the", THE;
    "len", LEN;
    "sum", SUM;
    "let", LET;
    "in", IN;
    "and", BINOP `And;
  ]
}

let white = [' ' '\t' '\r' '\n']+
let alpha = ['a'-'z' 'A'-'Z']
let digit = ['0'-'9']
let id = (alpha | '_') (alpha | digit | '_')*
let int = '-'? digit+

rule token = parse
  | white      { token lexbuf }
  | "("        { LPAREN }
  | ")"        { RPAREN }
  | "{"        { LCURLY }
  | "}"        { RCURLY }
  | "["        { LSQUARE }
  | "]"        { RSQUARE }
  | "->"       { ARROW }
  | ":"        { COLON }
  | "?"        { QUESTION }
  | "."        { DOT }
  | ","        { COMMA }
  | "="        { EQ }
  | "\""       { QUOTE }
  | "<="       { BINOP `Le }
  | ">="       { BINOP `Ge }
  | "<"        { BINOP `Lt }
  | ">"        { BINOP `Gt }
  | "=="       { BINOP `Eq }
  | "+"        { BINOP `Add }
  | "-"        { BINOP `Sub }
  | "*"        { BINOP `Mul }
  | "/"        { BINOP `Div }
  | int as x   { INT (Int.of_string x) }
  | id as x    {
      match Hashtbl.find keyword_tbl (String.lowercase x) with
        | Some t -> t
        | None -> ID x
    }
  | eof        { EOF }
  | _          { lex_error lexbuf (sprintf "unexpected character '%s'"
                                     (Lexing.lexeme lexbuf) ) }
