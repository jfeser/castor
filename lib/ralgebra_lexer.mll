{
open Base
open Ralgebra_parser
open Parser_utils


let keyword_tbl = Hashtbl.of_alist_exn (module String) [
"select", SELECT;
"dedup", DEDUP;
"filter", FILTER;
"join", JOIN;
"count", COUNT;
"zip", ZIP;
"cross", CROSS;
"aempty", AEMPTY;
"atuple", ATUPLE;
"alist", ALIST;
"aorderedidx", AORDEREDIDX;
"ahashidx", AHASHIDX;
"ascalar", ASCALAR;
"agg", AGG;
"min", MIN;
"max", MAX;
"avg", AVG;
"sum", SUM;
"int", INT_TYPE;
"bool", BOOL_TYPE;
"string", STRING_TYPE;
"as", AS;
"null", NULL;
"true", BOOL true;
"false", BOOL false;
  ]
}

let white = [' ' '\t' '\r']+
let alpha = ['a'-'z' 'A'-'Z']
let digit = ['0'-'9']
let id = (alpha | '_') (alpha | digit | '_')*
let int = '-'? digit+
let str = '"'

rule token = parse
  | '\n'       { Lexing.new_line lexbuf; token lexbuf }
  | white      { token lexbuf }
  | "("        { LPAREN }
  | ")"        { RPAREN }
  | "["        { LSBRAC }
  | "]"        { RSBRAC }
  | ":"        { COLON }
  | "."        { DOT }
  | ","        { COMMA }
  | "->"       { RARROW }
  | "<="       { LE }
  | ">="       { GE }
  | "<"        { LT }
  | ">"        { GT }
  | "="        { EQ }
  | "&&"       { AND }
  | "+"        { ADD }
  | "-"        { SUB }
  | "*"        { MUL }
  | "/"        { DIV }
  | "%"        { MOD }
  | int as x   { INT (Int.of_string x) }
  | '"'        { STR (string (Buffer.create 10) lexbuf) }
  | id as x    {
      match Hashtbl.find keyword_tbl (String.lowercase x) with
        | Some t -> t
        | None -> ID x
    }
  | eof        { EOF }
  | _          { lex_error lexbuf "unexpected character" }
and string buf = parse
  | [^ '"' '\n' '\\']+ {
      Buffer.add_string buf (Lexing.lexeme lexbuf);
      string buf lexbuf
    }
  | '\n' {
      Lexing.new_line lexbuf;
      Buffer.add_string buf (Lexing.lexeme lexbuf);
      string buf lexbuf
    }
  | '\\' '"' { Buffer.add_char buf '"'; string buf lexbuf }
  | '\\' { Buffer.add_char buf '\\'; string buf lexbuf }
  | '"' { Buffer.contents buf }
  | eof { lex_error lexbuf "eof in string literal" }
  | _ { lex_error lexbuf "unexpected character" }
