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
"zip", KIND Abslayout0.Zip;
"cross", KIND Abslayout0.Cross;
"aempty", AEMPTY;
"atuple", ATUPLE;
"alist", ALIST;
"aorderedidx", AORDEREDIDX;
"ahashidx", AHASHIDX;
"ascalar", ASCALAR;
"groupby", GROUPBY;
"min", MIN;
"max", MAX;
"avg", AVG;
"sum", SUM;
"int", PRIMTYPE (Type0.PrimType.IntT {nullable=true});
"bool", PRIMTYPE (Type0.PrimType.BoolT {nullable=true});
"string", PRIMTYPE (Type0.PrimType.StringT {nullable=true});
"as", AS;
"null", NULL;
"true", BOOL true;
"false", BOOL false;
"asc", ORDER `Asc;
"desc", ORDER `Desc;
"orderby", ORDERBY;
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
  | "||"       { OR }
  | "+"        { ADD }
  | "-"        { SUB }
  | "*"        { MUL }
  | "/"        { DIV }
  | "%"        { MOD }
  | int as x   { INT (Int.of_string x) }
  | '"'        { STR (string (Buffer.create 10) lexbuf) }
  | '#'        { comment lexbuf }
  | id as x    {
      match Hashtbl.find keyword_tbl (String.lowercase x) with
        | Some t -> t
        | None -> ID x
    }
  | eof        { EOF }
  | _          { lex_error lexbuf "unexpected character" }
and comment = parse
  | '\n' { Lexing.new_line lexbuf; token lexbuf }
  | _ { comment lexbuf }
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
