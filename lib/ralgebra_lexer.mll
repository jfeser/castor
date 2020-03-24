{

open Ralgebra_parser
open Parser_utils

module A = Ast

let keyword_tbl = Hashtbl.of_alist_exn (module String) [
"query", QUERY;
"select", SELECT;
"dedup", DEDUP;
"filter", FILTER;
"depjoin", DEPJOIN;
"join", JOIN;
"count", COUNT;
"zip", KIND A.Zip;
"cross", KIND A.Cross;
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
"int", PRIMTYPE (Prim_type.int_t);
"bool", PRIMTYPE (Prim_type.bool_t);
"string", PRIMTYPE (Prim_type.string_t);
"fixed", PRIMTYPE (Prim_type.fixed_t);
"as", AS;
"null", NULL;
"true", BOOL true;
"false", BOOL false;
"asc", ORDER Asc;
"desc", ORDER Desc;
"orderby", ORDERBY;
"if", IF;
"then", THEN;
"else", ELSE;
"month", MONTH A.Unop.Month;
"day", DAY A.Unop.Day;
"year", YEAR A.Unop.Year;
"date", DATEKW;
"exists", EXISTS;
"not", NOT A.Unop.Not;
"substring", SUBSTRING;
"strpos", STRPOS A.Binop.Strpos;
"strlen", STRLEN A.Unop.Strlen;
"concat", KIND A.Concat;
"to_year", EXTRACTY A.Unop.ExtractY;
"to_mon", EXTRACTM A.Unop.ExtractM;
"to_day", EXTRACTD A.Unop.ExtractD;
"row_number", ROW_NUMBER;
"range", RANGE;
  ]
}

let white = [' ' '\t' '\r']+
let alpha = ['a'-'z' 'A'-'Z']
let digit = ['0'-'9']
let id = (alpha | '_') (alpha | digit | '_')*
let int = '-'? digit+
let fixed = '-'? digit+ ('.' digit+)?
let str = '"'

rule token = parse
  | '\n'       { Lexing.new_line lexbuf; token lexbuf }
  | white      { token lexbuf }
  | "("        { LPAREN }
  | ")"        { RPAREN }
  | "["        { LSBRAC }
  | "]"        { RSBRAC }
  | "{"        { LCURLY }
  | "}"        { RCURLY }
  | ":"        { COLON }
  | "."        { DOT }
  | ","        { COMMA }
  | "<="       { LE A.Binop.Le }
  | ">="       { GE A.Binop.Ge }
  | "<"        { LT A.Binop.Lt }
  | ">"        { GT A.Binop.Gt }
  | "="        { EQ A.Binop.Eq }
  | "&&"       { AND A.Binop.And }
  | "||"       { OR A.Binop.Or }
  | "+"        { ADD A.Binop.Add }
  | "-"        { SUB A.Binop.Sub }
  | "*"        { MUL A.Binop.Mul }
  | "/"        { DIV A.Binop.Div }
  | "%"        { MOD A.Binop.Mod }
  | int as x   { INT (Int.of_string x) }
  | fixed as x { FIXED (Fixed_point.of_string x) }
  | '"'        { STR (string (Buffer.create 10) lexbuf) }
  | '#'        { comment lexbuf }
  | id as x    {
      match Hashtbl.find keyword_tbl (String.lowercase x) with
        | Some t -> t
        | None -> ID x
    }
  | eof        { EOF }
  | _          { lex_error lexbuf (sprintf "unexpected character '%c'"
                                     (Lexing.lexeme_char lexbuf
                                        (Lexing.lexeme_start lexbuf)) ) }
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
  | _ { lex_error lexbuf (sprintf "unexpected character '%c'"
                                     (Lexing.lexeme_char lexbuf
                                        (Lexing.lexeme_start lexbuf)) ) }
