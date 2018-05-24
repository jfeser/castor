{
open Base
open Ralgebra0
open Ralgebra_parser

let error lexbuf msg =
  let pos = Lexing.lexeme_start_p lexbuf in
  let col = pos.pos_cnum - pos.pos_bol in
  raise (ParseError (msg, pos.pos_lnum, col))
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
  | "Project"  { PROJECT }
  | "Select"   { SELECT }
  | "Dedup"    { DEDUP }
  | "Filter"   { FILTER }
  | "EqJoin"   { EQJOIN }
  | "Concat"   { CONCAT }
  | "Count"    { COUNT }
  | "Zip"      { ZIP }
  | "Cross"    { CROSS }
  | "AEmpty"   { AEMPTY }
  | "ATuple"   { ATUPLE }
  | "AList"    { ALIST }
  | "AOrderedIdx" { AORDEREDIDX }
  | "AHashIdx" { AHASHIDX }
  | "AScalar"  { ASCALAR }
  | "Agg"      { AGG }
  | "Min"      { MIN }
  | "Max"      { MAX }
  | "Avg"      { AVG }
  | "Sum"      { SUM }
  | "int"      { INT_TYPE }
  | "bool"     { BOOL_TYPE }
  | "string"   { STRING_TYPE }
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
  | "true"     { BOOL true }
  | "false"    { BOOL false }
  | "false"    { BOOL false }
  | '"'        { STR (string (Buffer.create 10) lexbuf) }
  | id as text { ID text }
  | eof        { EOF }
  | _          { error lexbuf "unexpected character" }
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
  | eof { error lexbuf "eof in string literal" }
  | _ { error lexbuf "unexpected character" }
