
{
  open Printf
  open Lexing
  open Sql_parser
  module T = Sql.Type

let error _ callerID =
  prerr_endline (sprintf "Lexer error : %s" callerID);
(*	update_pos buf;*)
	raise Parsing.Parse_error

let pos lexbuf = (lexeme_start lexbuf, lexeme_end lexbuf)

let advance_line_pos pos =
  { pos with pos_lnum = pos.pos_lnum + 1; pos_bol = pos.pos_cnum; }

let advance_line lexbuf =
  lexbuf.lex_curr_p <- advance_line_pos lexbuf.lex_curr_p

let keywords =
  let k = ref [
   "all",ALL;
   "and",AND;
   "any",ANY;
   "as",AS;
   "asc",ASC;
   "avg", AVG;
   "between",BETWEEN;
   "boolean",BOOLEAN;
   "by",BY;
   "case", CASE;
   "cast", CAST;
   "collate",COLLATE;
   "count", COUNT;
   "create",CREATE;
   "cross",CROSS;
   "date",DATE;
   "day", DAY;
   "days", DAY;
   "day_hour", DAY_HOUR;
   "day_microsecond", DAY_MICROSECOND;
   "day_minute", DAY_MINUTE;
   "day_second", DAY_SECOND;
   "desc",DESC;
   "distinct",DISTINCT;
   "div", DIV;
   "else", ELSE;
   "end",END;
   "except",EXCEPT;
   "exists",EXISTS;
   "false", FALSE;
   "for", FOR;
   "from",FROM;
   "group",GROUP;
   "having",HAVING;
   "hour", HOUR;
   "hour_microsecond", HOUR_MICROSECOND;
   "hour_minute", HOUR_MINUTE;
   "hour_second", HOUR_SECOND;
   "if",IF;
   "in",IN;
   "integer", INTEGER;
   "intersect",INTERSECT;
   "interval", INTERVAL;
   "is", IS;
   "join",JOIN;
   "key", KEY;
   "like", LIKE;
   "limit",LIMIT;
   "max", MAX;
   "microsecond", MICROSECOND;
   "min", MIN;
   "minute", MINUTE;
   "minute_microsecond", MINUTE_MICROSECOND;
   "minute_second", MINUTE_SECOND;
   "mod", MOD;
   "month", MONTH;
   "natural",NATURAL;
   "not",NOT;
   "null",NULL;
   "numeric", NUMERIC;
   "offset",OFFSET;
   "on",ON;
   "or",OR;
   "order",ORDER;
   "primary", PRIMARY;
   "quarter", QUARTER;
   "references", REFERENCES;
   "second", SECOND;
   "second_microsecond", SECOND_MICROSECOND;
   "select",SELECT;
   "some",SOME;
   "substr", SUBSTRING;
   "substring", SUBSTRING;
   "sum", SUM;
   "table", TABLE;
   "text", TEXT;
   "then", THEN;
   "time",TIME;
   "timestamp",TIMESTAMP;
   "true", TRUE;
   "union",UNION;
   "using",USING;
   "values",VALUES;
   "week", WEEK;
   "when", WHEN;
   "where",WHERE;
   "with", WITH;
   "year", YEAR;
   "years", YEAR;
   "year_month", YEAR_MONTH;
 ] in (* more *)
  let all token l = k := !k @ List.map (fun x -> x,token) l in
  all JOIN_TYPE1 ["left";"right";"full"];
  all JOIN_TYPE2 ["inner";"outer"];
  all LIKE_OP ["glob";"regexp";"match"];
  !k

(*
  Q: Why not convert all input to lowercase before lexing?
  A: Sometimes SQL is case-sensitive, also string contents should be preserved
*)

module Keywords = Map.Make(String)

let keywords =
  let add map (k,v) =
    let k = String.lowercase_ascii k in
    if Keywords.mem k map then
      failwith (sprintf "Lexeme %s is already associated with keyword." k)
    else
      Keywords.add k v map
  in
  List.fold_left add Keywords.empty keywords

(* FIXME case sensitivity??! *)

let get_ident str =
  let str = String.lowercase_ascii str in
  try Keywords.find str keywords with Not_found -> IDENT str

let ident str = IDENT (String.lowercase_ascii str)

let as_literal ch s =
  let s =
    String.to_seq s
    |> Seq.flat_map (fun x ->
        String.make (if x = ch then 2 else 1) x |> String.to_seq)
    |> String.of_seq
  in
  sprintf "%c%s%c" ch s ch
}

let digit = ['0'-'9']
let alpha = ['a'-'z' 'A'-'Z']
let ident = (alpha) (alpha | digit | '_' )*
let wsp = [' ' '\r' '\t']
let cmnt = "--" | "//" | "#"

(* extract separate statements *)
rule ruleStatement = parse
  | ['\n' ' ' '\r' '\t']+ as tok { `Space tok }
  | cmnt wsp* "[sqlgg]" wsp+ (ident+ as n) wsp* "=" wsp* ([^'\n']* as v) '\n' { `Prop (n,v) }
  | cmnt wsp* "@" (ident+ as name) [^'\n']* '\n' { `Prop ("name",name) }
  | '"' { let s = ruleInQuotes "" lexbuf in `Token (as_literal '"' s) }
  | "'" { let s = ruleInSingleQuotes "" lexbuf in `Token (as_literal '\'' s) }
  | "$" (ident? as tag) "$" { let s = ruleInDollarQuotes tag "" lexbuf in `Token (sprintf "$%s$%s$%s$" tag s tag) }
  | cmnt as s { `Comment (s ^ ruleComment "" lexbuf) }
  | "/*" { `Comment ("/*" ^ ruleCommentMulti "" lexbuf ^ "*/") }
  | ';' { `Semicolon }
  | [^ ';'] as c { `Char c }
  | eof { `Eof }
and
(* extract tail of the input *)
ruleTail acc = parse
  | eof { acc }
  | _* as str { ruleTail (acc ^ str) lexbuf }
and
ruleMain = parse
  | wsp   { ruleMain lexbuf }
  (* update line number *)
  | '\n'  { advance_line lexbuf; ruleMain lexbuf}

  | ';'   { SEMI }
  | '('		{ LPAREN }
  | ')'		{ RPAREN }
  | ','   { COMMA }
  | '.'   { DOT }
  | '{'   { LCURLY (lexeme_start lexbuf) }
  | '}'   { RCURLY (lexeme_start lexbuf) }

  | cmnt { ignore (ruleComment "" lexbuf); ruleMain lexbuf }
  | "/*" { ignore (ruleCommentMulti "" lexbuf); ruleMain lexbuf }

  | "*" { ASTERISK }
  | "=" { EQUAL }
  | "~" { TILDE }
  | "||" { CONCAT_OP }
  | "+" { PLUS }
  | "-" { MINUS }

  | "/" { DIV }
  | "%" { MOD }
  | "<<" { LSH }
  | ">>" { RSH }
  | "|" { BIT_OR }
  | "&" { BIT_AND }
  | ">" { GT }
  | ">=" { GE }
  | "<=" { LE }
  | "<" { LT }
  | "<>" | "!=" { NEQ }
  | "==" { EQ }
  | "<=>" { NOT_DISTINCT_OP }

  | "?" { PARAM (None,pos lexbuf) }
  | [':' '@'] (ident as str) { PARAM (Some str,pos lexbuf) }

  | '"' { ident (ruleInQuotes "" lexbuf) }
  | "'" { TEXT_LIT (ruleInSingleQuotes "" lexbuf) }
  (* http://www.postgresql.org/docs/current/interactive/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING *)
  | "$" (ident? as tag) "$" { TEXT_LIT (ruleInDollarQuotes tag "" lexbuf) }
  | "`" { ident (ruleInBackQuotes "" lexbuf) }
  | "[" { ident (ruleInBrackets "" lexbuf) }
  | ['x' 'X'] "'" { BLOB (ruleInSingleQuotes "" lexbuf) }

  | ident as str { get_ident str }
  | digit+ as str { INT_LIT (int_of_string str) }
  | digit+ '.' digit+ as str { FLOAT_LIT (float_of_string str) }
  | eof		{ EOF }
  | _	{ error lexbuf "ruleMain" }
and
(* FIXME factor out all that ruleIn* rules *)
ruleInQuotes acc = parse
  | '"'	        { acc }
  | eof	        { error lexbuf "no terminating quote" }
  | '\n'        { advance_line lexbuf; error lexbuf "EOL before terminating quote" }
  | "\"\""      { ruleInQuotes (acc^"\"") lexbuf }
  | [^'"' '\n']+ as s { ruleInQuotes (acc^s) lexbuf }
  | _		{ error lexbuf "ruleInQuotes" }
and
ruleInBrackets acc = parse
  | ']'	        { acc }
  | eof	        { error lexbuf "no terminating bracket" }
  | '\n'        { advance_line lexbuf; error lexbuf "EOL before terminating bracket" }
(*   | "\"\""      { ruleInQuotes (acc ^ "\"") lexbuf } *)
  | [^']' '\n']+  { ruleInBrackets (acc ^ lexeme lexbuf) lexbuf }
  | _		{ error lexbuf "ruleInBrackets" }
and
ruleInSingleQuotes acc = parse
  | '\''	      { acc }
  | eof	        { error lexbuf "no terminating single quote" }
  | '\n'        { advance_line lexbuf; error lexbuf "EOL before terminating single quote" }
  | "''"        { ruleInSingleQuotes (acc ^ "'") lexbuf }
  | [^'\'' '\n']+  { ruleInSingleQuotes (acc ^ lexeme lexbuf) lexbuf }
  | _		{ error lexbuf "ruleInSingleQuotes" }
and
ruleInBackQuotes acc = parse
  | '`'	        { acc }
  | eof	        { error lexbuf "no terminating back quote" }
  | '\n'        { advance_line lexbuf; error lexbuf "EOL before terminating back quote" }
  | "``"        { ruleInBackQuotes (acc ^ "`") lexbuf }
  | [^'`' '\n']+  { ruleInBackQuotes (acc ^ lexeme lexbuf) lexbuf }
  | _		{ error lexbuf "ruleInBackQuotes" }
and
ruleInDollarQuotes tag acc = parse
  | "$" (ident? as tag_) "$" { if tag_ = tag then acc else ruleInDollarQuotes tag (acc ^ sprintf "$%s$" tag_) lexbuf }
  | eof	        { error lexbuf "no terminating dollar quote" }
  | '\n'        { advance_line lexbuf; ruleInDollarQuotes tag (acc ^ "\n") lexbuf }
  (* match one char at a time to make sure delimiter matches longer *)
  | [^'\n']     { ruleInDollarQuotes tag (acc ^ lexeme lexbuf) lexbuf }
  | _		{ error lexbuf "ruleInDollarQuotes" }
and
ruleComment acc = parse
  | '\n'	{ advance_line lexbuf; acc }
  | eof	        { acc }
  | [^'\n']+    { let s = lexeme lexbuf in ruleComment (acc ^ s) lexbuf; }
  | _		{ error lexbuf "ruleComment"; }
and
ruleCommentMulti acc = parse
  | '\n'	{ advance_line lexbuf; ruleCommentMulti (acc ^ "\n") lexbuf }
  | "*/"	{ acc }
  | "*"
  | [^'\n' '*']+    { let s = lexeme lexbuf in ruleCommentMulti (acc ^ s) lexbuf }
  | _	        { error lexbuf "ruleCommentMulti" }

{

  let parse_rule = ruleMain

}
