/*
Simple SQL parser
*/


%{
    open Sql
    module Header = struct
      open Base

      (* preserve order *)
      let make_limit l =
        let param = function
          | _, `Const _ -> None
          | x, `Param (None,pos) -> Some ((Some (match x with `Limit -> "limit" | `Offset -> "offset"),pos), Type.Int)
          | _, `Param p -> Some (p, Type.Int)
        in
        List.filter_map ~f:param l, List.mem ~equal:Poly.(=) l (`Limit,`Const 1)

      let mk_not x = Fun (`Not, [x])

      let with_clauses = Base.Hashtbl.create (module String)

      let add_with n q =
        Base.Hashtbl.add_exn with_clauses ~key:n ~data:q

      let find_with n = Base.Hashtbl.find with_clauses n
    end
    include Header
%}

%token <int> INTEGER
%token <string> IDENT TEXT BLOB
%token <float> FLOAT
%token <Sql.param_id> PARAM
%token <int> LCURLY RCURLY
%token ALL AND ANY AS ASC ASTERISK AVG BETWEEN BIT_AND BIT_OR BY CASE CAST
COLLATE COMMA CONCAT_OP COUNT CROSS DATE DAY DAY_HOUR DAY_MICROSECOND DAY_MINUTE
DAY_SECOND DESC DISTINCT DIV DOT ELSE END EOF EQ EQUAL EXCEPT EXISTS FALSE FOR
FROM GE GROUP GT HAVING HOUR HOUR_MICROSECOND HOUR_MINUTE HOUR_SECOND IF IN
INTERSECT INTERVAL IS JOIN JOIN_TYPE1 JOIN_TYPE2 LE LIKE LIKE_OP LIMIT LPAREN
LSH LT MAX MICROSECOND MIN MINUS MINUTE MINUTE_MICROSECOND MINUTE_SECOND MOD
MONTH NATURAL NEQ NOT NOT_DISTINCT_OP NULL OFFSET ON OR ORDER PLUS QUARTER
RPAREN RSH SECOND SECOND_MICROSECOND SELECT SOME SUBSTRING SUM THEN TILDE TIME
TIMESTAMP TRUE UNION USING VALUES WEEK WHEN WHERE WITH YEAR YEAR_MONTH


%left OR CONCAT_OP
%left AND
%nonassoc NOT
%nonassoc BETWEEN CASE
%nonassoc EQUAL EQ NEQ NOT_DISTINCT_OP IS LIKE LIKE_OP IN
%nonassoc GT LT GE LE
%left BIT_OR
%left BIT_AND
%left LSH RSH
%left PLUS MINUS
%left ASTERISK MOD DIV
%nonassoc UNARY_MINUS TILDE
%nonassoc INTERVAL

%type <Sql.op Sql.expr> expr

%start <Sql.op Sql.query> input

%%

%inline either(X,Y): X | Y { }
%inline commas(X): l=separated_nonempty_list(COMMA,X) { l }
(* (x1,x2,...,xn) *)
%inline sequence_(X): LPAREN l=commas(X) { l }
%inline sequence(X): l=sequence_(X) RPAREN { l }

input: query EOF { $1 }

with_clause: WITH separated_nonempty_list(COMMA, with_body) {}
with_body:
  | id=IDENT AS LPAREN q=query RPAREN { add_with id q }

query:
  | with_clause? c=clauses o=loption(order) lim=limit_t?
    {
      { clauses=c; order=o; limit=lim; }
    }

clauses:
  | s=select { Clause (s, None) }
  | s=select op=compound_op c=clauses { Clause (s, Some (op, c)) }

select:
  | SELECT d=select_type r=commas(column1) f=from? w=where? g=loption(group) h=having?
    {
      { distinct=d; columns=r; from=f; where=w; group=g; having=h; }
    }

table_list: src=source joins=join_source* { (src,joins) }

join_source:
  | NATURAL maybe_join_type JOIN src=source { src,`Natural }
  | CROSS JOIN src=source { src,`Cross }
  | qualified_join src=source cond=join_cond { src,cond }

qualified_join: COMMA | maybe_join_type JOIN { }

join_cond: ON e=expr { `Search e }
  | USING l=sequence(IDENT) { `Using l }
  | (* *) { `Default }

source1:
  | id=IDENT
    {
      match find_with id with
      | Some s -> `Subquery s
      | None -> `Table id
    }
  | LPAREN s=query RPAREN { `Subquery s }
  | LPAREN s=table_list RPAREN { `Nested s }

source: src=source1 alias=maybe_as { src, alias }

select_type: DISTINCT { true } | ALL { false } | { false }

int_or_param: i=INTEGER { `Const i }
  | p=PARAM { `Param p }

limit_t: LIMIT lim=int_or_param { make_limit [`Limit,lim] }
  | LIMIT ofs=int_or_param COMMA lim=int_or_param { make_limit [`Offset,ofs; `Limit,lim] }
  | LIMIT lim=int_or_param OFFSET ofs=int_or_param { make_limit [`Limit,lim; `Offset,ofs] }

order: ORDER BY l=commas(pair(expr,order_type?)) { l }
order_type:
  | DESC { `Desc }
  | ASC { `Asc }

from: FROM t=table_list { t }
where: WHERE e=expr { e }
group: GROUP BY l=expr_list { l }
having: HAVING e=expr { e }

column1:
  | IDENT DOT ASTERISK { Sql.AllOf $1 }
  | ASTERISK { Sql.All }
  | e=expr m=maybe_as { Sql.Expr (e,m) }

maybe_as: AS? name=IDENT { Some name }
  | { None }

anyall: ANY | ALL | SOME { }

attr_name: cname=IDENT { { cname; tname=None} }
  | table=IDENT DOT cname=IDENT
  | IDENT DOT table=IDENT DOT cname=IDENT { {cname; tname=Some table} } (* FIXME database identifier *)

like_expr:
  | e1=expr; like; e2=expr %prec LIKE { Fun (`Like, [e1;e2]) }
  | e1=expr; NOT; like; e2=expr %prec LIKE { Fun (`Not, [Fun (`Like, [e1;e2])]) }

expr:
  | expr numeric_bin_op expr %prec PLUS { Fun ($2, [$1;$3]) }
  | expr boolean_bin_op expr %prec AND { Fun ($2, [$1;$3]) }
  | e1=expr; op=comparison_op; anyall?; e2=expr %prec EQUAL { Fun(op, [e1;e2]) }
  | expr CONCAT_OP expr { Fun (`Concat,[$1;$3]) }
  | e=like_expr { e }
  | op=unary_op; e=expr { Fun(op, [e]) }
  | MINUS expr %prec UNARY_MINUS { $2 }
  | INTERVAL e=expr i=interval_unit { Fun (`Interval i, [e]) }
  | LPAREN expr RPAREN { $2 }
  | attr_name collate? { Column $1 }
  | VALUES LPAREN n=IDENT RPAREN { Inserted n }
  | x = INTEGER; DAY { Fun (`Day, [Value (Int x)]) }
  | x = INTEGER; YEAR { Fun (`Year, [Value (Int x)]) }
  | v=literal_value { Value v }
  | e1=expr IN l=sequence(expr) { Fun (`In, [e1; Sequence l]) }
  | e1=expr NOT IN l=sequence(expr) { mk_not @@ Fun (`In, [e1; Sequence l]) }
  | e1=expr IN LPAREN q=query RPAREN { Fun (`In, [e1; Subquery (q, `AsValue)]) }
  | e1=expr NOT IN LPAREN q=query RPAREN { mk_not @@ Fun (`In, [e1; Subquery (q, `AsValue)]) }
  | e1=expr IN IDENT { e1 }
  | LPAREN q=query RPAREN { Subquery (q, `AsValue) }
  | PARAM { Param ($1,Any) }
  | p=PARAM LCURLY l=choices c2=RCURLY { let (name,(p1,_p2)) = p in Choices ((name,(p1,c2+1)),l) }
  | SUBSTRING LPAREN s=expr FROM p=expr FOR n=expr RPAREN
  | SUBSTRING LPAREN s=expr COMMA p=expr COMMA n=expr RPAREN { Fun (`Substring, [s;p;n]) }
  | SUBSTRING LPAREN s=expr either(FROM,COMMA) p=expr RPAREN { Fun (`Substring, [s;p]) }
  | f=IDENT LPAREN p=func_params RPAREN { Fun (`Call f, p) }
  | expr IS NULL { Fun (`IsNull, [$1]) }
  | expr IS NOT NULL { mk_not @@ Fun (`IsNull, [$1]) }
  | e1=expr IS e2=expr { Fun (`Is, [e1;e2]) }
  | e1=expr IS NOT e2=expr { mk_not @@ Fun (`Is, [e1;e2]) }
  | e1=expr IS DISTINCT FROM e2=expr { Fun (`IsDistinct, [e1;e2]) }
  | e1=expr IS NOT DISTINCT FROM e2=expr { mk_not @@ Fun (`IsDistinct, [e1;e2]) }
  | expr BETWEEN expr AND expr { Fun(`Between, [$1;$3;$5]) }
  | e1=expr; NOT; BETWEEN; e2=expr; AND; e3=expr { Fun (`Not, [Fun(`Between, [e1;e2;e3])]) }
  | EXISTS; LPAREN q=query RPAREN { Subquery (q,`Exists) }
  | NOT; EXISTS; LPAREN q=query RPAREN { Fun (`Not, [Subquery (q, `Exists)]) }
  | COUNT LPAREN e = expr RPAREN { Fun (`Count, [e]) }
  | COUNT LPAREN ASTERISK RPAREN { Fun (`Count, []) }
  | MIN LPAREN e = expr RPAREN { Fun (`Min, [e]) }
  | MAX LPAREN e = expr RPAREN { Fun (`Max, [e]) }
  | SUM LPAREN e = expr RPAREN { Fun (`Sum, [e]) }
  | AVG LPAREN e = expr RPAREN { Fun (`Avg, [e]) }
  | e=case { e }
  | IF LPAREN e1=expr COMMA e2=expr COMMA e3=expr RPAREN
    {
      Case ([(e1, e2)], Some e3)
    }

case:
    (* Simple CASE *)
  | CASE e1=expr branches=nonempty_list(case_branch) e2=preceded(ELSE, expr)? END
    {
      let branches = List.map (fun (v, x) -> Fun (`Eq, [e1; v]), x) branches in
      Case (branches, e2)
    }
(* Normal CASE *)
  | CASE branches=nonempty_list(case_branch) e2=preceded(ELSE, expr)? END
    {
      Case (branches, e2)
    }

case_branch: WHEN e1=expr THEN e2=expr { (e1, e2) }

like: LIKE | LIKE_OP { }

choice_body: c1=LCURLY e=expr c2=RCURLY { (c1,Some e,c2) }
choice: name=IDENT? e=choice_body?
    {
      let (c1,e,c2) = Option.value ~default:(0,None,0) e in ((name, (c1+1,c2)),e)
    }
choices: separated_nonempty_list(BIT_OR,choice) { $1 }

literal_value:
  | x = TEXT collate? { String x }
  | BLOB collate? { failwith "BLOB not supported" }
  | x = INTEGER { Int x }
  | x = FLOAT { Float x }
  | TRUE { Bool true }
  | FALSE { Bool false }
  | DATE; x = TEXT
  | TIME; x = TEXT
  | TIMESTAMP; x = TEXT
  | CAST LPAREN x=TEXT AS DATE RPAREN { Date x }
  | NULL { Null }

expr_list: l=commas(expr) { l }
func_params: DISTINCT? l=expr_list { l }
  | ASTERISK { [] }
  | (* *) { [] }
numeric_bin_op:
  | PLUS { `Add }
  | MINUS { `Sub }
  | ASTERISK { `Mul }
  | DIV { `Div }
  | MOD { `Mod }
  | LSH { `Lsh }
  | RSH { `Rsh }
  | BIT_OR { `Bit_or }
  | BIT_AND { `Bit_and }

comparison_op:
  | EQUAL | EQ {`Eq}
  | GT { `Gt }
  | GE { `Ge }
  | LT { `Lt }
  | LE { `Le }
  | NEQ { `Neq }
  | NOT_DISTINCT_OP { `NotDistinct }

boolean_bin_op:
  | AND {`And}
  | OR {`Or}

unary_op:
  | TILDE { `Bit_not }
  | NOT { `Not }

interval_unit:
  | MICROSECOND | SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR
  | SECOND_MICROSECOND | MINUTE_MICROSECOND | MINUTE_SECOND
  | HOUR_MICROSECOND | HOUR_SECOND | HOUR_MINUTE
  | DAY_MICROSECOND | DAY_SECOND | DAY_MINUTE | DAY_HOUR
  | YEAR_MONTH { }

collate: COLLATE IDENT { }

compound_op:
  | UNION { `Union }
  | UNION ALL { `UnionAll }
  | EXCEPT { `Except }
  | INTERSECT { `Intersect }

maybe_join_type: JOIN_TYPE1? JOIN_TYPE2? { }

