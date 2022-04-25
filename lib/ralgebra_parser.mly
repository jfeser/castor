%{
open Parser_utils
%}

%token <string> ID
%token <int> INT
%token <Fixed_point.t> FIXED
%token <bool> BOOL
%token <string> STR
%token <Ast.order> ORDER
%token <Ast.tuple> KIND
%token <Prim_type.t> PRIMTYPE
%token <Ast.Binop.t> EQ LT GT LE GE AND OR ADD SUB MUL DIV MOD STRPOS
%token <Ast.Unop.t> MONTH DAY YEAR NOT STRLEN EXTRACTY EXTRACTM EXTRACTD
%token AS DEPJOIN JOIN SELECT DEDUP FILTER COUNT GROUPBY MIN MAX AVG SUM LPAREN
RPAREN LSBRAC RSBRAC COLON DOT COMMA EOF AEMPTY ASCALAR ATUPLE ALIST AHASHIDX
AORDEREDIDX NULL ORDERBY IF THEN ELSE DATEKW EXISTS SUBSTRING ROW_NUMBER RANGE
LCURLY RCURLY QUERY

%start <Ast.t> ralgebra_eof
%start <Ast.t Ast.pred> expr_eof
%start <Name.t> name_eof
%start <Ast.t Ast.pred> value_eof
%start <Ast.Param.t> param_eof
%start <Ast.meta Ast.Query.t> query_eof
%%

ralgebra_eof: x = ralgebra; EOF { x }

expr_eof: x = expr; EOF { x }

name_eof: x = name; EOF { x }

value_eof: x = value; EOF { x }

param_eof:
  | x = param; EOF { x }
  | error { error "Expected a parameter." $startpos }

query_eof:
  | x = query; EOF { x }
  | error { error "Expected a query." $startpos }

arg:
  | k=ID; COLON; t=primtype; { (k, t) }
  | ID; COLON; error { error "Expected a type." $startpos }

param:
  | k=ID; COLON; t=primtype; { (k, t, None) }
  | k=ID; COLON; t=primtype; EQ; v=value { (k, t, Some v) }
  | ID; COLON; error { error "Expected a type." $startpos }
  | ID; COLON; primtype; EQ; error { error "Expected a value." $startpos }

primtype:
  | x = PRIMTYPE { x }
  | DATEKW { Prim_type.DateT {nullable=false} }

bracket_list(X): LSBRAC; l = separated_list(COMMA, X); RSBRAC { l }

separated_nonunit_list(sep, X):
  | x = X; sep; xs = separated_nonempty_list(sep, X) { x::xs }

parens(X): LPAREN; x = X; RPAREN { x }

key:
  | p = expr; { [p] }
  | LPAREN; ps = separated_nonunit_list(COMMA, expr); RPAREN { ps }

query:
  | QUERY; error { error "Expected a query name." $startpos }
  | QUERY; name=ID; LPAREN; args=separated_list(COMMA, arg); RPAREN;
    LCURLY; body = ralgebra; RCURLY { Ast.Query.{ name; args; body } }

ralgebra:
  | r = ralgebra_subquery { r }
  | name = ID { Ast.Relation {r_name=name; r_schema=None} |> node $symbolstartpos $endpos }

ub_op: LT { `Open } | LE { `Closed }
lb_op: GT { `Open } | GE { `Closed }

ralgebra_subquery:
  | SELECT; LPAREN; x = bracket_list(expr); COMMA; r = ralgebra; RPAREN
    { Ast.Select (x, r) |> node $symbolstartpos $endpos }

  | GROUPBY; LPAREN;
x = bracket_list(expr); COMMA;
k = bracket_list(name); COMMA;
r = ralgebra;
RPAREN { Ast.GroupBy (x, k, r) |> node $symbolstartpos $endpos }

  | FILTER; LPAREN; x = expr; COMMA; r = ralgebra; RPAREN
    { Ast.Filter (x, r) |> node $symbolstartpos $endpos }

  | DEPJOIN; LPAREN; d_lhs = ralgebra; AS; d_alias = ID; COMMA; d_rhs = ralgebra; RPAREN
    { Ast.DepJoin {d_lhs; d_alias; d_rhs} |> node $symbolstartpos $endpos }

  | JOIN; LPAREN; p = expr; COMMA; r1 = ralgebra; COMMA; r2 = ralgebra; RPAREN
    { Ast.Join({pred = p; r1; r2}) |> node $symbolstartpos $endpos }

  | DEDUP; LPAREN; r = ralgebra; RPAREN
    { Ast.Dedup r |> node $symbolstartpos $endpos }

  | ORDERBY; LPAREN;
key = bracket_list(pair(expr, option(ORDER))); COMMA;
rel = ralgebra;
RPAREN { Ast.OrderBy { key = List.map (fun (e, o) -> match o with
                                                   | Some o -> e, o
                                                   | None -> e, Ast.Asc) key; rel }
         |> node $symbolstartpos $endpos }

  | AEMPTY { node $symbolstartpos $endpos AEmpty }

  | ASCALAR; e = parens(expr) { Ast.AScalar e |> node $symbolstartpos $endpos }

  | ALIST; LPAREN; k = ralgebra; AS; s = ID; COMMA; v = ralgebra; RPAREN
    {
      Ast.AList { l_keys = k; l_scope = s; l_values = v } |> node $symbolstartpos $endpos
    }

  | ATUPLE; LPAREN; ls = bracket_list(ralgebra); COMMA; k = KIND; RPAREN
    { Ast.ATuple (ls, k) |> node $symbolstartpos $endpos }

  | AHASHIDX; LPAREN;
r = ralgebra; AS; s = ID; COMMA;
x = ralgebra; COMMA;
e = key;
RPAREN { Ast.(AHashIdx {hi_keys= r; hi_values= x; hi_key_layout = None; hi_lookup=e; hi_scope=s}) |> node $symbolstartpos $endpos }

  | AORDEREDIDX; LPAREN;
r = ralgebra; AS; s = ID; COMMA;
x = ralgebra; COMMA;
b = separated_list(COMMA, bound) RPAREN
    {
      Ast.(AOrderedIdx ({ oi_keys = r; oi_scope = s; oi_values = x; oi_lookup = b; oi_key_layout = None }))
      |> node $symbolstartpos $endpos
    }

  | RANGE; LPAREN; pl=expr; COMMA; ph=expr; RPAREN
    { Ast.(Range (pl, ph)) |> node $symbolstartpos $endpos }

  | error { error "Expected an operator or relation." $startpos }

lower_bound: op = option(lb_op); e = expr { (e, Option.value op ~default:`Closed) }
upper_bound: op = option(ub_op); e = expr { (e, Option.value op ~default:`Open) }
bound: l = option(lower_bound); COMMA; u = option(upper_bound) { (l, u) }

name:
  | r = ID; DOT; f = ID; { Name.create ~scope:r f }
  | r = ID; DOT; f = ID; COLON; t = PRIMTYPE { Name.create ~scope:r ~type_:t f }
  | f = ID; { Name.create f }
  | f = ID; COLON; t = PRIMTYPE { Name.create ~type_:t f }
  | RANGE { Name.create "range" }

e0_binop: x = STRPOS { x }
e0_unop: x = DAY | x = MONTH | x = YEAR | x = STRLEN | x = EXTRACTY | x = EXTRACTM | x = EXTRACTD { x }

null:
  | NULL; COLON; t = primtype { Ast.Null (Some t) }
  | NULL { Ast.Null None }

value:
  | x = INT { Ast.Int x }
  | DATEKW; x = parens(STR); { Ast.Date (Core.Date.of_string x) }
  | x = FIXED { Ast.Fixed x }
  | x = BOOL { Ast.Bool x }
  | x = STR { Ast.String x }
  | x = null { x }

e0:
  | x = value { x }
  | n = name { Ast.Name n }
  | op = e0_unop; x = parens(expr); { Ast.Unop (op, x) }
  | NOT; x = e0 {Ast.Unop (Not, x)}
  | MIN; f = parens(expr) { Ast.Min f }
  | MAX; f = parens(expr) { Ast.Max f }
  | AVG; f = parens(expr) { Ast.Avg f }
  | SUM; f = parens(expr) { Ast.Sum f }
  | COUNT; LPAREN; RPAREN; { Ast.Count }
  | ROW_NUMBER; LPAREN; RPAREN; { Ast.Row_number }
  | EXISTS; r = parens(ralgebra); {Ast.Exists (r)}
  | SUBSTRING; xs = parens(separated_nonempty_list(COMMA, expr)) {
                            match xs with
                            | [x1; x2; x3] -> Ast.Substring (x1, x2, x3)
                            | _ -> error "Unexpected arguments." $startpos
                          }
  | op = e0_binop; xs = parens(separated_nonempty_list(COMMA, expr)) {
                                match xs with
                                | [x1; x2] -> Ast.Binop (op, x1, x2)
                                | _ -> error "Unexpected arguments." $startpos
                              }
  | r = parens(ralgebra_subquery); {Ast.First (r)}
  | x = parens(expr) { x }

e1:
  | p = e0; AS; id = ID { Ast.As_pred (p, id) }
  | x = e0 { x }

e2_op: x = MUL | x = DIV | x = MOD { x }

e2:
  | p1 = e2; op = e2_op; p2 = e1 { Ast.Binop (op, p1, p2) }
  | x = e1 { x }

e3_op: x = ADD | x = SUB { x }

e3:
  | p1 = e3; op = e3_op; p2 = e2 { Ast.Binop (op, p1, p2) }
  | x = e2 { x }

e4_op: x = EQ | x = LT | x = LE | x = GT | x = GE { x }

e4:
  | p1 = e4; op = e4_op; p2 = e3 { Ast.Binop (op, p1, p2) }
  | x = e3 { x }

e5:
  | p1 = e4; op = AND; p2 = e5 { Ast.Binop (op, p1, p2) }
  | x = e4 { x }

e6:
  | p1 = e5; op = OR; p2 = e6 { Ast.Binop (op, p1, p2) }
  | x = e5 { x }

e7:
  | IF; p1 = e6; THEN; p2 = e7; ELSE; p3 = e7 { Ast.If (p1, p2, p3) }
  | x = e6 { x }

expr:
  | x = e7 { x }
