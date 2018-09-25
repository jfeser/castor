%{
    module A = Abslayout0
    open Parser_utils
%}

%token <string> ID
%token <int> INT
%token <Fixed_point.t> FIXED
%token <bool> BOOL
%token <string> STR
%token <[`Asc | `Desc]> ORDER
%token <Abslayout0.tuple> KIND
%token <Type0.PrimType.t> PRIMTYPE
%token <Core.Date.t> DATE

%token AS JOIN SELECT DEDUP FILTER COUNT GROUPBY MIN MAX AVG SUM LPAREN RPAREN
   LSBRAC RSBRAC COLON DOT COMMA EQ LT GT LE GE AND OR ADD SUB MUL DIV MOD EOF
   AEMPTY ASCALAR ATUPLE ALIST AHASHIDX AORDEREDIDX NULL ORDERBY IF THEN
   ELSE MONTH DAY YEAR DATEKW

%start <Abslayout0.t> ralgebra_eof
%start <Abslayout0.name> name_eof
%%

ralgebra_eof:
| x = ralgebra; EOF { x }

name_eof:
| x = name; EOF { x }

bracket_list(X):
| LSBRAC; l = separated_list(COMMA, X); RSBRAC { l }

separated_nonunit_list(sep, X):
| x = X; sep; xs = separated_nonempty_list(sep, X) { x::xs }

parens(X):
| LPAREN; x = X; RPAREN { x }
| error { error "Expected parentheses." $startpos }

key:
  | p = expr; { [p] }
  | LPAREN; ps = separated_nonunit_list(COMMA, expr); RPAREN { ps }

ralgebra:
  | SELECT; LPAREN;
  x = bracket_list(expr); COMMA;
  r = ralgebra;
  RPAREN { A.Select (x, r) |> node $symbolstartpos $endpos }

| GROUPBY; LPAREN;
  x = bracket_list(expr); COMMA;
  k = bracket_list(name); COMMA;
  r = ralgebra;
  RPAREN { A.GroupBy (x, k, r) |> node $symbolstartpos $endpos }

| FILTER; LPAREN;
  x = expr; COMMA;
  r = ralgebra;
  RPAREN { A.Filter (x, r) |> node $symbolstartpos $endpos }

| JOIN; LPAREN;
  p = expr; COMMA;
  r1 = ralgebra; COMMA;
  r2 = ralgebra;
  RPAREN { A.Join({pred = p; r1; r2}) |> node $symbolstartpos $endpos }

| DEDUP; LPAREN;
  r = ralgebra;
  RPAREN { A.Dedup r |> node $symbolstartpos $endpos }

| ORDERBY; LPAREN;
  key = bracket_list(expr); COMMA;
  rel = ralgebra; COMMA;
  order = ORDER;
  RPAREN { A.OrderBy { key; order; rel } |> node $symbolstartpos $endpos }

| AEMPTY { node $symbolstartpos $endpos AEmpty }

  | ASCALAR; e = parens(expr) { A.AScalar e |> node $symbolstartpos $endpos }

| ALIST; LPAREN;
  r = ralgebra; COMMA;
  x = ralgebra;
  RPAREN { A.AList (r, x) |> node $symbolstartpos $endpos }

| ATUPLE; LPAREN;
  ls = bracket_list(ralgebra); COMMA;
  k = KIND;
  RPAREN { A.ATuple (ls, k) |> node $symbolstartpos $endpos }

| AHASHIDX; LPAREN;
  r = ralgebra; COMMA;
  x = ralgebra; COMMA;
  e = key;
  RPAREN { A.(AHashIdx (r, x,  { lookup = e; hi_key_layout = None })) |> node $symbolstartpos $endpos }

| AORDEREDIDX; LPAREN;
  r = ralgebra; COMMA;
  x = ralgebra; COMMA;
  e1 = expr; COMMA;
  e2 = expr;
  RPAREN { A.(AOrderedIdx (r, x, { lookup_low = e1;
                                   lookup_high = e2;
                                   order = `Asc;
                                   oi_key_layout = None })) |> node $symbolstartpos $endpos }

| name = ID { A.Scan name |> node $symbolstartpos $endpos }

| r = ralgebra; AS; n = ID; { A.As (n, r) |> node $symbolstartpos $endpos }

| error { error "Expected an operator or relation." $startpos }

name:
| r = ID; DOT; f = ID; { A.({ relation = Some r; name = f; type_ = None }) }
| r = ID; DOT; f = ID; COLON; t = PRIMTYPE { A.({ relation = Some r; name = f; type_ = Some t }) }
| f = ID; { A.({ relation = None; name = f; type_ = None }) }
| f = ID; COLON; t = PRIMTYPE { A.({ relation = None; name = f; type_ = Some t }) }

e0:
| x = INT { A.Int x }
| DATEKW; x = parens(DATE); { A.Date x }
| x = FIXED { A.Fixed x }
| x = BOOL { A.Bool x }
| x = STR { A.String x }
| NULL { A.Null }
| n = name { A.Name n }
| DAY; x = parens(expr); { A.Unop (Day, x) }
| MONTH; x = parens(expr); { A.Unop (Month, x) }
| YEAR; x = parens(expr); { A.Unop (Year, x) }
| MIN; f = parens(expr) { A.Min f }
| MAX; f = parens(expr) { A.Max f }
| AVG; f = parens(expr) { A.Avg f }
| SUM; f = parens(expr) { A.Sum f }
| COUNT; LPAREN; RPAREN; { A.Count }
| x = parens(expr) { x }

e1:
| p = e0; AS; id = ID { A.As_pred (p, id) }
| x = e0 { x }

e2:
| p1 = e2; MUL; p2 = e1 { A.Binop (A.Mul, p1, p2) }
| p1 = e2; DIV; p2 = e1 { A.Binop (A.Div, p1, p2) }
| p1 = e2; MOD; p2 = e1 { A.Binop (A.Mod, p1, p2) }
| x = e1 { x }

e3:
| p1 = e3; ADD; p2 = e2 { A.Binop (A.Add, p1, p2) }
| p1 = e3; SUB; p2 = e2 { A.Binop (A.Sub, p1, p2) }
| x = e2 { x }

e4:
| p1 = e4; EQ; p2 = e3 { A.Binop (A.Eq, p1, p2) }
| p1 = e4; LT; p2 = e3 { A.Binop (A.Lt, p1, p2) }
| p1 = e4; LE; p2 = e3 { A.Binop (A.Le, p1, p2) }
| p1 = e4; GT; p2 = e3 { A.Binop (A.Gt, p1, p2) }
| p1 = e4; GE; p2 = e3 { A.Binop (A.Ge, p1, p2) }
| x = e3 { x }

e5:
| p1 = e4; AND; p2 = e5 { A.Binop (A.And, p1, p2) }
| x = e4 { x }

e6:
| p1 = e5; OR; p2 = e6 { A.Binop (A.Or, p1, p2) }
| x = e5 { x }

e7:
| IF; p1 = e6; THEN; p2 = e7; ELSE; p3 = e7 { A.If (p1, p2, p3) }
| x = e6 { x }

expr:
| x = e7 { x }

