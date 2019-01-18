%{
    module A = Abslayout0
    open Parser_utils
%}

%token <string> ID
%token <int> INT
%token <Fixed_point.t> FIXED
%token <bool> BOOL
%token <string> STR
%token <Abslayout0.order> ORDER
%token <Abslayout0.tuple> KIND
%token <Type0.PrimType.t> PRIMTYPE
%token <Core.Date.t> DATE
%token <Abslayout0.binop> EQ LT GT LE GE AND OR ADD SUB MUL DIV MOD STRPOS
%token <Abslayout0.unop> MONTH DAY YEAR NOT STRLEN EXTRACTY EXTRACTM EXTRACTD
%token AS JOIN SELECT DEDUP FILTER COUNT GROUPBY MIN MAX AVG SUM LPAREN RPAREN
   LSBRAC RSBRAC COLON DOT COMMA EOF
   AEMPTY ASCALAR ATUPLE ALIST AHASHIDX AORDEREDIDX NULL ORDERBY IF THEN
   ELSE DATEKW EXISTS SUBSTRING

%start <Abslayout0.t> ralgebra_eof
%start <Abslayout0.pred> expr_eof
%start <Abslayout0.name> name_eof
%%

ralgebra_eof:
| x = ralgebra; EOF { x }

expr_eof:
| x = expr; EOF { x }

name_eof:
| x = name; EOF { x }

bracket_list(X):
| LSBRAC; l = separated_list(COMMA, X); RSBRAC { l }

separated_nonunit_list(sep, X):
| x = X; sep; xs = separated_nonempty_list(sep, X) { x::xs }

parens(X):
| LPAREN; x = X; RPAREN { x }

key:
  | p = expr; { [p] }
  | LPAREN; ps = separated_nonunit_list(COMMA, expr); RPAREN { ps }

ralgebra:
  | r = ralgebra_subquery { r }
  | name = ID { A.Scan name |> node $symbolstartpos $endpos }
  | r = ralgebra; AS; n = ID; { A.As (n, r) |> node $symbolstartpos $endpos }

ralgebra_subquery:
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
  key = bracket_list(pair(expr, option(ORDER))); COMMA;
  rel = ralgebra;
  RPAREN { A.OrderBy { key = List.map (fun (e, o) -> match o with
                                                     | Some o -> e, o
                                                     | None -> e, A.Asc) key; rel }
           |> node $symbolstartpos $endpos }

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

| error { error "Expected an operator or relation." $startpos }

name:
| r = ID; DOT; f = ID; { A.({ relation = Some r; name = f; type_ = None }) }
| r = ID; DOT; f = ID; COLON; t = PRIMTYPE { A.({ relation = Some r; name = f; type_ = Some t }) }
| f = ID; { A.({ relation = None; name = f; type_ = None }) }
| f = ID; COLON; t = PRIMTYPE { A.({ relation = None; name = f; type_ = Some t }) }

e0_binop: x = STRPOS { x }
e0_unop: x = DAY | x = MONTH | x = YEAR | x = STRLEN | x = EXTRACTY | x = EXTRACTM | x = EXTRACTD { x }

e0:
| x = INT { A.Int x }
| DATEKW; x = parens(DATE); { A.Date x }
| x = FIXED { A.Fixed x }
| x = BOOL { A.Bool x }
| x = STR { A.String x }
| NULL { A.Null }
| n = name { A.Name n }
| op = e0_unop; x = parens(expr); { A.Unop (op, x) }
| NOT; x = e0 {A.Unop (Not, x)}
| MIN; f = parens(expr) { A.Min f }
| MAX; f = parens(expr) { A.Max f }
| AVG; f = parens(expr) { A.Avg f }
| SUM; f = parens(expr) { A.Sum f }
| COUNT; LPAREN; RPAREN; { A.Count }
| EXISTS; r = parens(ralgebra); {A.Exists (r)}
| SUBSTRING; xs = parens(separated_nonempty_list(COMMA, expr)) {
                      match xs with
                      | [x1; x2; x3] -> A.Substring (x1, x2, x3)
                      | _ -> error "Unexpected arguments." $startpos
                    }
| op = e0_binop; xs = parens(separated_nonempty_list(COMMA, expr)) {
                          match xs with
                          | [x1; x2] -> A.Binop (op, x1, x2)
                          | _ -> error "Unexpected arguments." $startpos
                        }
| r = parens(ralgebra_subquery); {A.First (r)}
| x = parens(expr) { x }

e1:
| p = e0; AS; id = ID { A.As_pred (p, id) }
| x = e0 { x }

e2_op: x = MUL | x = DIV | x = MOD { x }

e2:
| p1 = e2; op = e2_op; p2 = e1 { A.Binop (op, p1, p2) }
| x = e1 { x }

e3_op: x = ADD | x = SUB { x }

e3:
| p1 = e3; op = e3_op; p2 = e2 { A.Binop (op, p1, p2) }
| x = e2 { x }

e4_op: x = EQ | x = LT | x = LE | x = GT | x = GE { x }

e4:
| p1 = e4; op = e4_op; p2 = e3 { A.Binop (op, p1, p2) }
| x = e3 { x }

e5:
| p1 = e4; op = AND; p2 = e5 { A.Binop (op, p1, p2) }
| x = e4 { x }

e6:
| p1 = e5; op = OR; p2 = e6 { A.Binop (op, p1, p2) }
| x = e5 { x }

e7:
| IF; p1 = e6; THEN; p2 = e7; ELSE; p3 = e7 { A.If (p1, p2, p3) }
| x = e6 { x }

expr:
| x = e7 { x }

