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

%token AS
%token JOIN
%token SELECT
%token DEDUP
%token FILTER
%token COUNT
%token GROUPBY
%token MIN
%token MAX
%token AVG
%token SUM
%token LPAREN
%token RPAREN
%token LSBRAC
%token RSBRAC
%token COLON
%token DOT
%token COMMA
%token EQ
%token LT
%token GT
%token LE
%token GE
%token AND
%token OR
%token ADD
%token SUB
%token MUL
%token DIV
%token MOD
%token EOF
%token RARROW
%token AEMPTY
%token ASCALAR
%token ATUPLE
%token ALIST
%token AHASHIDX
%token AORDEREDIDX
%token NULL
%token ORDERBY
%token IF
%token THEN
%token ELSE
%token MONTH
%token DAY
%token YEAR
%token DATEKW

%start <Abslayout0.t> abs_ralgebra_eof
%start <Abslayout0.name> name_eof

%right OR
%right AND
%left GE GT LE LT EQ
%left ADD SUB
%left MUL DIV MOD
%nonassoc AS
%%

abs_ralgebra_eof:
| x = abs_ralgebra; EOF { x }

name_eof:
| x = name; EOF { x }

bracket_list(X):
| LSBRAC; l = separated_list(COMMA, X); RSBRAC { l }
| LSBRAC; separated_list(COMMA, X); error { error "Expected a ']'." $startpos }

parens(X):
| LPAREN; x = X; RPAREN { x }
| error { error "Expected parentheses." $startpos }

key:
  | p = abs_pred; { [p] }
  | LPAREN; ps = separated_list(COMMA, abs_pred); RPAREN { ps }

abs_ralgebra:
  | SELECT; LPAREN;
  x = bracket_list(abs_pred); COMMA;
  r = abs_ralgebra;
  RPAREN { A.Select (x, r) |> node $symbolstartpos $endpos }

| GROUPBY; LPAREN;
  x = bracket_list(abs_pred); COMMA;
  k = bracket_list(name); COMMA;
  r = abs_ralgebra;
  RPAREN { A.GroupBy (x, k, r) |> node $symbolstartpos $endpos }

| FILTER; LPAREN;
  x = abs_pred; COMMA;
  r = abs_ralgebra;
  RPAREN { A.Filter (x, r) |> node $symbolstartpos $endpos }

| JOIN; LPAREN;
  p = abs_pred; COMMA;
  r1 = abs_ralgebra; COMMA;
  r2 = abs_ralgebra;
  RPAREN { A.Join({pred = p; r1; r2}) |> node $symbolstartpos $endpos }

| DEDUP; LPAREN;
  r = abs_ralgebra;
  RPAREN { A.Dedup r |> node $symbolstartpos $endpos }

| ORDERBY; LPAREN;
  key = bracket_list(abs_pred); COMMA;
  rel = abs_ralgebra; COMMA;
  order = ORDER;
  RPAREN { A.OrderBy { key; order; rel } |> node $symbolstartpos $endpos }

| AEMPTY { node $symbolstartpos $endpos AEmpty }

  | ASCALAR; e = parens(abs_pred) { A.AScalar e |> node $symbolstartpos $endpos }

| ALIST; LPAREN;
  r = abs_ralgebra; COMMA;
  x = abs_ralgebra;
  RPAREN { A.AList (r, x) |> node $symbolstartpos $endpos }

| ATUPLE; LPAREN;
  ls = bracket_list(abs_ralgebra); COMMA;
  k = KIND;
  RPAREN { A.ATuple (ls, k) |> node $symbolstartpos $endpos }

| AHASHIDX; LPAREN;
  r = abs_ralgebra; COMMA;
  x = abs_ralgebra; COMMA;
  e = key;
  RPAREN { A.(AHashIdx (r, x,  { lookup = e; hi_key_layout = None })) |> node $symbolstartpos $endpos }

| AORDEREDIDX; LPAREN;
  r = abs_ralgebra; COMMA;
  x = abs_ralgebra; COMMA;
  e1 = abs_pred; COMMA;
  e2 = abs_pred;
  RPAREN { A.(AOrderedIdx (r, x, { lookup_low = e1;
                                   lookup_high = e2;
                                   order = `Asc;
                                   oi_key_layout = None })) |> node $symbolstartpos $endpos }

| name = ID { A.Scan name |> node $symbolstartpos $endpos }

| r = abs_ralgebra; AS; n = ID; { A.As (n, r) |> node $symbolstartpos $endpos }

| error { error "Expected an operator or relation." $startpos }

name:
| r = ID; DOT; f = ID; { A.({ relation = Some r; name = f; type_ = None }) }
| r = ID; DOT; f = ID; COLON; t = PRIMTYPE { A.({ relation = Some r; name = f; type_ = Some t }) }
| f = ID; { A.({ relation = None; name = f; type_ = None }) }
| f = ID; COLON; t = PRIMTYPE { A.({ relation = None; name = f; type_ = Some t }) }

abs_pred:
| n = name { A.Name n }
| x = INT { A.Int x }
| DATEKW; x = parens(DATE); { A.Date x }
| DAY; x = parens(INT); { A.Interval (x, `Days) }
| MONTH; x = parens(INT); { A.Interval (x, `Months) }
| YEAR; x = parens(INT); { A.Interval (x, `Years) }
| SUB; x = INT { A.Int (-x) }
| x = FIXED { A.Fixed x }
| SUB; x = FIXED { A.Fixed (Fixed_point.(-x)) }
| x = BOOL { A.Bool x }
| x = STR { A.String x }
| NULL { A.Null }
| LPAREN; p = abs_pred; RPAREN { p }
| p1 = abs_pred; EQ; p2 = abs_pred { A.Binop (A.Eq, p1, p2) }
| p1 = abs_pred; LT; p2 = abs_pred { A.Binop (A.Lt, p1, p2) }
| p1 = abs_pred; LE; p2 = abs_pred { A.Binop (A.Le, p1, p2) }
| p1 = abs_pred; GT; p2 = abs_pred { A.Binop (A.Gt, p1, p2) }
| p1 = abs_pred; GE; p2 = abs_pred { A.Binop (A.Ge, p1, p2) }
| p1 = abs_pred; ADD; p2 = abs_pred { A.Binop (A.Add, p1, p2) }
| p1 = abs_pred; SUB; p2 = abs_pred { A.Binop (A.Sub, p1, p2) }
| p1 = abs_pred; MUL; p2 = abs_pred { A.Binop (A.Mul, p1, p2) }
| p1 = abs_pred; DIV; p2 = abs_pred { A.Binop (A.Div, p1, p2) }
| p1 = abs_pred; MOD; p2 = abs_pred { A.Binop (A.Mod, p1, p2) }
| p1 = abs_pred; AND; p2 = abs_pred { A.Binop (A.And, p1, p2) }
| p1 = abs_pred; OR; p2 = abs_pred { A.Binop (A.Or, p1, p2) }
| COUNT; LPAREN; RPAREN; { A.Count }
| MIN; f = parens(abs_pred) { A.Min f }
| MAX; f = parens(abs_pred) { A.Max f }
| AVG; f = parens(abs_pred) { A.Avg f }
| SUM; f = parens(abs_pred) { A.Sum f }
| p = abs_pred; AS; id = ID { A.As_pred (p, id) }
| IF; p1 = abs_pred; THEN; p2 = abs_pred; ELSE; p3 = abs_pred { A.If (p1, p2, p3) }

lambda(X):
| n = ID; RARROW; x = X { (n, x) }
| error { error "Expected a lambda." $startpos }
