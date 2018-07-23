%{
    module A = Abslayout0
    open Parser_utils
%}

%token <string> ID
%token <int> INT
%token <bool> BOOL
%token <string> STR

%token AS
%token JOIN
%token SELECT
%token DEDUP
%token FILTER
%token COUNT
%token AGG
%token MIN
%token MAX
%token AVG
%token SUM
%token INT_TYPE
%token BOOL_TYPE
%token STRING_TYPE
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
%token ZIP
%token CROSS
%token AEMPTY
%token ASCALAR
%token ATUPLE
%token ALIST
%token AHASHIDX
%token AORDEREDIDX
%token NULL

%start <Abslayout0.t> abs_ralgebra_eof
%start <Abslayout0.name> name_eof

%left OR
%left AND
%nonassoc GE GT LE LT EQ
%left ADD SUB
%left MUL DIV MOD
%%

abs_ralgebra_eof:
| x = abs_ralgebra; EOF { x }

name_eof:
| x = name; EOF { x }

bracket_list(X):
| error { error "Expected a '['." $startpos }
| LSBRAC; l = separated_list(COMMA, X); RSBRAC { l }
| LSBRAC; separated_list(COMMA, X); error { error "Expected a ']'." $startpos }

parens(X):
| LPAREN; x = X; RPAREN { x }
| error { error "Expected parentheses." $startpos }

abs_ralgebra:
| SELECT; LPAREN; x = bracket_list(abs_pred); COMMA; r = abs_ralgebra; RPAREN { A.Select (x, r) |> node }
| AGG; LPAREN; x = bracket_list(abs_agg_expr); COMMA; k = bracket_list(name); COMMA; r = abs_ralgebra; RPAREN { A.Agg (x, k, r) |> node }
| FILTER; LPAREN; x = abs_pred; COMMA; r = abs_ralgebra; RPAREN { A.Filter (x, r) |> node }
| JOIN; LPAREN; p = abs_pred; COMMA; r1 = abs_ralgebra; COMMA; r2 = abs_ralgebra; RPAREN { A.Join({pred = p; r1; r2}) |> node }
| DEDUP; LPAREN; r = abs_ralgebra; RPAREN { A.Dedup r |> node }
| AEMPTY { node AEmpty }
| ASCALAR; e = parens(abs_pred) { A.AScalar e |> node }
| ALIST; LPAREN; r = abs_ralgebra; COMMA; x = abs_ralgebra RPAREN { A.AList (r, x) |> node }
| ATUPLE; LPAREN; ls = bracket_list(abs_ralgebra); COMMA; k = kind; RPAREN { A.ATuple (ls, k) |> node }
| AHASHIDX; LPAREN; r = abs_ralgebra; COMMA; x = abs_ralgebra; COMMA; e = abs_pred RPAREN { A.(AHashIdx (r, x,  { lookup = e })) |> node }
| AORDEREDIDX; LPAREN; r = abs_ralgebra; COMMA; x = abs_ralgebra; COMMA; e1 = abs_pred; COMMA; e2 = abs_pred; COMMA; e3 = abs_pred; COMMA { A.(AOrderedIdx (r, x, { lookup_low = e1; lookup_high = e2; order = e3 })) |> node }
| name = ID { A.Scan name |> node }
| r = abs_ralgebra; AS; n = ID; { A.As (n, r) |> node }
| error { error "Expected an operator or relation." $startpos }

name:
| r = ID; DOT; f = ID; { A.({ relation = Some r; name = f; type_ = None }) }
| r = ID; DOT; f = ID; COLON; t = primtype { A.({ relation = Some r; name = f; type_ = Some t }) }
| f = ID; { A.({ relation = None; name = f; type_ = None }) }
| f = ID; COLON; t = primtype { A.({ relation = None; name = f; type_ = Some t }) }

primtype:
| INT_TYPE { IntT }
| BOOL_TYPE { BoolT }
| STRING_TYPE { StringT }
| error { error "Expected a type." $startpos }

abs_pred:
| n = name { A.Name n }
| x = INT { A.Int x }
| x = BOOL { A.Bool x }
| x = STR { A.String x }
| NULL { A.Null }
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
| p1 = abs_pred; AND; p2 = abs_pred { A.Varop (A.And, [p1; p2]) }
| p1 = abs_pred; OR; p2 = abs_pred { A.Varop (A.Or, [p1; p2]) }

abs_agg_expr:
| COUNT { A.Count }
| MIN; f = parens(name) { A.Min f }
| MAX; f = parens(name) { A.Max f }
| AVG; f = parens(name) { A.Avg f }
| SUM; f = parens(name) { A.Sum f }
| f = name { A.Key f }

lambda(X):
| n = ID; RARROW; x = X { (n, x) }
| error { error "Expected a lambda." $startpos }

kind:
| ZIP; { A.Zip }
| CROSS; { A.Cross }
