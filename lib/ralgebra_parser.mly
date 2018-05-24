%{
    module R = Ralgebra0
    module A = Abslayout0

    let error msg pos =
      let col = pos.Lexing.pos_cnum - pos.pos_bol in
      raise (R.ParseError (msg, pos.pos_lnum, col))

    let fst (x, _) = x
    let snd (_, x) = x
%}

%token <string> ID
%token <int> INT
%token <bool> BOOL
%token <string> STR

%token PROJECT
%token SELECT
%token DEDUP
%token FILTER
%token EQJOIN
%token CONCAT
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

%start <(string * string, string, Layout.t) Ralgebra0.t> ralgebra_eof
%start <(string * string, (string * string, string) Abslayout0.layout) Abslayout0.ralgebra> abs_ralgebra_eof

%left OR
%left AND
%nonassoc GE GT LE LT EQ
%left ADD SUB
%left MUL DIV MOD
%%

ralgebra_eof:
| x = ralgebra; EOF { x }

abs_ralgebra_eof:
| x = abs_ralgebra(layout); EOF { x }


bracket_list(X):
| error { error "Expected a '['." $startpos }
| LSBRAC; l = separated_list(COMMA, X); RSBRAC { l }
| LSBRAC; separated_list(COMMA, X); error { error "Expected a ']'." $startpos }

parens(X):
| LPAREN; x = X; RPAREN { x }
| error { error "Expected parentheses." $startpos }

ralgebra:
| PROJECT; LPAREN; fields = bracket_list(field); COMMA; r = ralgebra; RPAREN { R.Project (fields, r) }
| PROJECT; LPAREN; error { error "Expected a list of fields." $startpos }
| PROJECT; LPAREN; bracket_list(field); error { error "Expected a ','." $startpos }
| COUNT; r = parens(ralgebra); { R.Count r }
| AGG; LPAREN; a = bracket_list(agg_expr); COMMA; k = bracket_list(field); COMMA; r = ralgebra; RPAREN { R.Agg (a, k, r) }
| FILTER; LPAREN; pred = pred; COMMA; r = ralgebra; RPAREN { R.Filter (pred, r) }
| EQJOIN; LPAREN; f1 = field; COMMA; f2 = field; COMMA; r1 = ralgebra; COMMA; r2 = ralgebra; RPAREN { R.EqJoin(f1, f2, r1, r2) }
| CONCAT; rs = parens(separated_list(COMMA, ralgebra)); { R.Concat rs }
| r = ID { R.Relation r }
| error { error "Expected an operator or relation." $startpos }

abs_ralgebra(X):
| SELECT; LPAREN; exprs = bracket_list(pred); COMMA; r = abs_ralgebra(X); RPAREN { A.Select (exprs, r) }
| AGG; LPAREN; a = bracket_list(agg_expr); COMMA; k = bracket_list(field); COMMA; r = abs_ralgebra(X); RPAREN { A.Agg (a, k, r) }
| FILTER; LPAREN; pred = pred; COMMA; r = abs_ralgebra(X); RPAREN { A.Filter (pred, r) }
| EQJOIN; LPAREN; f1 = field; COMMA; f2 = field; COMMA; r1 = abs_ralgebra(X); COMMA; r2 = abs_ralgebra(X); RPAREN { A.EqJoin(f1, f2, r1, r2) }
| DEDUP; LPAREN; r = abs_ralgebra(X); RPAREN { A.Dedup r }
| r = X { A.Scan r }
| error { error "Expected an operator or relation." $startpos }

field:
| r = ID; DOT; f = ID; { (r, f) }
| error { error "Expected a field." $startpos }

primtype:
| INT_TYPE { IntT }
| BOOL_TYPE { BoolT }
| STRING_TYPE { StringT }
| error { error "Expected a type." $startpos }

pred:
| x = ID; COLON; t = primtype { Var (x, t) }
| ID; COLON; error { error "Expected a type." $startpos }
| f = field; { R.Field f }
| x = INT { R.Int x }
| x = BOOL { R.Bool x }
| x = STR { R.String x }
| p1 = pred; EQ; p2 = pred { R.Binop (R.Eq, p1, p2) }
| p1 = pred; LT; p2 = pred { R.Binop (R.Lt, p1, p2) }
| p1 = pred; LE; p2 = pred { R.Binop (R.Le, p1, p2) }
| p1 = pred; GT; p2 = pred { R.Binop (R.Gt, p1, p2) }
| p1 = pred; GE; p2 = pred { R.Binop (R.Ge, p1, p2) }
| p1 = pred; ADD; p2 = pred { R.Binop (R.Add, p1, p2) }
| p1 = pred; SUB; p2 = pred { R.Binop (R.Sub, p1, p2) }
| p1 = pred; MUL; p2 = pred { R.Binop (R.Mul, p1, p2) }
| p1 = pred; DIV; p2 = pred { R.Binop (R.Div, p1, p2) }
| p1 = pred; MOD; p2 = pred { R.Binop (R.Mod, p1, p2) }
| p1 = pred; AND; p2 = pred { R.Varop (R.And, [p1; p2]) }
| p1 = pred; OR; p2 = pred { R.Varop (R.Or, [p1; p2]) }
| pred; error { error "Expected an operator." $startpos }

agg_expr:
| COUNT { R.Count }
| MIN; f = parens(field) { R.Min f }
| MAX; f = parens(field) { R.Max f }
| AVG; f = parens(field) { R.Avg f }
| SUM; f = parens(field) { R.Sum f }
| f = field { R.Key f }

lambda(X):
| n = ID; RARROW; x = X { (n, x) }

kind:
| ZIP; { A.Zip }
| CROSS; { A.Cross }

layout:
| AEMPTY { AEmpty }
| ASCALAR; e = parens(pred) { A.AScalar e }
| ALIST; LPAREN; r = abs_ralgebra(ID); COMMA; lam = lambda(layout); RPAREN { A.AList (r, fst lam, snd lam) }
| ATUPLE; LPAREN; ls = separated_list(COMMA, layout); COMMA; k = kind; RPAREN { A.ATuple (ls, k) }
| AHASHIDX; LPAREN; r = abs_ralgebra(ID); COMMA; lam = lambda(layout); COMMA; e = pred RPAREN { A.(AHashIdx (r, fst lam, snd lam, { lookup = e })) }
| AORDEREDIDX; LPAREN; r = abs_ralgebra(ID); COMMA; lam = lambda(layout); COMMA; e1 = pred; COMMA; e2 = pred; COMMA; e3 = pred; COMMA { A.(AOrderedIdx (r, fst lam, snd lam, { lookup_low = e1; lookup_high = e2; order = e3 })) }
