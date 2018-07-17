%{
    open Printf
    module R = Ralgebra0
    module A = Abslayout0

    let error msg pos =
      let col = pos.Lexing.pos_cnum - pos.pos_bol in
      raise (R.ParseError (msg, pos.pos_lnum, col))

    let fst (x, _) = x
    let snd (_, x) = x
    let node r = A.({ node = r; meta = () })
%}

%token <string> ID
%token <int> INT
%token <bool> BOOL
%token <string> STR

%token AS
%token JOIN
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
%start <(Type0.PrimType.t option Abslayout0.name, unit) Abslayout0.ralgebra> abs_ralgebra_eof
%start <Type0.PrimType.t option Abslayout0.name> name_eof

%left OR
%left AND
%nonassoc GE GT LE LT EQ
%left ADD SUB
%left MUL DIV MOD
%%

ralgebra_eof:
| x = ralgebra; EOF { x }

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

abs_ralgebra:
| SELECT; LPAREN; lam = lambda(bracket_list(abs_pred)); COMMA; r = abs_ralgebra; RPAREN { A.Select (fst lam, snd lam, r) |> node }
| AGG; LPAREN; lam = lambda(bracket_list(abs_agg_expr)); COMMA; k = bracket_list(name); COMMA; r = abs_ralgebra; RPAREN { A.Agg (fst lam, snd lam, k, r) |> node }
| FILTER; LPAREN; lam = lambda(abs_pred); COMMA; r = abs_ralgebra; RPAREN { A.Filter (fst lam, snd lam, r) |> node }
| JOIN; LPAREN; p = abs_pred; COMMA; r1 = abs_ralgebra; AS; n1 = ID; COMMA; r2 = abs_ralgebra; AS; n2 = ID; RPAREN { A.Join({pred = p; r1_name = n1; r1; r2_name = n2; r2}) |> node }
| DEDUP; LPAREN; r = abs_ralgebra; RPAREN { A.Dedup r |> node }
| AEMPTY { node AEmpty }
| ASCALAR; e = parens(abs_pred) { A.AScalar e |> node }
| ALIST; LPAREN; r = abs_ralgebra; COMMA; lam = lambda(abs_ralgebra); RPAREN { A.AList (r, fst lam, snd lam) |> node }
| ATUPLE; LPAREN; ls = bracket_list(abs_ralgebra); COMMA; k = kind; RPAREN { A.ATuple (ls, k) |> node }
| AHASHIDX; LPAREN; r = abs_ralgebra; COMMA; lam = lambda(abs_ralgebra); COMMA; e = abs_pred RPAREN { A.(AHashIdx (r, fst lam, snd lam, { lookup = e })) |> node }
| AORDEREDIDX; LPAREN; r = abs_ralgebra; COMMA; lam = lambda(abs_ralgebra); COMMA; e1 = abs_pred; COMMA; e2 = abs_pred; COMMA; e3 = abs_pred; COMMA { A.(AOrderedIdx (r, fst lam, snd lam, { lookup_low = e1; lookup_high = e2; order = e3 })) |> node }
| name = ID { A.Scan name |> node }
| error { error "Expected an operator or relation." $startpos }

field:
| r = ID; DOT; f = ID; { (r, f) }
| q = ID; DOT; r = ID; DOT; f = ID; { (r, sprintf "%s.%s" q f) }
| error { error "Expected a field." $startpos }

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

pred:
| x = ID; COLON; t = primtype { R.Var (x, t) }
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

abs_pred:
| n = name { A.Name n }
| x = INT { A.Int x }
| x = BOOL { A.Bool x }
| x = STR { A.String x }
| p1 = abs_pred; EQ; p2 = abs_pred { A.Binop (R.Eq, p1, p2) }
| p1 = abs_pred; LT; p2 = abs_pred { A.Binop (R.Lt, p1, p2) }
| p1 = abs_pred; LE; p2 = abs_pred { A.Binop (R.Le, p1, p2) }
| p1 = abs_pred; GT; p2 = abs_pred { A.Binop (R.Gt, p1, p2) }
| p1 = abs_pred; GE; p2 = abs_pred { A.Binop (R.Ge, p1, p2) }
| p1 = abs_pred; ADD; p2 = abs_pred { A.Binop (R.Add, p1, p2) }
| p1 = abs_pred; SUB; p2 = abs_pred { A.Binop (R.Sub, p1, p2) }
| p1 = abs_pred; MUL; p2 = abs_pred { A.Binop (R.Mul, p1, p2) }
| p1 = abs_pred; DIV; p2 = abs_pred { A.Binop (R.Div, p1, p2) }
| p1 = abs_pred; MOD; p2 = abs_pred { A.Binop (R.Mod, p1, p2) }
| p1 = abs_pred; AND; p2 = abs_pred { A.Varop (R.And, [p1; p2]) }
| p1 = abs_pred; OR; p2 = abs_pred { A.Varop (R.Or, [p1; p2]) }

agg_expr:
| COUNT { R.Count }
| MIN; f = parens(field) { R.Min f }
| MAX; f = parens(field) { R.Max f }
| AVG; f = parens(field) { R.Avg f }
| SUM; f = parens(field) { R.Sum f }
| f = field { R.Key f }

abs_agg_expr:
| COUNT { A.Count }
| MIN; f = parens(name) { A.Min f }
| MAX; f = parens(name) { A.Max f }
| AVG; f = parens(name) { A.Avg f }
| SUM; f = parens(name) { A.Sum f }
| f = name { A.Key f }

lambda(X):
| n = ID; RARROW; x = X { (n, x) }

kind:
| ZIP; { A.Zip }
| CROSS; { A.Cross }
