%{
    open Ralgebra0

    let error msg pos =
      let col = pos.Lexing.pos_cnum - pos.pos_bol in
      raise (ParseError (msg, pos.pos_lnum, col))
%}

%token <string> ID
%token <int> INT
%token <bool> BOOL
%token <string> STR

%token PROJECT
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
%token EOF

%start <(string * string, string, Layout.t) Ralgebra0.t> ralgebra_eof

%left OR
%left AND
%nonassoc GE
%nonassoc GT
%nonassoc LE
%nonassoc LT
%nonassoc EQ
%%

ralgebra_eof:
| x = ralgebra; EOF { x }

bracket_list(X):
| error { error "Expected a '['." $startpos }
| LSBRAC; l = separated_list(COMMA, X); RSBRAC { l }
| LSBRAC; separated_list(COMMA, X); error { error "Expected a ']'." $startpos }

parens(X):
| LPAREN; x = X; RPAREN { x }
| error { error "Expected parentheses." $startpos }

ralgebra:
| PROJECT; LPAREN; fields = bracket_list(field); COMMA; r = ralgebra; RPAREN { Project (fields, r) }
| PROJECT; LPAREN; error { error "Expected a list of fields." $startpos }
| PROJECT; LPAREN; bracket_list(field); error { error "Expected a ','." $startpos }
| COUNT; r = parens(ralgebra); { Count r }
| AGG; LPAREN; a = bracket_list(agg_expr); COMMA; k = bracket_list(field); COMMA; r = ralgebra; RPAREN { Agg (a, k, r) }
| FILTER; LPAREN; pred = pred; COMMA; r = ralgebra; RPAREN { Filter (pred, r) }
| EQJOIN; LPAREN; f1 = field; COMMA; f2 = field; COMMA; r1 = ralgebra; COMMA; r2 = ralgebra; RPAREN { EqJoin(f1, f2, r1, r2) }
| CONCAT; rs = parens(separated_list(COMMA, ralgebra)); { Concat rs }
| r = ID { Relation r }
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
| f = field; { Field f }
| x = INT { Int x }
| x = BOOL { Bool x }
| x = STR { String x }
| p1 = pred; EQ; p2 = pred { Binop (Eq, p1, p2) }
| p1 = pred; LT; p2 = pred { Binop (Lt, p1, p2) }
| p1 = pred; LE; p2 = pred { Binop (Le, p1, p2) }
| p1 = pred; GT; p2 = pred { Binop (Gt, p1, p2) }
| p1 = pred; GE; p2 = pred { Binop (Ge, p1, p2) }
| p1 = pred; AND; p2 = pred { Varop (And, [p1; p2]) }
| p1 = pred; OR; p2 = pred { Varop (Or, [p1; p2]) }
| pred; error { error "Expected an operator." $startpos }

agg_expr:
| COUNT { Count }
| MIN; f = parens(field) { Min f }
| MAX; f = parens(field) { Max f }
| AVG; f = parens(field) { Avg f }
| f = parens(field) { Key f }
