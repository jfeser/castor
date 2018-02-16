%{
    open Ralgebra0

    let error err = Base.Error.(of_string err |> raise)
%}

%token <string> ID

%token PROJECT
%token FILTER
%token EQJOIN
%token CONCAT
%token INT
%token BOOL
%token STRING
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

%start <(string * string, string) Ralgebra0.t> ralgebra_eof

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

operator: | PROJECT | FILTER | EQJOIN | CONCAT { () }

list(X):
| error { error "Expected a '['." }
| LSBRAC; l = separated_list(COMMA, X); RSBRAC { l }
| LSBRAC; separated_list(COMMA, X); error { error "Expected a ']'." }

ralgebra:
| PROJECT; LPAREN; fields = list(field); COMMA; r = ralgebra; RPAREN { Project (fields, r) }
| PROJECT; LPAREN; error { error "Expected a list of fields." }
| PROJECT; LPAREN; list(field); error { error "Expected a ','." }
| FILTER; LPAREN; pred = pred; COMMA; r = ralgebra; RPAREN { Filter (pred, r) }
| EQJOIN; LPAREN; f1 = field; COMMA; f2 = field; COMMA; r1 = ralgebra; COMMA; r2 = ralgebra; RPAREN { EqJoin(f1, f2, r1, r2) }
| CONCAT; LPAREN; rs = separated_list(COMMA, ralgebra); RPAREN { Concat rs }
| r = ID { Relation r }
| operator; error { error "Expected a '('." }

field:
| r = ID; DOT; f = ID; { (r, f) }

primtype:
| INT { IntT }
| BOOL { BoolT }
| STRING { StringT }

pred:
| x = ID; COLON; t = primtype { Var (x, t) }
| ID; COLON; error { error "Expected a type." }
| f = field; { Field f }
| p1 = pred; EQ; p2 = pred { Binop (Eq, p1, p2) }
| p1 = pred; LT; p2 = pred { Binop (Lt, p1, p2) }
| p1 = pred; LE; p2 = pred { Binop (Le, p1, p2) }
| p1 = pred; GT; p2 = pred { Binop (Gt, p1, p2) }
| p1 = pred; GE; p2 = pred { Binop (Ge, p1, p2) }
| p1 = pred; AND; p2 = pred { Varop (And, [p1; p2]) }
| p1 = pred; OR; p2 = pred { Varop (Or, [p1; p2]) }
| pred; error { error "Expected an operator." }
