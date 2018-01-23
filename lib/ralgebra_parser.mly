%{
    open Ralgebra0
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

%nonassoc EQ
%nonassoc LT
%nonassoc LE
%nonassoc GT
%nonassoc GE
%left AND
%left OR
%%

ralgebra_eof:
| x = ralgebra; EOF { x }

ralgebra:
| PROJECT; LPAREN; fields = separated_list(COMMA, field); COMMA; r = ralgebra; RPAREN { Project (fields, r) }
| FILTER; LPAREN; pred = pred; COMMA; r = ralgebra; RPAREN { Filter (pred, r) }
| EQJOIN; LPAREN; f1 = field; COMMA; f2 = field; COMMA; r1 = ralgebra; COMMA; r2 = ralgebra; RPAREN { EqJoin(f1, f2, r1, r2) }
| CONCAT; LPAREN; rs = separated_list(COMMA, ralgebra); RPAREN { Concat rs }
| r = ID { Relation r }

field:
| r = ID; DOT; f = ID; { (r, f) }

primtype:
| INT { IntT }
| BOOL { BoolT }
| STRING { StringT }

pred:
| x = ID; COLON; t = primtype { Var (x, t) }
| f = field; { Field f }
| p1 = pred; EQ; p2 = pred { Binop (Eq, p1, p2) }
| p1 = pred; LT; p2 = pred { Binop (Lt, p1, p2) }
| p1 = pred; LE; p2 = pred { Binop (Le, p1, p2) }
| p1 = pred; GT; p2 = pred { Binop (Gt, p1, p2) }
| p1 = pred; GE; p2 = pred { Binop (Ge, p1, p2) }
| p1 = pred; AND; p2 = pred { Varop (And, [p1; p2]) }
| p1 = pred; OR; p2 = pred { Varop (Or, [p1; p2]) }
