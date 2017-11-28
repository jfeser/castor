%{
    open Expr0
%}

%token <string> ID

%token LBRACKET
%token RBRACKET
%token LPAREN
%token RPAREN
%token STAR
%token BAR
%token LARROW
%token COMMA
%token EOF

%start <Expr0.t> expr_eof
%%

expr_eof:
| x = expr; EOF { x }

expr:
| x = ID; { Id x }
| LPAREN; x = separated_nonempty_list(COMMA, expr); RPAREN { Tuple x }
| LBRACKET;
  body = expr;
  BAR;
  binds = separated_nonempty_list(COMMA, binding);
  RBRACKET { Comp { body; binds } }
| STAR; x = expr { Ptr x }

binding:
| lhs = ID; LARROW; rhs = expr { ([lhs], rhs) }
| LPAREN; lhs = separated_nonempty_list(COMMA, ID); RPAREN; LARROW; rhs = expr { (lhs, rhs) }
