%{

    open Parser_utils
%}

%token <string> ID
%token <int> INT
%token <Cozy.binop> BINOP
%token LPAREN RPAREN LCURLY RCURLY ARROW COLON QUESTION DOT COMMA EOF LSQUARE RSQUARE EQ QUOTE
%token MAP FLATMAP FILTER DISTINCT SUM LEN THE LET IN

%start <Cozy.query> query_eof
%%

query_eof:
  | x = query; EOF { x }

query:
  | MAP f=lambda q=query { `Map (f, q) }
  | FILTER f=lambda q=query { `Filter (f, q) }
  | FLATMAP f=qlambda q=query { `Flatmap (f, q) }
  | DISTINCT q=query { `Distinct q }
  | LPAREN q=query RPAREN { q }
  | i=ID { `Table i }
  | LSQUARE RSQUARE { `Empty }
  | error { error "Expected a query." $startpos }

qlambda:
  | LCURLY arg=ID ARROW body=query RCURLY { `Lambda (arg, body) }
  | LCURLY ID ARROW expr error { error "Expected a }." $startpos }
  | LCURLY ID ARROW error { error "Expected an expr." $startpos }
  | LCURLY ID error { error "Expected an ->." $startpos }
  | LCURLY error { error "Expected an id." $startpos }
  | error { error "Expected a {." $startpos }

lambda:
  | LCURLY arg=ID ARROW body=expr RCURLY { `Lambda (arg, body) }
  | LCURLY ID ARROW expr error { error "Expected a }." $startpos }
  | LCURLY ID ARROW error { error "Expected an expr." $startpos }
  | LCURLY ID error { error "Expected an ->." $startpos }
  | LCURLY error { error "Expected an id." $startpos }
  | error { error "Expected a {." $startpos }

expr:
  | x=ID { `Var x }
  | x=INT { `Int x }
  | SUM q=query { `Sum q }
  | LEN q=query { `Len q }
  | THE q=query { `The q }
  | id=ID LPAREN args=separated_list(COMMA, expr) RPAREN { `Call (id, args) }
  | LPAREN args=separated_list(COMMA, expr) RPAREN { `Tuple args }
  | x=expr DOT i=INT { `TupleGet (x, i) }
  | e1=expr op=BINOP e2=expr { `Binop (op, e1, e2) }
  | e1=expr QUESTION e2=expr COLON e3=expr { `Cond (e1, e2, e3) }
  | LPAREN e=expr RPAREN { e }
  | LET id=ID EQ e1=expr IN e2=expr { `Let (id, e1, e2) }
  | QUOTE QUOTE { `String "" }
