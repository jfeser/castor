%{
    open Parser_utils
%}

%token <string> ID
%token <int> INT
%token LPAREN RPAREN COMMA COLON EOF

%start <(string * string list * int option) list> transforms_eof
%%

transforms_eof:
| x = transforms; EOF { x }

transforms:
| x = separated_list(COMMA, transform) { x }

transform:
| n = ID;
  a = delimited(LPAREN, separated_list(COMMA, ID), RPAREN)?;
  i = preceded(COLON, INT)?
        { (n, Base.Option.value ~default:[] a, i) }
| error { error "Expected a transform." $startpos }

