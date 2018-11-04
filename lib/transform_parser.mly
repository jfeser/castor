%{
    open Parser_utils
%}

%token <string> ID
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
  i = preceded(COLON, ID)?
        {
          let open Base in
          (n, Option.value ~default:[] a, Option.map ~f:Int.of_string i)
        }
| error { error "Expected a transform." $startpos }

