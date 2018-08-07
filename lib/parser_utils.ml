exception ParseError of string * int * int

let error msg pos =
  let col = pos.Lexing.pos_cnum - pos.pos_bol in
  raise (ParseError (msg, pos.pos_lnum, col))

let lex_error lexbuf msg =
  let pos = Lexing.lexeme_start_p lexbuf in
  let col = pos.pos_cnum - pos.pos_bol in
  raise (ParseError (msg, pos.pos_lnum, col))

let node r = Abslayout0.{node= r; meta= ref Core.Univ_map.empty}
