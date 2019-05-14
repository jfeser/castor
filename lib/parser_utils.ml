open! Core
open Abslayout0

exception ParseError of string * int * int

let error msg pos =
  let col = pos.Lexing.pos_cnum - pos.pos_bol in
  raise (ParseError (msg, pos.pos_lnum, col))

let lex_error lexbuf msg =
  let pos = Lexing.lexeme_start_p lexbuf in
  let col = pos.pos_cnum - pos.pos_bol in
  raise (ParseError (msg, pos.pos_lnum, col))

let node _spos _epos r =
  let node = {node= r; meta= ref Univ_map.empty} in
  node

(* let node = Meta.set node Meta.start_pos spos in
   * Meta.set node Meta.end_pos epos *)
