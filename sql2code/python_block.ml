open Core

type t =
  | Stmts of string list
  | Block of {header: string; body: t}
  | Seq of t list

let empty = Stmts []

let to_string ?(width = 4) x =
  let buf = Buffer.create 0 in
  let rec to_string indent =
    let indent_str = String.init (indent * width) ~f:(Fn.const ' ') in
    let add_line s =
      Buffer.add_string buf indent_str ;
      Buffer.add_string buf s ;
      Buffer.add_char buf '\n'
    in
    function
    | Stmts stmts ->
        List.iter stmts ~f:add_line
    | Block {header; body} ->
        add_line header ;
        to_string (indent + 1) body
    | Seq xs ->
        List.iter xs ~f:(to_string indent)
  in
  to_string 0 x ; Buffer.contents buf
