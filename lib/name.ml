open Base
open Printf
open Collections

module T = struct
  type t = Abslayout0.name =
    { relation: string option
    ; name: string
    ; type_: Type0.PrimType.t option [@compare.ignore] }
  [@@deriving compare, hash, sexp]
end

include T
include Comparator.Make (T)
module C = Comparable.Make (T)

module O : Comparable.Infix with type t := t = C

let create ?relation ?type_ name = {relation; name; type_}

let type_exn ({type_; _} as n) =
  match type_ with
  | Some t -> t
  | None -> Error.create "Missing type." n [%sexp_of: t] |> Error.raise

let rel_exn ({relation; _} as n) =
  match relation with
  | Some t -> t
  | None -> Error.create "Missing relation." n [%sexp_of: t] |> Error.raise

let to_var {relation; name; _} =
  match relation with Some r -> sprintf "%s_%s" r name | None -> name

let to_sql {relation; name; _} =
  match relation with
  | Some r -> sprintf "%s.\"%s\"" r name
  | None -> sprintf "\"%s\"" name

let pp fmt =
  let open Caml.Format in
  function
  | {relation= Some r; name; _} -> fprintf fmt "%s.%s" r name
  | {relation= None; name; _} -> fprintf fmt "%s" name

let of_lexbuf_exn lexbuf =
  try Ralgebra_parser.name_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
    raise e

let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

let fresh f fmt = create (Fresh.name f fmt)
