open Base
open Printf
open Db

module T = struct
  type t = Abslayout0.name =
    {relation: string option; name: string; type_: Type0.PrimType.t option}
  [@@deriving sexp]
end

module Compare = struct
  module T = struct
    type t = T.t =
      {relation: string option; name: string; type_: Type0.PrimType.t option}
    [@@deriving compare, hash, sexp]
  end

  include T
  include Comparable.Make (T)
end

module Compare_no_type = struct
  module T = struct
    type t = T.t =
      { relation: string option
      ; name: string
      ; type_: Type0.PrimType.t option [@compare.ignore] }
    [@@deriving compare, hash, sexp]
  end

  include T
  include Comparable.Make (T)
end

module Compare_name_only = struct
  module T = struct
    type t = T.t =
      { relation: string option [@compare.ignore]
      ; name: string
      ; type_: Type0.PrimType.t option [@compare.ignore] }
    [@@deriving compare, hash, sexp]
  end

  include T
  include Comparable.Make (T)
end

include T

let create ?relation ?type_ name = {relation; name; type_}

let type_exn ({type_; _} as n) =
  match type_ with
  | Some t -> t
  | None -> Error.create "Missing type." n [%sexp_of: t] |> Error.raise

let to_var {relation; name; _} =
  match relation with Some r -> sprintf "%s_%s" r name | None -> name

let to_sql {relation; name; _} =
  match relation with
  | Some r -> sprintf "%s.\"%s\"" r name
  | None -> sprintf "\"%s\"" name

let of_lexbuf_exn lexbuf =
  try Ralgebra_parser.name_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
    raise e

let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

let of_field ?rel f = {relation= rel; name= f.Field.fname; type_= Some f.type_}
