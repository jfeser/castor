open Base

type t =
  | Id of string
  | Tuple of t list
  | Comp of { body : t; binds: (string list * t) list }
  | Ptr of t
[@@deriving compare, sexp]
