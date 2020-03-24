open Ast

type binop = [ `Le | `Lt | `Ge | `Gt | `Add | `Sub | `Mul | `Div | `Eq | `And ]
[@@deriving sexp]

type expr =
  [ `Binop of binop * expr * expr
  | `Call of string * expr list
  | `Cond of expr * expr * expr
  | `Int of int
  | `Len of query
  | `Sum of query
  | `The of query
  | `Tuple of expr list
  | `TupleGet of expr * int
  | `Var of string
  | `Let of string * expr * expr
  | `String of string ]
[@@deriving sexp]

and 'a lambda = [ `Lambda of string * 'a ] [@@deriving sexp]

and query =
  [ `Distinct of query
  | `Filter of expr lambda * query
  | `Flatmap of query lambda * query
  | `Map of expr lambda * query
  | `Table of string
  | `Empty ]
[@@deriving sexp]

val cost : query -> Big_o.t

val to_string : string -> Name.t List.t -> t -> string
(** Convert a layout to a cozy query. Takes the name for the benchmark, a list
   of parameters, and a layout. *)
