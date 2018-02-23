open Base
open Db
open Type0

exception ParseError of string * int * int

type op =
  | Eq
  | Lt
  | Le
  | Gt
  | Ge
  | And
  | Or
[@@deriving compare, sexp]

type 'f pred =
  | Var of TypedName.t
  | Field of 'f
  | Int of int
  | Bool of bool
  | String of string
  | Binop of (op * 'f pred * 'f pred)
  | Varop of (op * 'f pred list)
[@@deriving compare, sexp]

type ('f, 'r) t =
  | Project of 'f list * ('f, 'r) t
  | Filter of 'f pred * ('f, 'r) t
  | EqJoin of 'f * 'f * ('f, 'r) t * ('f, 'r) t
  | Scan of Layout.t
  | Concat of ('f, 'r) t list
  | Relation of 'r
  | Count of ('f, 'r) t
[@@deriving compare, sexp]
