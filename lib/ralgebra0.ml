open Core
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
  | Add
  | Sub
  | Mul
  | Div
  | Mod
[@@deriving compare, sexp, bin_io, hash]

type 'f pred =
  | Var of TypedName.t
  | Field of 'f
  | Int of int
  | Bool of bool
  | String of string
  | Binop of (op * 'f pred * 'f pred)
  | Varop of (op * 'f pred list)
[@@deriving compare, sexp, bin_io, hash]

type 'f agg =
  | Count
  | Key of 'f
  | Sum of 'f
  | Avg of 'f
  | Min of 'f
  | Max of 'f
[@@deriving compare, sexp, bin_io, hash]

type ('f, 'r, 'l) t =
  | Project of 'f list * ('f, 'r, 'l) t
  | Filter of 'f pred * ('f, 'r, 'l) t
  | EqJoin of 'f * 'f * ('f, 'r, 'l) t * ('f, 'r, 'l) t
  | Scan of 'l
  | Concat of ('f, 'r, 'l) t list
  | Relation of 'r
  | Count of ('f, 'r, 'l) t
  | Agg of 'f agg list * 'f list * ('f, 'r, 'l) t
[@@deriving compare, sexp, bin_io, hash]
