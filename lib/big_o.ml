open Core

type t =
  | Const
  | Var of string
  | Poly of t * int
  | Log of t
  | Prod of t * t
  | Sum of t * t
[@@deriving compare, equal, sexp]

let ( + ) x y =
  match (x, y) with
  | Const, z | z, Const -> z
  | _ -> if [%equal: t] x y then x else Sum (x, y)

let ( * ) x y =
  match (x, y) with
  | Const, z | z, Const -> z
  | _ -> if [%equal: t] x y then Poly (x, 2) else Prod (x, y)

let sum xs = List.fold_left xs ~init:Const ~f:( + )
