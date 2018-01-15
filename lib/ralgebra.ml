open Base
open Printf
open Db

module T = struct
  type op =
    | Eq
    | Lt
    | Le
    | Gt
    | Ge
    | And
    | Or
  [@@deriving compare, sexp]

  type pred =
    | Var of string
    | Field of Field.t
    | Binop of (op * pred * pred)
    | Varop of (op * pred list)
  [@@deriving compare, sexp]

  type t =
    | Project of Field.t list * t
    | Filter of pred * t
    | EqJoin of Field.t * Field.t * t * t
    | Scan of Layout.t
    | Concat of t list
    | Relation of Relation.t
  [@@deriving compare, sexp]
end
include T
include Comparable.Make(T)

let rec pred_fields : pred -> Field.t list = function
  | Var _ -> []
  | Field f -> [f]
  | Binop (_, p1, p2) -> (pred_fields p1) @ (pred_fields p2)
  | Varop (_, ps) -> List.concat_map ps ~f:pred_fields

let op_to_string : op -> string = function
  | Eq -> "="
  | Lt -> "<"
  | Le -> "<="
  | Gt -> ">"
  | Ge -> ">="
  | And -> "And"
  | Or -> "Or"

let rec pred_to_string : pred -> string = function
  | Var v -> v
  | Field f -> f.name
  | Binop (op, p1, p2) ->
    let os = op_to_string op in
    let s1 = pred_to_string p1 in
    let s2 = pred_to_string p2 in
    sprintf "%s %s %s" s1 os s2
  | Varop (op, ps) ->
    let ss = List.map ps ~f:pred_to_string |> String.concat ~sep:", " in
    sprintf "%s(%s)" (op_to_string op) ss

let rec to_string : t -> string =
  function
  | Project (fs, r) -> sprintf "Project(%s)" (to_string r)
  | Filter (p, r) -> sprintf "Filter(%s, %s)" (pred_to_string p) (to_string r)
  | EqJoin (f1, f2, r1, r2) ->
    sprintf "EqJoin(%s, %s)" (to_string r1) (to_string r2)
  | Scan l -> sprintf "Scan(%s)" (Layout.to_string l)
  | Concat rs ->
    List.map rs ~f:to_string |> String.concat ~sep:", " |> sprintf "Concat(%s)"
  | Relation r -> r.name

let rec layouts : t -> Layout.t list = function
  | Project (_, r)
  | Filter (_, r) -> layouts r
  | EqJoin (_,_, r1, r2) -> layouts r1 @ layouts r2
  | Scan l -> [l]
  | Concat rs -> List.map rs ~f:layouts |> List.concat
  | Relation _ -> []
