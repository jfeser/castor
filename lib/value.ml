open Base
open Collections
open Printf

type t =
  | Int of int
  | Date of Date.t
  | String of string
  | Bool of bool
  | Fixed of Fixed_point.t
  | Null
[@@deriving compare, sexp]

let to_sql = function
  | Int x -> Int.to_string x
  | Fixed x -> Fixed_point.to_string x
  | Date x -> sprintf "date('%s')" (Core.Date.to_string x)
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> sprintf "'%s'" s
  | Null -> "null"

let to_int = function Int x -> x | _ -> failwith "Not an int."

let to_bool = function Bool x -> x | _ -> failwith "Not a bool."

let to_string = function String x -> x | _ -> failwith "Not a string."

let to_pred =
  let module A = Abslayout0 in
  function
  | Int x -> A.Int x
  | String x -> A.String x
  | Bool x -> A.Bool x
  | Fixed x -> A.Fixed x
  | Null -> A.Null
  | Date x -> A.Date x
