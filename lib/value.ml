open Base
open Collections
open Printf

module T = struct
  type t =
    | Int of int
    | Date of Date.t
    | String of string
    | Bool of bool
    | Fixed of Fixed_point.t
    | Null
  [@@deriving compare, hash, sexp, variants]
end

include T
module C = Comparable.Make (T)

module O : Comparable.Infix with type t := t = C

let of_pred =
  let module A = Abslayout0 in
  function
  | A.Int x -> Int x
  | A.String x -> String x
  | A.Bool x -> Bool x
  | A.Fixed x -> Fixed x
  | A.Null -> Null
  | A.Date x -> Date x
  | _ -> failwith "Not a value."

let to_sql = function
  | Int x -> Int.to_string x
  | Fixed x -> Fixed_point.to_string x
  | Date x -> sprintf "date('%s')" (Core.Date.to_string x)
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> sprintf "'%s'" s
  | Null -> "null"

let to_int = function Int x -> x | _ -> failwith "Not an int."

let to_date = function Date x -> x | _ -> failwith "Not an date."

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

let ( + ) x y =
  match (x, y) with
  | Int a, Int b -> Int (a + b)
  | Fixed a, Fixed b -> Fixed Fixed_point.(a + b)
  | Int a, Fixed b | Fixed b, Int a -> Fixed Fixed_point.(b + of_int a)
  | Date a, Int b -> Date (Date.add_days a b)
  | _ -> Error.create "Cannot +" (x, y) [%sexp_of: t * t] |> Error.raise

let neg = function
  | Int a -> Int (Int.neg a)
  | Fixed a -> Fixed Fixed_point.(-a)
  | Date _ | String _ | Bool _ | Null -> failwith "Cannot neg"

let ( - ) x y = x + neg y

let ( * ) x y =
  match (x, y) with
  | Int a, Int b -> Int (a * b)
  | Fixed a, Fixed b -> Fixed Fixed_point.(a * b)
  | Int a, Fixed b | Fixed b, Int a -> Fixed Fixed_point.(b * of_int a)
  | _ -> failwith "Cannot *"

let ( / ) x y =
  match (x, y) with
  | Int a, Int b -> Fixed (Fixed_point.of_float Float.(of_int a / of_int b))
  | Fixed a, Fixed b -> Fixed Fixed_point.(a / b)
  | Fixed a, Int b -> Fixed Fixed_point.(a / of_int b)
  | Int a, Fixed b -> Fixed Fixed_point.(of_int a / b)
  | _ -> Error.create "Cannot /" (x, y) [%sexp_of: t * t] |> Error.raise

let ( % ) x y = to_int x % to_int y |> int
