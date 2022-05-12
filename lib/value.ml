open Core
open Collections

module T = struct
  type t =
    | Int of int
    | Date of Date.t
    | String of string
    | Bool of bool
    | Fixed of Fixed_point.t
    | Null
  [@@deriving compare, equal, hash, sexp, variants]
end

include T
module C = Comparable.Make (T)
module O : Comparable.Infix with type t := t = C

let of_pred = function
  | `Int x -> Int x
  | `String x -> String x
  | `Bool x -> Bool x
  | `Fixed x -> Fixed x
  | `Null _ -> Null
  | `Date x -> Date x
  | _ -> failwith "Not a value."

let to_sql = function
  | Int x -> Int.to_string x
  | Fixed x -> Fixed_point.to_string x
  | Date x -> sprintf "date('%s')" (Date.to_string x)
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> sprintf "'%s'" s
  | Null -> "null"

let to_param = function
  | Int x -> Int.to_string x
  | Fixed x -> Fixed_point.to_string x
  | Date x -> Date.to_string x
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> s
  | Null -> "null"

let pp fmt v = Format.fprintf fmt "%s" (to_sql v)
let to_int = function Int x -> Some x | _ -> None
let to_date = function Date x -> Some x | _ -> None
let to_bool = function Bool x -> Some x | _ -> None
let to_string = function String x -> Some x | _ -> None

let type_error expected v =
  Error.create (sprintf "Expected a %s." expected) v [%sexp_of: t]
  |> Error.raise

let to_int_exn = function Int x -> x | v -> type_error "int" v
let to_date_exn = function Date x -> x | v -> type_error "date" v
let to_bool_exn = function Bool x -> x | v -> type_error "bool" v
let to_string_exn = function String x -> x | v -> type_error "string" v

let to_pred =
  let module A = Ast in
  function
  | Int x -> `Int x
  | String x -> `String x
  | Bool x -> `Bool x
  | Fixed x -> `Fixed x
  | Null -> `Null None
  | Date x -> `Date x

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

let ( % ) x y = to_int_exn x % to_int_exn y |> int
