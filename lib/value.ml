open Base

type t = Int of int | String of string | Bool of bool | Null
[@@deriving compare, hash, sexp]

let to_sql = function
  | `Int x -> Int.to_string x
  | `String x -> String.escaped x
  | _ -> failwith "unimplemented"

let to_int = function Int x -> x | _ -> failwith "Not an int."

let to_bool = function Bool x -> x | _ -> failwith "Not a bool."
