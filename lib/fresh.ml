open Core

type t = int Hashtbl.M(String).t

let create () = Hashtbl.create (module String)

let name tbl fmt =
  let key = string_of_format fmt in
  (* if String.exists ~f:Char.is_digit key then
   *   Error.create "Name template could collide." key [%sexp_of: string]
   *   |> Error.raise ; *)
  let ctr = Hashtbl.find tbl key |> Option.value ~default:0 in
  let name = sprintf fmt ctr in
  Hashtbl.set tbl ~key ~data:(ctr + 1);
  name

let int tbl = name tbl "%d" |> Int.of_string
let reset = Hashtbl.clear
