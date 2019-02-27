open Core
open Yojson.Basic
open Postgresql

type t = {nrows: int; cost: float}

let explain (conn : connection) query =
  let r : result = conn#exec (sprintf "explain (format json) %s" query) in
  let json_str =
    match r#status with
    | Single_tuple | Tuples_ok -> r#getvalue 0 0
    | _ ->
        let status = result_status r#status in
        Error.(
          create "Postgres error." (status, r#error, query)
            [%sexp_of: string * string * string]
          |> raise)
  in
  let json = from_string json_str in
  try
    let plan = Util.to_list json |> List.hd_exn |> Util.member "Plan" in
    let nrows = Util.member "Plan Rows" plan |> Util.to_int in
    let cost = Util.member "Total Cost" plan |> Util.to_number in
    {nrows; cost}
  with Util.Type_error _ as e -> Error.(of_exn e |> tag ~tag:json_str |> raise)
