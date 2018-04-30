open Base
open Printf

open Db
open Layout

type t =
  | Tuple of Field.t list
  | Table of t * Layout.table

let eval_relation : Postgresql.connection -> Field.t list -> string -> Tuple.t list =
  fun conn fields query ->
    exec ~verbose:false conn query
    |> List.mapi ~f:(fun i vs ->
        let m_values = List.map2 vs fields ~f:(fun v f ->
            let pval = if String.(v = "") then `Null else
                match f.dtype with
                | DInt -> `Int (Int.of_string v)
                | DString -> `String v
                | DBool ->
                  begin match v with
                    | "t" -> `Bool true
                    | "f" -> `Bool false
                    | _ -> failwith "Unknown boolean value."
                  end
                | _ -> `Unknown v
            in
            let value = Value.({ rel = Relation.dummy; field = f; value = pval }) in
            value)
        in
        match m_values with
        | Ok v -> v
        | Unequal_lengths ->
          Error.(create "Unexpected tuple width." (List.length fields, List.length vs)
                   [%sexp_of:int * int] |> raise))

let to_layout : Postgresql.connection -> t -> Layout.t = fun conn al ->
  let rec f tf = function
    | Tuple fs ->
      let query =
        List.map fs ~f:(fun f -> sprintf "%s.%s" f.relation.name f.name)
        |> String.concat ~sep:", "
        |> (fun fs_str -> sprintf "select %s from %s" fs_str
               (List.hd_exn fs).relation.name)
        |> tf
      in
      eval_relation conn fs query
      |> List.map ~f:(fun tup -> cross_tuple (List.map ~f:(fun v -> of_value v) tup))
      |> unordered_list
    | Table (value, ({ field = key; lookup } as t)) ->
      let key_query = sprintf "select %s from %s" key.name key.relation.name |> tf in
      let keys = eval_relation conn [key] key_query |> List.map ~f:(fun [x] -> x) in
      let map = List.map keys ~f:(fun key_value ->
          let value_tf q =
            sprintf "select * from (%s) as t where %s = %s"
              q key.name (Value.to_sql key_value)
            |> tf
          in
          key_value, f value_tf value)
                |> Map.of_alist_exn (module ValueMap.Elem)
      in
      table map t
  in
  f (fun x -> x) al

let partition : PredCtx.Key.t -> Field.t -> t -> t = fun k f l ->
  Table (l, { field = f; lookup = k })

