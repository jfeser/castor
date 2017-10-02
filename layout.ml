open Base
open Base.Polymorphic_compare
open Base.Printf
open Postgresql

exception Error of string

type id = string [@@deriving show]

type dtype =
  | DInt of { min_val : int; max_val : int; distinct : int }
  | DString of { min_bits : int; max_bits : int; distinct : int }
  | DTimestamp of { distinct : int }
  | DInterval of { distinct : int }
  | DBool of { distinct : int }
[@@deriving show]

type relation = {
  attrs : (id * dtype) list;
  card : int;
} [@@deriving show]

type expr =
  | And of expr list
  | Or of expr list
  | Not of expr
  | Eq of expr * expr
  | Gt of expr * expr
  | Lt of expr * expr
  | Int of int
  | Float of float
  | String of string

type condition =
  | Filter of expr
  | OrderBy of ((id * id) list * [`Desc | `Asc])
  | Limit of int

type t =
  | Relation of relation
  | Comp of {
      head : [`Attr of (id * id) | `Idx of (id * int)] list;
      packing : [`Packed | `Aligned of int];
      vars : (id * t) list;
      conds : condition list
    }

type layout =
  | Bitstring of int
  | Block of { cols : (int * layout) list; count : int }

let exec : ?verbose : bool -> ?params : string list -> connection -> string -> string list list =
  fun ?(verbose=true) ?(params=[]) conn query ->
    let query = match params with
      | [] -> query
      | _ ->
          List.foldi params ~init:query ~f:(fun i q v ->
            String.substr_replace_all ~pattern:(sprintf "$%d" i) ~with_:v q)
    in
    if verbose then print_endline query;
    let r = conn#exec query in
    match r#status with
    | Postgresql.Fatal_error -> failwith r#error
    | _ -> r#get_all_lst

let relation_from_db : connection -> string -> relation =
  fun conn name ->
    let card =
      exec ~params:[name] conn "select count(*) from $0"
      |> (fun ([ct_s]::_) -> int_of_string ct_s)
    in
    let attrs =
      exec ~params:[name] conn "select column_name, data_type from information_schema.columns where table_name='$0'"
      |> List.map ~f:(fun [attr_name; dtype_s] ->
          let distinct =
            exec ~params:[name; attr_name] conn "select count(*) from (select distinct $1 from $0) as t"
            |> (fun ([ct_s]::_) -> int_of_string ct_s)
          in
          let dtype = match dtype_s with
            | "character varying" ->
              let [min_bits; max_bits] =
                exec ~params:[attr_name; name] conn
                  "select min(l), max(l) from (select bit_length($0) as l from $1) as t"
                |> (fun (t::_) -> List.map ~f:int_of_string t)
              in
              DString { distinct; min_bits; max_bits }
            | "integer" ->
              let [min_val; max_val] =
                exec ~params:[attr_name; name] conn "select min($0), max($0) from $1"
                |> (fun (t::_) -> List.map ~f:int_of_string t)
              in
              DInt { distinct; min_val; max_val }
            | "timestamp without time zone" -> DTimestamp { distinct }
            | "interval" -> DInterval { distinct }
            | "boolean" -> DBool { distinct }
            | s -> raise (Error (sprintf "Unknown dtype %s" s))
          in
          (name, dtype))
    in
    { attrs; card }

let bytes : int -> int = fun x -> x * 8

let rec eval : t -> layout = function
  | Relation _ -> failwith "Expected a comprehension."
  | Comp { head; packing; vars; _ } ->
    let inputs =
      List.map vars ~f:(fun (name, comp) -> (name, eval comp))
    in


let () =
  try
    let conn = new connection ~dbname:"sam_analytics" () in
    let app_users = relation_from_db conn "app_users" in
    (* let app_user_scores = relation_from_db conn "app_user_scores" in
     * let app_user_device_settings = relation_from_db conn "app_user_device_settings" in *)
    print_endline "Loading relations...";
    print_endline (show_relation app_users);
    flush_all ()
  with Postgresql.Error e -> print_endline (string_of_error e)


