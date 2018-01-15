open Base

let exec : ?verbose : bool -> ?params : string list -> Postgresql.connection ->
  string -> string list list =
  fun ?(verbose=true) ?(params=[]) conn query ->
    let query = match params with
      | [] -> query
      | _ ->
        List.foldi params ~init:query ~f:(fun i q v ->
            String.substr_replace_all
              ~pattern:(Printf.sprintf "$%d" i) ~with_:v q)
    in
    if verbose then Stdio.print_endline query;
    let r = conn#exec query in
    match r#status with
    | Postgresql.Fatal_error -> failwith r#error
    | _ -> r#get_all_lst

let exec1 : ?verbose : bool -> ?params : string list -> Postgresql.connection ->
  string -> string list =
  fun ?verbose ?params conn query ->
    exec ?verbose ?params conn query
    |> List.map ~f:(function
        | [x] -> x
        | _ -> failwith "Unexpected query results.")

let exec2 : ?verbose : bool -> ?params : string list -> Postgresql.connection -> string -> (string * string) list =
  fun ?verbose ?params conn query ->
    exec ?verbose ?params conn query
    |> List.map ~f:(function
        | [x; y] -> (x, y)
        | _ -> failwith "Unexpected query results.")

type dtype =
  | DInt of { min_val : int; max_val : int; distinct : int }
  | DString of { min_bits : int; max_bits : int; distinct : int }
  | DTimestamp of { distinct : int }
  | DInterval of { distinct : int }
  | DBool of { distinct : int }
[@@deriving compare, sexp]

module Field = struct
  module T = struct
    type t = {
      name: string;
      dtype : dtype;
    } [@@deriving compare, sexp]
  end
  include T
  include Comparable.Make(T)

  let dummy = { name = ""; dtype = DBool { distinct = 0 } }
end

module Relation = struct
  type t = {
    name : string;
    fields : Field.t list;
    card : int;
  } [@@deriving compare, sexp]

  let dummy = { name = ""; fields = []; card = 0; }

  let from_db : Postgresql.connection -> string -> t =
    fun conn name ->
      let card =
        exec1 ~params:[name] conn "select count(*) from $0"
        |> List.hd_exn
        |> fun ct_s -> Int.of_string ct_s
      in
      let fields =
        exec2 ~params:[name] conn
          "select column_name, data_type from information_schema.columns where table_name='$0'"
        |> List.map ~f:(fun (field_name, dtype_s) ->
            let distinct =
              exec1 ~params:[name; field_name] conn
                "select count(*) from (select distinct $1 from $0) as t"
              |> List.hd_exn
              |> fun ct_s -> Int.of_string ct_s
            in
            let dtype = match dtype_s with
              | "character varying" ->
                let min_bits, max_bits =
                  exec2 ~params:[field_name; name] conn
                    "select min(l), max(l) from (select bit_length($0) as l from $1) as t"
                  |> List.hd_exn
                  |> fun (x, y) -> Int.of_string x, Int.of_string y
                in
                DString { distinct; min_bits; max_bits }
              | "integer" ->
                let min_val, max_val =
                  exec2 ~params:[field_name; name] conn
                    "select min($0), max($0) from $1"
                  |> List.hd_exn
                  |> fun (x, y) -> Int.of_string x, Int.of_string y
                in
                DInt { distinct; min_val; max_val }
              | "timestamp without time zone" -> DTimestamp { distinct }
              | "interval" -> DInterval { distinct }
              | "boolean" -> DBool { distinct }
              | s -> failwith (Printf.sprintf "Unknown dtype %s" s)
            in
            Field.({ name = field_name; dtype }))
      in
      { name; fields; card }
end
