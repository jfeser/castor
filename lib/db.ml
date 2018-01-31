open Base
open Collections

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
    Logs.debug (fun m -> m "Executing query: %s" query);
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
[@@deriving compare, hash, sexp]

module Field = struct
  module T = struct
    type t = {
      name: string;
      dtype : dtype;
    } [@@deriving compare, hash, sexp]
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
  } [@@deriving compare, hash, sexp]

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

  let field_exn : t -> string -> Field.t = fun r n -> 
    List.find_exn r.fields ~f:(fun f -> String.(f.name = n))
end

type primvalue =
  [`Int of int | `String of string | `Bool of bool | `Unknown of string]
[@@deriving compare, hash, sexp]

module Value = struct
  module T = struct
    type t = {
      rel : Relation.t;
      field : Field.t;
      idx : int;
      value : primvalue;
    } [@@deriving compare, hash, sexp]
  end
  include T
  include Comparator.Make(T)
end

module Tuple = struct
  module T = struct
    type t = Value.t list [@@deriving compare, sexp]
  end
  include T
  include Comparable.Make(T)

  module ValueF = struct
    module T = struct
      type t = Value.t
      let compare v1 v2 = Field.compare v1.Value.field v2.Value.field
      let sexp_of_t = Value.sexp_of_t
    end
    include T
    include Comparable.Make(T)
  end

  let field : t -> Field.t -> Value.t option = fun t f ->
    List.find t ~f:(fun v -> Field.(f = v.field))

  let field_exn : t -> Field.t -> Value.t = fun t f ->
    Option.value_exn
      (List.find t ~f:(fun v -> Field.(f = v.field)))

  let merge : t -> t -> t = fun t1 t2 ->
    List.append t1 t2 |> List.dedup (module ValueF)

  let merge_many : t list -> t = fun ts ->
    List.concat ts |> List.dedup (module ValueF)
end

module Schema = struct
  type t = Field.t list [@@deriving compare, sexp]

  let of_tuple : Tuple.t -> t = List.map ~f:(fun v -> v.Value.field)
  let of_relation : Relation.t -> t = fun r -> r.fields

  let has_field : t -> Field.t -> bool = List.mem ~equal:Field.(=)

  let overlaps : t list -> bool = fun schemas ->
    let schemas = List.map schemas ~f:(Set.of_list (module Field)) in
    let tot = List.sum (module Int) schemas ~f:Set.length in
    let utot =
      schemas
      |> Set.union_list (module Field)
      |> Set.length
    in
    tot > utot

  let field_idx : t -> Field.t -> int option = fun s f ->
    List.find_mapi s ~f:(fun i f' -> if Field.equal f f' then Some i else None)

  let field_idx_exn : t -> Field.t -> int = fun s f ->
    Option.value_exn (field_idx s f)
end
