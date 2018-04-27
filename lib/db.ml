open Base
open Printf

module Pervasives = Caml.Pervasives
open Bin_prot.Std

open Collections

let subst_params : string list -> string -> string = fun params query ->
  match params with
  | [] -> query
  | _ -> List.foldi params ~init:query ~f:(fun i q v ->
      String.substr_replace_all ~pattern:(Printf.sprintf "$%d" i) ~with_:v q)

let exec_psql : ?params : string list -> db:string -> string -> int =
  fun ?(params=[]) ~db query ->
    let query = sprintf "psql -d %s -c \"%s\"" db (subst_params params query) in
    Logs.debug (fun m -> m "Executing query: %s" query);
    Caml.Sys.command query

let rec exec : ?max_retries : int -> ?verbose : bool -> ?params : string list -> Postgresql.connection -> string -> string list list =
  fun ?(max_retries=0) ?(verbose=true) ?(params=[]) conn query ->
    let query = subst_params params query in
    Logs.debug (fun m -> m "Executing query: %s" query);
    let r = conn#exec query in
    match r#status with
    | Fatal_error ->
      Error.create "Postgres fatal error." (r#error, query) [%sexp_of:string * string]
      |> Error.raise
    | Nonfatal_error -> begin match r#error_code with
        | SERIALIZATION_FAILURE
        | DEADLOCK_DETECTED ->
          (* See:
             https://www.postgresql.org/message-id/1368066680.60649.YahooMailNeo%40web162902.mail.bf1.yahoo.com
          *)
          if max_retries > 0 then
            exec ~max_retries:(max_retries - 1) ~verbose conn query
          else
            Error.create "Transaction failed." query [%sexp_of:string] |> Error.raise
        | e ->
          Logs.warn (fun m -> m "Postgres error (nonfatal): %s"
                        (Postgresql.Error_code.to_string e)); []
      end
    | Tuples_ok -> r#get_all_lst
    | Single_tuple -> Logs.debug (fun m -> m "Returning single tuple."); r#get_all_lst
    | Bad_response -> Error.create "Bad response." query [%sexp_of:string] |> Error.raise
    | s -> Logs.debug (fun m -> m "Returning nothing: %s" (Postgresql.result_status s)); []

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

let exec1_first : ?verbose : bool -> ?params : string list -> Postgresql.connection -> string -> string =
  fun ?verbose ?params conn query ->
    match exec ?verbose ?params conn query with
    | [[x]] -> x
    | r -> Error.create "Unexpected query results." r [%sexp_of:string list list] |> Error.raise

type dtype =
  | DInt
  | DRational
  | DFloat
  | DString
  | DTimestamp
  | DDate
  | DInterval
  | DBool
[@@deriving compare, hash, sexp, bin_io]

module Field = struct
  module T = struct
    type t = {
      name: string;
      dtype : dtype;
    } [@@deriving compare, hash, sexp, bin_io]
  end
  include T
  include Comparable.Make(T)

  let dummy = { name = ""; dtype = DBool }

  let to_string : t -> string = fun f -> f.name
end

module Relation = struct
  module T = struct
    type t = {
      name : string;
      fields : Field.t list;
    } [@@deriving compare, hash, sexp, bin_io]
  end
  include T
  include Comparable.Make(T)

  let dummy = { name = ""; fields = []; }

  let from_db : Postgresql.connection -> string -> t =
    fun conn name ->
      let fields =
        exec2 ~params:[name] conn
          "select column_name, data_type from information_schema.columns where table_name='$0'"
        |> List.map ~f:(fun (field_name, dtype_s) ->
            let dtype = match dtype_s with
              | "character" | "character varying" | "varchar" | "text" -> DString
              | "integer" | "smallint" | "bigint" -> DInt
              | "numeric" -> DRational
              | "real" -> DFloat
              | "double" -> DFloat
              | "timestamp without time zone" -> DTimestamp
              | "date" -> DDate
              | "interval" -> DInterval
              | "boolean" -> DBool
              | s -> failwith (Printf.sprintf "Unknown dtype %s" s)
            in
            Field.({ name = field_name; dtype }))
      in
      { name; fields }

  let sample : ?seed:int -> Postgresql.connection -> int -> t -> unit =
    fun ?(seed = 0) conn size r ->
      exec conn ~params:[Int.to_string seed] "set seed to $0" |> ignore;
      let query = {|
        create temp table if not exists $0 as (select * from $0 order by random() limit $1)
      |} in
      exec conn ~params:[r.name; Int.to_string size] query |> ignore

  let field_exn : t -> string -> Field.t = fun r n -> 
    List.find_exn r.fields ~f:(fun f -> String.(f.name = n))
end

type primvalue =
  [`Int of int | `String of string | `Bool of bool | `Unknown of string | `Null]
[@@deriving compare, hash, sexp, bin_io]

let primvalue_to_sql : primvalue -> string = function
  | `Int x -> Int.to_string x
  | `String x -> String.escaped x
  | _ -> failwith "unimplemented"

module Value = struct
  module T = struct
    type t = {
      rel : Relation.t;
      field : Field.t;
      value : primvalue;
    } [@@deriving compare, hash, sexp, bin_io]
  end
  include T
  include Comparator.Make(T)

  let of_int_exn : int -> t = fun x ->
    { rel = Relation.dummy; field = Field.dummy; value = `Int x }

  let to_int_exn : t -> int = function
    | { value = `Int x } -> x
    | v -> Error.create "Expected an int." v [%sexp_of:t] |> Error.raise
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
  module T = struct
    type t = Field.t list [@@deriving sexp]

    (** Schemas are compared as bags. *)
    let compare : t -> t -> int = fun s1 s2 ->
      [%compare:Field.t list]
        (List.sort ~compare:[%compare:Field.t] s1)
        (List.sort ~compare:[%compare:Field.t] s2)
  end
  include T
  include Comparable.Make(T)

  let to_string : t -> string = fun s ->
    List.map s ~f:(fun f -> f.name)
    |> String.concat ~sep:", "
    |> sprintf "[%s]"

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
    Int.(tot > utot)

  let field_idx : t -> Field.t -> int option = fun s f ->
    List.find_mapi s ~f:(fun i f' -> if Field.equal f f' then Some i else None)

  let field_idx_exn : t -> Field.t -> int = fun s f ->
    Option.value_exn (field_idx s f)
      ~error:(Error.create "Field not in schema." (f, s) [%sexp_of:Field.t * t])
end
