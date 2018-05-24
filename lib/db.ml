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

(** Process postgres errors for queries which do not need to retry. *)
let process_errors r = match (r#status : Postgresql.result_status) with
  | Fatal_error | Nonfatal_error ->
    let err = r#error in
    let msg = sprintf "Postgres error: %s" err in
    Logs.err (fun m -> m "%s" msg);
    Error.of_string msg |> Error.raise
  | _ -> r

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

type field_t = {
  name : string;
  dtype : dtype;
  relation : relation_t;
}
and relation_t = {
  name : string;
  fields : field_t list;
} [@@deriving compare, hash, sexp, bin_io]

module Relation = struct
  module T = struct
    type t = relation_t [@@deriving compare, hash, sexp, bin_io]
  end
  include T
  include Comparable.Make(T)

  let dummy = { name = ""; fields = []; }

  let from_db : Postgresql.connection -> string -> t =
    fun conn name ->
      let rel = { name; fields = [] } in
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
            { name = field_name; dtype; relation = rel })
      in
      (* TODO: Remove this. *)
      Obj.set_field (Obj.repr rel) 1 (Obj.repr fields);
      rel

  let sample : ?seed:int -> Postgresql.connection -> int -> t -> unit =
    fun ?(seed = 0) conn size r ->
      exec conn ~params:[Int.to_string seed] "set seed to $0" |> ignore;
      let query = {|
        create temp table if not exists $0 as (select * from $0 order by random() limit $1)
      |} in
      exec conn ~params:[r.name; Int.to_string size] query |> ignore

  let field_exn : t -> string -> field_t = fun r n ->
    match List.find r.fields ~f:(fun f -> String.(f.name = n)) with
    | Some f -> f
    | None ->
      Error.create "Field not found." (n, r.name) [%sexp_of:string * string]
      |> Error.raise

  (* For testing only! *)
  let of_name : string -> t = fun n -> { dummy with name = n }
end

module Field = struct
  module T = struct
    type t = field_t = {
      name : string;
      dtype : dtype; [@compare.ignore]
      relation : Relation.t; [@compare.ignore]
    } [@@deriving compare, hash, sexp, bin_io]
  end
  include T
  include Comparable.Make(T)

  let dummy = { name = ""; dtype = DBool; relation = Relation.dummy }

  let to_string : t -> string = fun f -> f.name

  (* For testing only! *)
  let of_name : string -> t = fun n -> { dummy with name = n }
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

  let of_primvalue v = { value = v; rel = Relation.dummy; field = Field.dummy }

  let of_int_exn : int -> t = fun x ->
    { rel = Relation.dummy; field = Field.dummy; value = `Int x }

  let to_int_exn : t -> int = function
    | { value = `Int x } -> x
    | v -> Error.create "Expected an int." v [%sexp_of:t] |> Error.raise

  let to_sql : t -> string = fun { rel; field; value } ->
    primvalue_to_sql value
end

module Tuple = struct
  module T = struct
    type t = Value.t list [@@deriving compare, sexp, hash]
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

let result_to_tuples : Postgresql.result -> primvalue Map.M(String).t Seq.t = fun r ->
  Seq.range 0 r#ntuples ~stop:`exclusive
  |> Seq.unfold_with ~init:() ~f:(fun () tup_i -> 
      let tup = List.init r#nfields ~f:(fun field_i ->
          let value = r#getvalue tup_i field_i in
          let type_ = r#ftype field_i in
          let primval = match type_ with
            | BOOL ->
              begin match value with
                | "t" -> `Bool true
                | "f" -> `Bool false
                | _ -> failwith "Unknown boolean value."
              end
            | NAME
            | INT8 | INT2 | INT4 | NUMERIC -> `Int (Int.of_string value)
            | CHAR | TEXT | VARCHAR -> `String value
            | FLOAT4 | FLOAT8
            |BYTEA
            |INT2VECTOR
            |REGPROC|OID|TID
            |XID|CID|OIDVECTOR|JSON
            |POINT|LSEG|PATH|BOX
            |POLYGON|LINE
            |ABSTIME|RELTIME|TINTERVAL
            |UNKNOWN|CIRCLE|CASH|MACADDR
            |INET|CIDR|ACLITEM|BPCHAR
            |DATE|TIME|TIMESTAMP
            |TIMESTAMPTZ|INTERVAL|TIMETZ|BIT
            |VARBIT|REFCURSOR
            |REGPROCEDURE|REGOPER|REGOPERATOR
            |REGCLASS|REGTYPE|RECORD|CSTRING
            |ANY|ANYARRAY|VOID|TRIGGER
            |LANGUAGE_HANDLER|INTERNAL|OPAQUE
            |ANYELEMENT|JSONB -> `Unknown value
          in
          (r#fname field_i, primval))
                |> Map.of_alist_exn (module String)
      in
      Yield (tup, ()))

let exec_cursor :
  'a. ?batch_size : int
  -> ?verbose : bool
  -> ?params : string list
  -> Postgresql.connection -> string -> primvalue Map.M(String).t Seq.t =
  let fresh = Fresh.create () in
  fun ?(batch_size=1000) ?(verbose=true) ?(params=[]) conn query ->
    let open Stdio in
    let query = subst_params params query in
    Logs.debug (fun m -> m "Executing cursor query: %s" query);

    let cur = Fresh.name fresh "cur%d" in
    let declare_query = sprintf "declare %s cursor with hold for %s;" cur query in
    let fetch_query = sprintf "fetch %d from %s;" batch_size cur in
    let close_query = sprintf "close %s;" cur in

    conn#exec declare_query |> process_errors |> ignore;
    let db_idx = ref 1 in
    let seq = Seq.unfold_step ~init:(`Not_done 1) ~f:(function
        | `Done -> Done
        | `Not_done idx when idx <> !db_idx ->
          let move_query = sprintf "move absolute %d %s;" idx cur in
          conn#exec move_query |> process_errors |> ignore;
          db_idx := idx; Skip (`Not_done idx)
        | `Not_done idx ->
          let r = conn#exec fetch_query |> process_errors in
          let tups = result_to_tuples r in
          db_idx := !db_idx + r#ntuples;
          let idx = idx + r#ntuples in
          let state = if r#ntuples < batch_size then `Done else `Not_done idx in
          Yield (tups, state)) |> Seq.concat
    in
    (* Caml.Gc.finalise (fun _ ->
     *     printf "Deallocating cursor!\n";
     *     conn#exec close_query |> process_errors |> ignore)
     *   seq; *)
    seq
