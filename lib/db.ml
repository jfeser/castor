open Base
open Printf
module Pervasives = Caml.Pervasives
open Bin_prot.Std
open Collections

let () =
  Caml.Printexc.register_printer (function
    | Postgresql.Error e -> Some (Postgresql.string_of_error e)
    | _ -> None )

let subst_params : string list -> string -> string =
 fun params query ->
  match params with
  | [] -> query
  | _ ->
      List.foldi params ~init:query ~f:(fun i q v ->
          String.substr_replace_all ~pattern:(Printf.sprintf "$%d" i) ~with_:v q )

let exec_psql : ?params:string list -> db:string -> string -> int =
 fun ?(params = []) ~db query ->
  let query = sprintf "psql -d %s -c \"%s\"" db (subst_params params query) in
  Logs.debug (fun m -> m "Executing query: %s" query) ;
  Caml.Sys.command query

(** Process postgres errors for queries which do not need to retry. *)
let process_errors query r =
  match (r#status : Postgresql.result_status) with
  | Fatal_error | Nonfatal_error ->
      let err = r#error in
      let msg = sprintf "Postgres error: %s" err in
      Logs.err (fun m -> m "%s" msg) ;
      Error.of_string msg
      |> fun e -> Error.tag_arg e "query" query [%sexp_of: string] |> Error.raise
  | _ -> r

let rec exec :
       ?max_retries:int
    -> ?verbose:bool
    -> ?params:string list
    -> Postgresql.connection
    -> string
    -> string list list =
 fun ?(max_retries = 0) ?(verbose = true) ?(params = []) conn query ->
  let query = subst_params params query in
  Logs.debug (fun m -> m "Executing query: %s" query) ;
  let r = conn#exec query in
  match r#status with
  | Fatal_error ->
      Error.create "Postgres fatal error." (r#error, query)
        [%sexp_of: string * string]
      |> Error.raise
  | Nonfatal_error -> (
    match r#error_code with
    | SERIALIZATION_FAILURE | DEADLOCK_DETECTED ->
        if
          (* See:
             https://www.postgresql.org/message-id/1368066680.60649.YahooMailNeo%40web162902.mail.bf1.yahoo.com
          *)
          max_retries > 0
        then exec ~max_retries:(max_retries - 1) ~verbose conn query
        else
          Error.create "Transaction failed." query [%sexp_of: string] |> Error.raise
    | e ->
        Logs.warn (fun m ->
            m "Postgres error (nonfatal): %s" (Postgresql.Error_code.to_string e) ) ;
        [] )
  | Tuples_ok -> r#get_all_lst
  | Single_tuple ->
      Logs.debug (fun m -> m "Returning single tuple.") ;
      r#get_all_lst
  | Bad_response ->
      Error.create "Bad response." query [%sexp_of: string] |> Error.raise
  | s ->
      Logs.debug (fun m -> m "Returning nothing: %s" (Postgresql.result_status s)) ;
      []

let exec1 :
       ?verbose:bool
    -> ?params:string list
    -> Postgresql.connection
    -> string
    -> string list =
 fun ?verbose ?params conn query ->
  exec ?verbose ?params conn query
  |> List.map ~f:(function [x] -> x | _ -> failwith "Unexpected query results.")

let exec2 :
       ?verbose:bool
    -> ?params:string list
    -> Postgresql.connection
    -> string
    -> (string * string) list =
 fun ?verbose ?params conn query ->
  exec ?verbose ?params conn query
  |> List.map ~f:(function
       | [x; y] -> (x, y)
       | _ -> failwith "Unexpected query results." )

let exec1_first :
       ?verbose:bool
    -> ?params:string list
    -> Postgresql.connection
    -> string
    -> string =
 fun ?verbose ?params conn query ->
  match exec ?verbose ?params conn query with
  | [[x]] -> x
  | r ->
      Error.create "Unexpected query results." r [%sexp_of: string list list]
      |> Error.raise

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

type field_t = {fname: string; dtype: dtype}

and relation_t = {rname: string; fields: field_t list}
[@@deriving compare, hash, sexp, bin_io]

module Relation = struct
  module T = struct
    type t = relation_t [@@deriving compare, hash, sexp, bin_io]

    let sexp_of_t {rname; _} = [%sexp_of: string] rname
  end

  include T
  include Comparable.Make (T)

  let dummy = {rname= ""; fields= []}

  let from_db : Postgresql.connection -> string -> t =
   fun conn name ->
    let rel = {rname= name; fields= []} in
    let fields =
      exec2 ~params:[name] conn
        "select column_name, data_type from information_schema.columns where \
         table_name='$0'"
      |> List.map ~f:(fun (field_name, dtype_s) ->
             let dtype =
               match dtype_s with
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
             {fname= field_name; dtype} )
    in
    (* TODO: Remove this. *)
    {rel with fields}

  let sample : ?seed:int -> Postgresql.connection -> int -> t -> unit =
   fun ?(seed = 0) conn size r ->
    exec conn ~params:[Int.to_string seed] "set seed to $0" |> ignore ;
    let query =
      {|
        create temp table if not exists $0 as (select * from $0 order by random() limit $1)
      |}
    in
    exec conn ~params:[r.rname; Int.to_string size] query |> ignore

  let field_exn : t -> string -> field_t =
   fun r n ->
    match List.find r.fields ~f:(fun f -> String.(f.fname = n)) with
    | Some f -> f
    | None ->
        Error.create "Field not found." (n, r.rname) [%sexp_of: string * string]
        |> Error.raise

  (* For testing only! *)
  let of_name : string -> t = fun n -> {dummy with rname= n}
end

module Field = struct
  module T = struct
    type t = field_t = {fname: string; dtype: dtype [@compare.ignore]}
    [@@deriving compare, hash, sexp, bin_io]
  end

  include T
  include Comparable.Make (T)

  let dummy = {fname= ""; dtype= DBool}

  let to_string : t -> string = fun f -> f.fname

  (* For testing only! *)
  let of_name : string -> t = fun n -> {dummy with fname= n}
end

let result_to_tuples r =
  Seq.range 0 r#ntuples ~stop:`exclusive
  |> Seq.unfold_with ~init:() ~f:(fun () tup_i ->
         let tup =
           List.init r#nfields ~f:(fun field_i ->
               let value = r#getvalue tup_i field_i in
               let type_ = r#ftype field_i in
               let primval =
                 match type_ with
                 | Postgresql.BOOL -> (
                   match value with
                   | "t" -> Value.Bool true
                   | "f" -> Bool false
                   | _ -> failwith "Unknown boolean value." )
                 | INT8 | INT2 | INT4 ->
                     if String.(value = "") then Null else Int (Int.of_string value)
                 | CHAR | TEXT | VARCHAR -> String value
                 | FLOAT4 | FLOAT8 | NUMERIC -> Fixed (Fixed_point.of_string value)
                 (* Time & date types *)
                 | DATE | TIME | TIMESTAMP | TIMESTAMPTZ | INTERVAL | TIMETZ
                  |ABSTIME | RELTIME
                  |TINTERVAL
                 (* Geometric types. *)
                  |POINT | LSEG | PATH | BOX | POLYGON | LINE
                  |CIRCLE
                 (* Network types *)
                  |MACADDR | INET
                  |CIDR
                 (* Other types*)
                  |NAME | BYTEA | INT2VECTOR | JSON | CASH | ACLITEM | BPCHAR
                  |BIT | VARBIT | JSONB ->
                     (* Store unknown values as strings. *)
                     String value
                 | OID | OIDVECTOR | TID | XID | CID | REFCURSOR | REGPROC
                  |REGPROCEDURE | REGOPER | REGOPERATOR | REGCLASS | REGTYPE ->
                     failwith "Postgres internal type."
                 | ANY | ANYARRAY | VOID | CSTRING | INTERNAL | LANGUAGE_HANDLER
                  |RECORD | TRIGGER | OPAQUE | ANYELEMENT | UNKNOWN ->
                     failwith "Pseudo type."
               in
               (r#fname field_i, primval) )
           |> Map.of_alist_exn (module String)
         in
         Yield (tup, ()) )

let exec_and_raise (conn : Postgresql.connection) query =
  conn#exec query |> process_errors query

let exec_cursor =
  let fresh = Fresh.create () in
  fun ?(batch_size = 1000) ?(params = []) conn query ->
    Logs.debug (fun m -> m "Running %s." query) ;
    let query = subst_params params query in
    let cur = Fresh.name fresh "cur%d" in
    let declare_query =
      sprintf "declare %s scroll cursor with hold for %s;" cur query
    in
    let fetch_query = sprintf "fetch %d from %s;" batch_size cur in
    (* let close_query = sprintf "close %s;" cur in *)
    exec_and_raise conn declare_query |> ignore ;
    let db_idx = ref 1 in
    let seq =
      Seq.unfold_step ~init:(`Not_done 1) ~f:(function
        | `Done -> Done
        | `Not_done idx when idx <> !db_idx ->
            Stdio.printf "moving cursor from %d to %d\n" !db_idx idx ;
            let move_query = sprintf "move absolute %d %s;" idx cur in
            exec_and_raise conn move_query |> ignore ;
            db_idx := idx ;
            Skip (`Not_done idx)
        | `Not_done idx ->
            let r = exec_and_raise conn fetch_query in
            let tups = result_to_tuples r in
            db_idx := !db_idx + r#ntuples ;
            let idx = idx + r#ntuples in
            let state = if r#ntuples < batch_size then `Done else `Not_done idx in
            Yield (tups, state) )
      |> Seq.concat
    in
    (* Caml.Gc.finalise (fun _ ->
     *     printf "Deallocating cursor!\n";
     *     conn#exec close_query |> process_errors |> ignore)
     *   seq; *)
    seq
