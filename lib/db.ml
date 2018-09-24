open Base
open Printf
module Pervasives = Caml.Pervasives
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

let exec3 ?verbose ?params conn query =
  exec ?verbose ?params conn query
  |> List.map ~f:(function
       | [x; y; z] -> (x, y, z)
       | _ -> failwith "Unexpected query results." )

type field_t = {fname: string; type_: Type.PrimType.t}

and relation_t = {rname: string; fields: field_t list}
[@@deriving compare, hash, sexp]

module Relation = struct
  type t = relation_t = {rname: string; fields: field_t list}
  [@@deriving compare, hash, sexp]

  let sexp_of_t {rname; _} = [%sexp_of: string] rname

  let from_db conn name =
    let rel = {rname= name; fields= []} in
    let fields =
      exec3 ~params:[name] conn
        "select column_name, data_type, is_nullable from \
         information_schema.columns where table_name='$0'"
      |> List.map ~f:(fun (name, type_str, nullable_str) ->
             let open Type.PrimType in
             let nullable = String.(nullable_str = "YES") in
             let type_ =
               match type_str with
               | "character" | "character varying" | "varchar" | "text" ->
                   StringT {nullable}
               | "date" | "interval" | "integer" | "smallint" | "bigint" ->
                   IntT {nullable}
               | "boolean" -> BoolT {nullable}
               | "numeric" | "real" | "double" -> FixedT {nullable}
               | "timestamp without time zone" -> StringT {nullable}
               | s -> failwith (Printf.sprintf "Unknown dtype %s" s)
             in
             {fname= name; type_} )
    in
    (* TODO: Remove this. *)
    {rel with fields}
end

module Field = struct
  type t = field_t = {fname: string; type_: Type.PrimType.t [@compare.ignore]}
  [@@deriving compare, hash, sexp]
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
                 | DATE -> Int (Date.of_string value |> Date.to_int)
                 (* Time & date types *)
                 | TIME | TIMESTAMP | TIMESTAMPTZ | INTERVAL | TIMETZ | ABSTIME
                  |RELTIME
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
