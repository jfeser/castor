open Base
open Printf
module Pervasives = Caml.Pervasives
open Collections
module Psql = Postgresql

let () =
  Caml.Printexc.register_printer (function
    | Postgresql.Error e -> Some (Postgresql.string_of_error e)
    | _ -> None )

type t = {db: string; port: string option; conn: Psql.connection}

let create ?port db = {db; port; conn= new Psql.connection ~dbname:db ?port ()}

let subst_params params query =
  match params with
  | [] -> query
  | _ ->
      List.foldi params ~init:query ~f:(fun i q v ->
          String.substr_replace_all ~pattern:(Printf.sprintf "$%d" i) ~with_:v q )

let rec exec ?(max_retries = 0) ?(params = []) db query =
  let query = subst_params params query in
  Logs.debug (fun m -> m "Executing query (retries=%d): %s" max_retries query) ;
  let r = (db.conn)#exec query in
  let fail r =
    Error.(
      create "Postgres error." (r#error, query) [%sexp_of: string * string] |> raise)
  in
  match r#status with
  | Nonfatal_error -> (
    match r#error_code with
    | SERIALIZATION_FAILURE | DEADLOCK_DETECTED ->
        if
          (* See:
             https://www.postgresql.org/message-id/1368066680.60649.YahooMailNeo%40web162902.mail.bf1.yahoo.com
          *)
          max_retries > 0
        then exec ~max_retries:(max_retries - 1) db query
        else fail r
    | _ -> fail r )
  | Single_tuple | Tuples_ok | Command_ok -> r
  | _ -> fail r

let result_to_tuples (r : Psql.result) =
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
                 (* Blank padded character strings *)
                 | BPCHAR -> String (String.strip value)
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
                  |NAME | BYTEA | INT2VECTOR | JSON | CASH | ACLITEM | BIT
                  |VARBIT | JSONB ->
                     (* Store unknown values as strings. *)
                     Logs.warn (fun m -> m "Unknown value: %s" value) ;
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

let result_to_strings (r : Psql.result) = r#get_all_lst

let command_ok (r : Psql.result) =
  match r#status with
  | Psql.Command_ok -> ()
  | _ ->
      Error.(
        create "Unexpected query response." r#error [%sexp_of: string] |> raise)

let exec1 ?params conn query =
  exec ?params conn query |> result_to_strings
  |> List.map ~f:(function
       | [x] -> x
       | t ->
           Error.create "Unexpected query results." t [%sexp_of: string list]
           |> Error.raise )

let exec3 ?params conn query =
  exec ?params conn query |> result_to_strings
  |> List.map ~f:(function
       | [x; y; z] -> (x, y, z)
       | t ->
           Error.create "Unexpected query results." t [%sexp_of: string list]
           |> Error.raise )

type field_t = {fname: string; type_: Type.PrimType.t}

and relation_t = {rname: string; fields: field_t list}
[@@deriving compare, hash, sexp]

module Relation = struct
  type db = t

  type t = relation_t = {rname: string; fields: field_t list}
  [@@deriving compare, hash, sexp]

  let sexp_of_t {rname; _} = [%sexp_of: string] rname

  let from_db conn rname =
    let rel = {rname; fields= []} in
    let fields =
      exec3 ~params:[rname] conn
        "select column_name, data_type, is_nullable from \
         information_schema.columns where table_name='$0'"
      |> List.map ~f:(fun (fname, type_str, nullable_str) ->
             let open Type.PrimType in
             let nullable =
               if String.(nullable_str = "YES") then
                 let nulls =
                   exec1 ~params:[fname; rname] conn
                     "select \"$0\" from \"$1\" where \"$0\" = null limit 1"
                 in
                 List.length nulls > 0
               else false
             in
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
             {fname; type_} )
    in
    {rel with fields}

  let all_from_db conn =
    let names =
      exec1 conn
        "select tablename from pg_catalog.pg_tables where schemaname='public'"
    in
    List.map names ~f:(from_db conn)
end

module Field = struct
  type t = field_t = {fname: string; type_: Type.PrimType.t [@compare.ignore]}
  [@@deriving compare, hash, sexp]
end

let exec_cursor =
  let fresh = Fresh.create () in
  fun ?(batch_size = 100) ?(params = []) db query ->
    Logs.debug (fun m -> m "Running %s." query) ;
    let db = create ?port:db.port db.db in
    let query = subst_params params query in
    let cur = Fresh.name fresh "cur%d" in
    let declare_query =
      sprintf "begin transaction; declare %s cursor for %s;" cur query
    in
    let fetch_query = sprintf "fetch %d from %s;" batch_size cur in
    exec db declare_query |> command_ok ;
    let db_idx = ref 1 in
    let seq =
      Seq.unfold_step ~init:(`Not_done 1) ~f:(function
        | `Done -> (db.conn)#finish ; Done
        | `Not_done idx when idx <> !db_idx ->
            Error.(
              create "Out of sync with underlying cursor." (idx, !db_idx)
                [%sexp_of: int * int]
              |> raise)
        | `Not_done idx ->
            let r = exec db fetch_query in
            let tups = result_to_tuples r in
            db_idx := !db_idx + r#ntuples ;
            let idx = idx + r#ntuples in
            let state = if r#ntuples < batch_size then `Done else `Not_done idx in
            Yield (tups, state) )
      |> Seq.concat
    in
    seq
