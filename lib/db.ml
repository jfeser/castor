open! Lwt
open Collections
module Psql = Postgresql

include (val Log.make ~level:(Some Info) "castor.db")

let default_pool_size = 3

let () =
  Caml.Printexc.register_printer (function
    | Postgresql.Error e -> Some (Postgresql.string_of_error e)
    | _ -> None)

type t = {
  uri : string;
  conn : (Psql.connection[@sexp.opaque]); [@compare.ignore]
  pool : (Psql.connection Lwt_pool.t[@sexp.opaque]); [@compare.ignore]
}
[@@deriving compare, sexp]

let connect uri = new Psql.connection ~conninfo:uri ()

let valid c =
  match c#status with
  | Psql.Ok -> true
  | Bad -> false
  | _ -> failwith "Unexpected connection status."

[@@@warning "-52"]

let ensure_finish c =
  try c#finish
  with Failure "Postgresql.check_null: connection already finished" -> ()

[@@@warning "+52"]

let create ?(pool_size = default_pool_size) uri =
  {
    uri;
    conn = connect uri;
    pool =
      Lwt_pool.create pool_size
        ~dispose:(fun c ->
          ensure_finish c;
          return_unit)
        ~validate:(fun c -> valid c |> return)
        ~check:(fun c is_ok -> is_ok (valid c))
        (fun () -> connect uri |> return);
  }

let close { conn; _ } = ensure_finish conn

let conn { conn; _ } = conn

let param =
  let open Command.Let_syntax in
  [%map_open
    let m_uri =
      flag "db" (optional string) ~doc:"CONNINFO the database to connect to"
    in
    let uri =
      match m_uri with Some uri -> uri | None -> Sys.getenv_exn "CASTOR_DB"
    in
    create uri]

let subst_params params query =
  match params with
  | [] -> query
  | _ ->
      List.foldi params ~init:query ~f:(fun i q v ->
          String.substr_replace_all ~pattern:(Printf.sprintf "$%d" i) ~with_:v q)

let rec exec ?(max_retries = 0) ?(params = []) db query =
  let query = subst_params params query in
  let r = db.conn#exec query in
  let fail r =
    Error.(
      create "Postgres error." (r#error, query) [%sexp_of: string * string]
      |> raise)
  in
  match r#status with
  | Nonfatal_error -> (
      warn (fun m -> m "Received nonfatal error. Retrying.");
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

let result_to_strings (r : Psql.result) = r#get_all_lst

let command_ok (r : Psql.result) =
  match r#status with
  | Command_ok -> Or_error.return ()
  | _ -> Or_error.error "Unexpected query response." r#error [%sexp_of: string]

let command_ok_exn (r : Psql.result) = command_ok r |> Or_error.ok_exn

let exec1 ?params conn query =
  exec ?params conn query |> result_to_strings
  |> List.map ~f:(function
       | [ x ] -> x
       | t ->
           Error.create "Unexpected query results." t [%sexp_of: string list]
           |> Error.raise)

let exec2 ?params conn query =
  exec ?params conn query |> result_to_strings
  |> List.map ~f:(function
       | [ x; y ] -> (x, y)
       | t ->
           Error.create "Unexpected query results." t [%sexp_of: string list]
           |> Error.raise)

let exec3 ?params conn query =
  exec ?params conn query |> result_to_strings
  |> List.map ~f:(function
       | [ x; y; z ] -> (x, y, z)
       | t ->
           Error.create "Unexpected query results." t [%sexp_of: string list]
           |> Error.raise)

let type_of_field_exn =
  let f conn fname rname =
    let open Prim_type in
    let rows =
      exec2 ~params:[ rname; fname ] conn
        "select data_type, is_nullable from information_schema.columns where \
         table_name='$0' and column_name='$1'"
    in
    match rows with
    | [ (type_str, nullable_str) ] -> (
        let nullable =
          if String.(strip nullable_str = "YES") then
            let nulls =
              exec1 ~params:[ fname; rname ] conn
                "select \"$0\" from \"$1\" where \"$0\" is null limit 1"
            in
            List.length nulls > 0
          else false
        in
        match type_str with
        | "character" | "char" -> StringT { nullable; padded = true }
        | "character varying" | "varchar" | "text" ->
            StringT { nullable; padded = false }
        | "interval" | "integer" | "smallint" | "bigint" -> IntT { nullable }
        | "date" -> DateT { nullable }
        | "boolean" -> BoolT { nullable }
        | "numeric" ->
            let min, max, is_int =
              exec3 ~params:[ fname; rname ] conn
                {|
select
  min(round("$0")), max(round("$0")),
  min(case when round("$0") = "$0" then 1 else 0 end)
from "$1"
|}
              |> List.hd_exn
            in
            let is_int = Int.of_string is_int = 1 in
            let fits_in_an_int63 =
              try
                Int.of_string min |> ignore;
                Int.of_string max |> ignore;
                true
              with Failure _ -> false
            in
            if is_int then
              if fits_in_an_int63 then IntT { nullable }
              else (
                warn (fun m ->
                    m "Numeric column loaded as string: %s.%s" rname fname);
                StringT { nullable; padded = false } )
            else FixedT { nullable }
        | "real" | "double" -> FixedT { nullable }
        | "timestamp without time zone" | "time without time zone" ->
            StringT { nullable; padded = false }
        | s -> failwith (Printf.sprintf "Unknown dtype %s" s) )
    | _ -> failwith "Unexpected db results."
  in
  let memo = Memo.general (fun (conn, n1, n2) -> f conn n1 n2) in
  fun conn fname rname -> memo (conn, fname, rname)

let relation conn r_name =
  let r_schema =
    exec1 ~params:[ r_name ] conn
      "select column_name from information_schema.columns where table_name='$0'"
    |> List.map ~f:(fun fname ->
           let type_ = type_of_field_exn conn fname r_name in
           (Name.create fname, type_))
    |> Option.some
  in
  Relation.{ r_name; r_schema }

let relation_memo = Memo.general (fun (conn, rname) -> relation conn rname)

let relation conn rname = relation_memo (conn, rname)

let relation_count =
  let f conn r_name =
    exec1 ~params:[ r_name ] conn "select count(*) from $0"
    |> List.hd_exn |> Int.of_string
  in
  let memo = Memo.general (fun (c, n) -> f c n) in
  fun c n -> memo (c, n)

let all_relations conn =
  let names =
    exec1 conn
      "select tablename from pg_catalog.pg_tables where schemaname='public'"
  in
  List.map names ~f:(relation conn)

let relation_has_field conn f =
  List.find (all_relations conn) ~f:(fun r ->
      List.exists (Option.value_exn r.Relation.r_schema) ~f:(fun (n, _) ->
          String.(Name.name n = f)))

let is_null = String.is_empty

let load_int s =
  try Ok (Int.of_string s)
  with Failure _ -> (
    try
      (* Try loading 'ints' of the form x.00 *)
      Ok (Int.of_string (String.drop_suffix s 3))
    with Failure e -> Or_error.error_string e )

let load_padded_string v = String.rstrip ~drop:(fun c -> Char.(c = ' ')) v

let load_value type_ =
  let open Prim_type in
  match type_ with
  | BoolT _ -> (
      fun value ->
        match value with
        | "t" -> Ok (Value.Bool true)
        | "f" -> Ok (Bool false)
        | _ ->
            Error
              (Error.create "Unknown boolean value." value [%sexp_of: string]) )
  | IntT _ ->
      fun value -> Or_error.map (load_int value) ~f:(fun x -> Value.Int x)
  | StringT { padded = true; _ } ->
      fun value -> Ok (String (load_padded_string value))
  | StringT { padded = false; _ } -> fun value -> Ok (String value)
  | FixedT _ -> fun value -> Ok (Fixed (Fixed_point.of_string value))
  | DateT _ -> fun value -> Ok (Date (Date.of_string value))
  | NullT -> fun _ -> Ok Null
  | VoidT | TupleT _ -> fun _ -> Error (Error.of_string "Not a value type.")

let load_tuples_list_exn s =
  let loaders = List.map s ~f:load_value |> Array.of_list in
  let nfields = List.length s in

  fun (r : Postgresql.result) ->
    if nfields <> r#nfields then
      Error.(
        create "Unexpected tuple width." (r#get_fnames_lst, s)
          [%sexp_of: string list * Prim_type.t list]
        |> raise)
    else
      List.init r#ntuples ~f:(fun tidx ->
          List.init nfields ~f:(fun fidx ->
              if r#getisnull tidx fidx then Value.Null
              else loaders.(fidx) (r#getvalue tidx fidx) |> Or_error.ok_exn))

let exec_exn db schema query = exec db query |> load_tuples_list_exn schema

let exec_cursor_exn ?(count = 4096) db schema query =
  let declare_query = sprintf "declare cur cursor for %s" query
  and fetch_query = sprintf "fetch %d from cur" count
  and open_trans = "begin"
  and close_trans = "commit" in

  let db = create db.uri in
  exec db open_trans |> command_ok_exn;
  exec db declare_query |> command_ok_exn;
  let loader = load_tuples_list_exn schema in
  Seq.unfold ~init:() ~f:(fun () ->
      let tups = exec db fetch_query |> loader in
      if List.length tups > 0 then Some (tups, ())
      else (
        exec db close_trans |> command_ok_exn;
        close db;
        None ))

let check db sql =
  let open Or_error.Let_syntax in
  let name = Fresh.name Global.fresh "check%d" in
  let%bind () = command_ok (db.conn#prepare name sql) in
  command_ok (db.conn#exec (sprintf "deallocate %s" name))

let eq_join_type =
  let f db n1 n2 =
    match (relation_has_field db n1, relation_has_field db n2) with
    | Some r1, Some r2 ->
        let c1 = relation_count db r1.r_name in
        let c2 = relation_count db r2.r_name in
        let c3 =
          exec1 db
          @@ sprintf "select count(*) from %s, %s where %s = %s" r1.r_name
               r2.r_name n1 n2
          |> List.hd_exn |> Int.of_string
        in
        if c1 = c3 && c2 = c3 then Ok `Both
        else if c1 = c3 then Ok `Left
        else if c2 = c3 then Ok `Right
        else Ok `Neither
    | _ -> Error (Error.of_string "Not a join between base fields.")
  in
  let memo = Memo.general (fun (c, n1, n2) -> f c n1 n2) in
  fun c n1 n2 -> memo (c, n1, n2)

module Async = struct
  type db = t

  open Lwt

  type error = {
    query : string;
    info : [ `Timeout | `Exn of Exn.t | `Msg of string ];
  }
  [@@deriving sexp_of]

  type 'a exec =
    ?timeout:float ->
    ?bound:int ->
    db ->
    'a ->
    (Value.t list list, error) Result.t Lwt_stream.t

  let return_never =
    let p, _ = wait () in
    p

  let return_unit_timeout t =
    let p, r = wait () in
    Lwt_timeout.(create (Int.of_float t) (wakeup_later r) |> start);
    p

  let to_error { query; info = err } =
    let err =
      match err with
      | `Timeout -> Error.createf "Timed out."
      | `Exn e -> Error.of_exn e
      | `Msg m -> Error.of_string m
    in
    Error.tag_arg err "query" query [%sexp_of: string]

  let exec_sql ?timeout ?(bound = 4096) db (schema, query) =
    let stream, strm = Lwt_stream.create_bounded bound in

    let load_tuples = load_tuples_list_exn schema in

    (* Create a new database connection. *)
    let exec () =
      Lwt_pool.use db.pool (fun conn ->
          (* We cancel if the query times out. It's important that this run
             inside the call to Lwt_use so we don't start the timer before we
             get a connection. *)
          let timeout =
            Option.map timeout ~f:return_unit_timeout
            |> Option.value ~default:return_never
          in

          let fail msg =
            let%lwt () = strm#push (Result.fail { query; info = msg }) in
            strm#close;
            conn#reset;
            return_unit
          in

          (* OCaml can't convert integers to fds for reasons, so magic is
             required. *)
          let fd = conn#socket |> Obj.magic |> Lwt_unix.of_unix_file_descr in

          let wait_for_cancel () =
            let%lwt () = protected timeout in
            info (fun m -> m "Query timeout: %s" query);
            exec db (sprintf "select pg_cancel_backend(%d);" conn#backend_pid)
            |> ignore;
            fail `Timeout
          in

          let read_results k =
            match conn#get_result with
            | Some r -> (
                match r#status with
                | Tuples_ok ->
                    info (fun m -> m "Got a %d tuple batch" r#ntuples);
                    let%lwt () = strm#push (Result.return (load_tuples r)) in
                    k ()
                | Fatal_error -> fail (`Msg r#error)
                | _ -> fail (`Msg "Unexpected status") )
            | None ->
                strm#close;
                return_unit
          in

          (* Wait for results from a send_query call. consume_input pulls any
             available input from the database *)
          let rec wait_for_results () =
            conn#consume_input;
            if conn#is_busy then
              pick
                [
                  Lwt_unix.wait_read fd >>= wait_for_results; wait_for_cancel ();
                ]
            else read_results wait_for_results
          in
          try%lwt
            conn#send_query query;
            wait_for_results ()
          with exn -> fail (`Exn exn))
    in
    async exec;
    stream

  let exec ?timeout ?bound db r =
    info (fun m -> m "Running query:@ %a" Abslayout_pp.pp r);
    exec_sql ?timeout ?bound db
      (Schema.types r, Sql.of_ralgebra r |> Sql.to_string)
end
