open! Core
open! Lwt
open Collections
module Psql = Postgresql

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

let conn { conn; _ } = conn

let param =
  let open Command.Let_syntax in
  [%map_open
    let uri =
      flag "db" (required string) ~doc:"CONNINFO the database to connect to"
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
            let min, max, max_scale =
              exec3 ~params:[ fname; rname ] conn
                "select min(\"$0\"), max(\"$0\"), max(scale(\"$0\")) from \
                 \"$1\""
              |> List.hd_exn
            in
            let is_int = Int.of_string max_scale = 0 in
            let fits_in_an_int63 =
              try
                Int.of_string min |> ignore;
                Int.of_string max |> ignore;
                true
              with Failure _ -> false
            in
            if is_int then
              if fits_in_an_int63 then IntT { nullable }
              else StringT { nullable; padded = false }
            else FixedT { nullable }
        | "real" | "double" -> FixedT { nullable }
        | "timestamp without time zone" | "time without time zone" ->
            StringT { nullable; padded = false }
        | s -> failwith (Printf.sprintf "Unknown dtype %s" s) )
    | _ -> failwith "Unexpected db results."
  in
  let memo = Memo.general (fun (conn, n1, n2) -> f conn n1 n2) in
  fun conn fname rname -> memo (conn, fname, rname)

let relation with_types conn r_name =
  let r_schema =
    exec1 ~params:[ r_name ] conn
      "select column_name from information_schema.columns where table_name='$0'"
    |> List.map ~f:(fun fname ->
           let type_ =
             if with_types then Some (type_of_field_exn conn fname r_name)
             else None
           in
           Name.create ?type_ fname)
    |> Option.some
  in
  Relation.{ r_name; r_schema }

let relation_memo =
  Memo.general (fun (conn, rname, with_types) -> relation with_types conn rname)

let relation ?(with_types = true) conn rname =
  relation_memo (conn, rname, with_types)

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
      List.exists (Option.value_exn r.Relation.r_schema) ~f:(fun n ->
          String.(Name.name n = f)))

let load_value type_ value =
  let open Prim_type in
  match type_ with
  | BoolT { nullable } -> (
      match value with
      | "t" -> Ok (Value.Bool true)
      | "f" -> Ok (Bool false)
      | "" when nullable -> Ok Null
      | _ ->
          Error (Error.create "Unknown boolean value." value [%sexp_of: string])
      )
  | IntT { nullable } ->
      if String.(value = "") then
        if nullable then Ok Null
        else Error (Error.of_string "Unexpected null integer.")
      else Ok (Int (Int.of_string value))
  | StringT { padded = true; _ } ->
      Ok (String (String.rstrip ~drop:(fun c -> Char.(c = ' ')) value))
  | StringT { padded = false; _ } -> Ok (String value)
  | FixedT { nullable } ->
      if String.(value = "") then
        if nullable then Ok Null
        else Error (Error.of_string "Unexpected null fixed.")
      else Ok (Fixed (Fixed_point.of_string value))
  | DateT { nullable } ->
      if String.(value = "") then
        if nullable then Ok Null
        else Error (Error.of_string "Unexpected null integer.")
      else Ok (Date (Date.of_string value))
  | NullT ->
      if String.(value = "") then Ok Null
      else
        Error (Error.create "Expected a null value." value [%sexp_of: string])
  | VoidT | TupleT _ -> Error (Error.of_string "Not a value type.")

let load_tuples_exn s (r : Postgresql.result) =
  let nfields = List.length s in
  if nfields <> r#nfields then
    Error.(
      create "Unexpected tuple width." (r#get_fnames_lst, s)
        [%sexp_of: string list * Prim_type.t list]
      |> raise)
  else
    let gen =
      Gen.init ~limit:r#ntuples (fun tidx ->
          Array.of_list_mapi s ~f:(fun fidx type_ ->
              if r#getisnull tidx fidx then Value.Null
              else load_value type_ (r#getvalue tidx fidx) |> Or_error.ok_exn))
    in
    gen

let load_tuples_list_exn s (r : Postgresql.result) =
  let nfields = List.length s in
  if nfields <> r#nfields then
    Error.(
      create "Unexpected tuple width." (r#get_fnames_lst, s)
        [%sexp_of: string list * Prim_type.t list]
      |> raise)
  else
    List.init r#ntuples ~f:(fun tidx ->
        Array.of_list_mapi s ~f:(fun fidx type_ ->
            if r#getisnull tidx fidx then Value.Null
            else load_value type_ (r#getvalue tidx fidx) |> Or_error.ok_exn))

let exec_cursor_exn =
  let fresh = Fresh.create () in
  fun ?(batch_size = 100) ?(params = []) db schema query ->
    let db = create db.uri in
    Caml.Gc.finalise (fun db -> try db.conn#finish with Failure _ -> ()) db;
    let query = subst_params params query in
    let cur = Fresh.name fresh "cur%d" in
    let declare_query =
      sprintf "begin transaction; declare %s cursor for %s;" cur query
    in
    let fetch_query = sprintf "fetch %d from %s;" batch_size cur in
    exec db declare_query |> command_ok_exn;
    let db_idx = ref 1 in
    let seq =
      Gen.unfold
        (function
          | `Done ->
              db.conn#finish;
              None
          | `Not_done idx when idx <> !db_idx ->
              Some
                ( Error.(
                    create "Out of sync with underlying cursor." (idx, !db_idx)
                      [%sexp_of: int * int]
                    |> raise),
                  `Done )
          | `Not_done idx ->
              let r = exec db fetch_query in
              let tups = load_tuples_exn schema r in
              db_idx := !db_idx + r#ntuples;
              let idx = idx + r#ntuples in
              let state =
                if r#ntuples < batch_size then `Done else `Not_done idx
              in
              Some (tups, state))
        (`Not_done 1)
      |> Gen.flatten
    in
    seq

let exec_lwt_exn ?(params = []) ?timeout db schema query =
  let query = subst_params params query in
  let stream, push = Lwt_stream.create () in

  (* Create a new database connection. *)
  let exec () =
    Lwt_pool.use db.pool (fun conn ->
        let fail msg =
          push (Some (Result.fail msg));
          push None;
          conn#reset;
          return_unit
        in

        (* OCaml can't convert integers to fds for reasons, so magic is required. *)
        let fd = conn#socket |> Obj.magic |> Lwt_unix.of_unix_file_descr in
        (* Wait for results from a send_query call. consume_input pulls any
         available input from the database *)
        let rec wait_for_results () =
          conn#consume_input;
          if conn#is_busy then
            let%lwt () = Lwt_unix.wait_read fd in
            wait_for_results ()
          else
            match conn#get_result with
            | Some r -> (
                match r#status with
                | Tuples_ok ->
                    load_tuples_list_exn schema r
                    |> List.iter ~f:(fun t -> push (Some (Result.return t)));
                    wait_for_results ()
                | Fatal_error -> fail (`Exn (Failure r#error))
                | _ -> fail (`Exn (Failure "Unexpected status")) )
            | None ->
                push None;
                return_unit
        in
        let execute_query () =
          conn#send_query query;
          wait_for_results ()
        in
        let execute_with_timeout t =
          try%lwt Lwt_unix.with_timeout t execute_query
          with Lwt_unix.Timeout ->
            Log.debug (fun m -> m "Query timeout: %s" query);
            exec db (sprintf "select pg_cancel_backend(%d);" conn#backend_pid)
            |> ignore;
            fail `Timeout
        in
        try%lwt
          match timeout with
          | Some t -> execute_with_timeout t
          | None -> execute_query ()
        with exn -> fail (`Exn exn))
  in
  async exec;
  stream

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
