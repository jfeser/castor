open! Core
open! Lwt
open Collections
module Psql = Postgresql
module A = Abslayout0

let () =
  Caml.Printexc.register_printer (function
    | Postgresql.Error e -> Some (Postgresql.string_of_error e)
    | _ -> None)

type t = {uri: string; conn: Psql.connection sexp_opaque [@compare.ignore]}
[@@deriving compare, sexp]

let num_conns = ref 0

let create uri = {uri; conn= new Psql.connection ~conninfo:uri ()}

let conn {conn; _} = conn

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

let result_to_strings (r : Psql.result) = r#get_all_lst

let command_ok (r : Psql.result) =
  match r#status with
  | Command_ok -> Or_error.return ()
  | _ -> Or_error.error "Unexpected query response." r#error [%sexp_of: string]

let command_ok_exn (r : Psql.result) = command_ok r |> Or_error.ok_exn

let exec1 ?params conn query =
  exec ?params conn query |> result_to_strings
  |> List.map ~f:(function
       | [x] -> x
       | t ->
           Error.create "Unexpected query results." t [%sexp_of: string list]
           |> Error.raise)

let exec3 ?params conn query =
  exec ?params conn query |> result_to_strings
  |> List.map ~f:(function
       | [x; y; z] -> (x, y, z)
       | t ->
           Error.create "Unexpected query results." t [%sexp_of: string list]
           |> Error.raise)

let relation conn r_name =
  let r_schema =
    exec3 ~params:[r_name] conn
      "select column_name, data_type, is_nullable from information_schema.columns \
       where table_name='$0'"
    |> List.map ~f:(fun (fname, type_str, nullable_str) ->
           let open Type.PrimType in
           let nullable =
             if String.(strip nullable_str = "YES") then
               let nulls =
                 exec1 ~params:[fname; r_name] conn
                   "select \"$0\" from \"$1\" where \"$0\" is null limit 1"
               in
               List.length nulls > 0
             else false
           in
           let type_ =
             match type_str with
             | "character" | "char" -> StringT {nullable; padded= true}
             | "character varying" | "varchar" | "text" ->
                 StringT {nullable; padded= false}
             | "interval" | "integer" | "smallint" | "bigint" -> IntT {nullable}
             | "date" -> DateT {nullable}
             | "boolean" -> BoolT {nullable}
             | "numeric" ->
                 let min, max, max_scale =
                   exec3 ~params:[fname; r_name] conn
                     "select min(\"$0\"), max(\"$0\"), max(scale(\"$0\")) from \
                      \"$1\""
                   |> List.hd_exn
                 in
                 let is_int = Int.of_string max_scale = 0 in
                 let fits_in_an_int63 =
                   try
                     Int.of_string min |> ignore ;
                     Int.of_string max |> ignore ;
                     true
                   with Failure _ -> false
                 in
                 if is_int then
                   if fits_in_an_int63 then IntT {nullable}
                   else StringT {nullable; padded= false}
                 else FixedT {nullable}
             | "real" | "double" -> FixedT {nullable}
             | "timestamp without time zone" -> StringT {nullable; padded= false}
             | s -> failwith (Printf.sprintf "Unknown dtype %s" s)
           in
           Name.create ~type_ fname)
    |> Option.some
  in
  A.{r_name; r_schema}

let relation_memo = Memo.general (fun (conn, rname) -> relation conn rname)

let relation conn rname = relation_memo (conn, rname)

let all_relations conn =
  let names =
    exec1 conn
      "select tablename from pg_catalog.pg_tables where schemaname='public'"
  in
  List.map names ~f:(relation conn)

let relation_has_field conn f =
  List.find (all_relations conn) ~f:(fun r ->
      List.exists (Option.value_exn r.A.r_schema) ~f:(fun n -> Name.name n = f))

let load_value type_ value =
  let open Type.PrimType in
  match type_ with
  | BoolT {nullable} -> (
    match value with
    | "t" -> Ok (Value.Bool true)
    | "f" -> Ok (Bool false)
    | "" when nullable -> Ok Null
    | _ -> Error (Error.create "Unknown boolean value." value [%sexp_of: string]) )
  | IntT {nullable} ->
      if String.(value = "") then
        if nullable then Ok Null
        else Error (Error.of_string "Unexpected null integer.")
      else Ok (Int (Int.of_string value))
  | StringT {padded= true; _} ->
      Ok (String (String.rstrip ~drop:(fun c -> c = ' ') value))
  | StringT {padded= false; _} -> Ok (String value)
  | FixedT {nullable} ->
      if String.(value = "") then
        if nullable then Ok Null
        else Error (Error.of_string "Unexpected null fixed.")
      else Ok (Fixed (Fixed_point.of_string value))
  | DateT {nullable} ->
      if String.(value = "") then
        if nullable then Ok Null
        else Error (Error.of_string "Unexpected null integer.")
      else Ok (Date (Date.of_string value))
  | NullT ->
      if String.(value = "") then Ok Null
      else Error (Error.create "Expected a null value." value [%sexp_of: string])
  | VoidT | TupleT _ -> Error (Error.of_string "Not a value type.")

let load_tuples_exn s (r : Postgresql.result) =
  let nfields = List.length s in
  if nfields <> r#nfields then
    Error.(
      create "Unexpected tuple width." (r#get_fnames_lst, s)
        [%sexp_of: string list * Type.PrimType.t list]
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
        [%sexp_of: string list * Type.PrimType.t list]
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
    Caml.Gc.finalise (fun db -> try (db.conn)#finish with Failure _ -> ()) db ;
    let query = subst_params params query in
    let cur = Fresh.name fresh "cur%d" in
    let declare_query =
      sprintf "begin transaction; declare %s cursor for %s;" cur query
    in
    let fetch_query = sprintf "fetch %d from %s;" batch_size cur in
    exec db declare_query |> command_ok_exn ;
    let db_idx = ref 1 in
    let seq =
      Gen.unfold
        (function
          | `Done -> (db.conn)#finish ; None
          | `Not_done idx when idx <> !db_idx ->
              Some
                ( Error.(
                    create "Out of sync with underlying cursor." (idx, !db_idx)
                      [%sexp_of: int * int]
                    |> raise)
                , `Done )
          | `Not_done idx ->
              let r = exec db fetch_query in
              let tups = load_tuples_exn schema r in
              db_idx := !db_idx + r#ntuples ;
              let idx = idx + r#ntuples in
              let state = if r#ntuples < batch_size then `Done else `Not_done idx in
              Some (tups, state))
        (`Not_done 1)
      |> Gen.flatten
    in
    seq

let exec_cursor_lwt_exn ?(batch_size = 100) ?(params = []) ?timeout db schema query
    =
  (* Create a new database connection. *)
  let cdb = create db.uri in
  incr num_conns ;
  (* Prepare the query. *)
  let begin_query, fetch_query, end_query =
    let cur = Fresh.name Global.fresh "cur%d" in
    let query = subst_params params query in
    ( sprintf "begin transaction; declare %s cursor for %s;" cur query
    , sprintf "fetch %d from %s;" batch_size cur
    , "end transaction;" )
  in
  (* Wait for results from a send_query call. consume_input pulls any available
     input from the database *)
  let rec wait_for_results rs =
    (cdb.conn)#consume_input ;
    if (cdb.conn)#is_busy then
      (* TODO: Better not to busy wait here. *)
      let%lwt () = Lwt_unix.sleep 0.01 in
      wait_for_results rs
    else
      match (cdb.conn)#get_result with
      | Some r -> wait_for_results (r :: rs)
      | None -> return rs
  in
  let stream, push = Lwt_stream.create () in
  let rec read_batch () =
    (cdb.conn)#send_query fetch_query ;
    let%lwt rs = wait_for_results [] in
    let tups = List.concat_map rs ~f:(load_tuples_list_exn schema) in
    List.iter tups ~f:(fun t -> push (Some (Result.return t))) ;
    if List.length tups < batch_size then (push None ; return_unit)
    else read_batch ()
  in
  let execute_cursor () =
    exec cdb begin_query |> command_ok_exn ;
    let%lwt () = read_batch () in
    exec cdb end_query |> command_ok_exn ;
    return_unit
  in
  async (fun () ->
      finalize
        (fun () ->
          match timeout with
          | Some t ->
              catch
                (fun () -> Lwt_unix.with_timeout t execute_cursor)
                (fun exn ->
                  push (Some (Result.fail exn)) ;
                  let pid = (cdb.conn)#backend_pid in
                  exec db (sprintf "select pg_cancel_backend(%d);" pid) |> ignore ;
                  return_unit)
          | None -> execute_cursor ())
        (fun () -> decr num_conns ; (cdb.conn)#finish ; return_unit)) ;
  stream

let check db sql =
  let open Or_error.Let_syntax in
  let name = Fresh.name Global.fresh "check%d" in
  let%bind () = command_ok ((db.conn)#prepare name sql) in
  command_ok ((db.conn)#exec (sprintf "deallocate %s" name))
