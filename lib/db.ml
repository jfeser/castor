open Core
open! Lwt
open Collections
module Psql = Postgresql
include (val Log.make ~level:(Some Warning) "castor.db")

let default_pool_size = 10

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

let with_conn ?pool_size uri f =
  Exn.protectx (create ?pool_size uri) ~f ~finally:close

let conn { conn; _ } = conn

let rec psql_exec ?(max_retries = 0) db query =
  let r = db.conn#exec query in
  let fail r =
    Or_error.error "Postgres error." (r#error, query)
      [%sexp_of: string * string]
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
          then psql_exec ~max_retries:(max_retries - 1) db query
          else fail r
      | _ -> fail r)
  | Single_tuple | Tuples_ok | Command_ok -> Ok r
  | _ -> fail r

let result_to_strings (r : Psql.result) = r#get_all_lst

let command_ok (r : Psql.result) =
  match r#status with
  | Command_ok -> Or_error.return ()
  | _ -> Or_error.error "Unexpected query response." r#error [%sexp_of: string]

let command_ok_exn (r : Psql.result) = command_ok r |> Or_error.ok_exn

let run conn query =
  let open Or_error.Let_syntax in
  let%map result = psql_exec conn query in
  result_to_strings result

let run1 conn query =
  let open Or_error.Let_syntax in
  let%map results = run conn query in
  List.map results ~f:(function [ x ] -> x | _ -> assert false)

let run2 conn query =
  let open Or_error.Let_syntax in
  let%map results = run conn query in
  List.map results ~f:(function [ x; x' ] -> (x, x') | _ -> assert false)

let run3 conn query =
  let open Or_error.Let_syntax in
  let%map results = run conn query in
  List.map results ~f:(function
    | [ x; x'; x'' ] -> (x, x', x'')
    | _ -> assert false)

let type_of_field =
  let open Or_error.Let_syntax in
  let f conn fname rname =
    let open Prim_type in
    let%bind rows =
      run2 conn
      @@ sprintf
           "select data_type, is_nullable from information_schema.columns \
            where table_name='%s' and column_name='%s'"
           rname fname
    in
    match rows with
    | [ (type_str, nullable_str) ] -> (
        let%bind nullable =
          if String.(strip nullable_str = "YES") then
            let%map nulls =
              run1 conn
              @@ sprintf
                   "select \"%s\" from \"%s\" where \"%s\" is null limit 1"
                   fname rname fname
            in
            List.length nulls > 0
          else Ok false
        in
        match type_str with
        | "character" | "char" -> return @@ StringT { nullable; padded = true }
        | "character varying" | "varchar" | "text" ->
            return @@ StringT { nullable; padded = false }
        | "interval" | "integer" | "smallint" | "bigint" ->
            return @@ IntT { nullable }
        | "date" -> return @@ DateT { nullable }
        | "boolean" -> return @@ BoolT { nullable }
        | "numeric" ->
            let%bind min, max, is_int =
              let%bind result =
                run3 conn
                @@ sprintf
                     {|
select
  min(round("%s")), max(round("%s")),
  min(case when round("%s") = "%s" then 1 else 0 end)
from "%s"
|}
                     fname fname fname fname rname
              in
              match List.hd result with
              | Some t -> return t
              | None -> Or_error.error_string "unexpected query result"
            in
            let is_int = Int.of_string is_int = 1 in
            let fits_in_an_int63 =
              try
                Int.of_string min |> ignore;
                Int.of_string max |> ignore;
                true
              with Failure _ -> false
            in
            return
            @@
            if is_int then
              if fits_in_an_int63 then IntT { nullable }
              else (
                warn (fun m ->
                    m "Numeric column loaded as string: %s.%s" rname fname);
                StringT { nullable; padded = false })
            else FixedT { nullable }
        | "real" | "double" -> return @@ FixedT { nullable }
        | "timestamp without time zone" | "time without time zone" ->
            return @@ StringT { nullable; padded = false }
        | s -> Or_error.error "Unknown dtype" s [%sexp_of: string])
    | _ -> Or_error.error_string "unexpected query result"
  in
  let memo = Memo.general (fun (conn, n1, n2) -> f conn n1 n2) in
  fun conn fname rname -> memo (conn, fname, rname)

let relation conn r_name =
  (* Ensure that table exists in the db. *)
  let table_exists =
    run1 conn
    @@ sprintf
         "select table_name from information_schema.tables where \
          table_name='%s'"
         r_name
    |> Or_error.ok_exn |> List.is_empty |> not
  in
  if not table_exists then
    Error.create "Table does not exist." r_name [%sexp_of: string]
    |> Error.raise;

  let r_schema =
    run1 conn
    @@ sprintf
         "select column_name from information_schema.columns where \
          table_name='%s'"
         r_name
    |> Or_error.ok_exn
    |> List.map ~f:(fun fname ->
           let type_ = type_of_field conn fname r_name |> Or_error.ok_exn in
           (Name.create fname, type_))
    |> List.sort ~compare:[%compare: Name.t * Prim_type.t]
    |> Option.some
  in
  Relation.{ r_name; r_schema }

let relation_memo = Memo.general (fun (conn, rname) -> relation conn rname)
let relation conn rname = relation_memo (conn, rname)

let load_int s =
  try Ok (Int.of_string s)
  with Failure _ -> (
    (* Try loading 'ints' of the form x.00 *)
    match String.chop_suffix s ~suffix:".00" with
    | Some s' -> (
        try Ok (Int.of_string s') with Failure e -> Or_error.error_string e)
    | None -> Or_error.error_string "Not an integer.")

let%test "" = Poly.(load_int "5" = Ok 5)
let%test "" = Poly.(load_int "5.00" = Ok 5)
let%test "" = Result.is_error @@ load_int "5.01"

let load_padded_string v = String.rstrip ~drop:(fun c -> Char.(c = ' ')) v

let load_value type_ =
  let open Prim_type in
  match type_ with
  | BoolT _ -> (
      fun value ->
        match value with
        | "t" -> Ok (Value.Bool true)
        | "f" -> Ok (Bool false)
        | _ -> Or_error.error "Unknown boolean value." value [%sexp_of: string])
  | IntT _ ->
      fun value -> Or_error.map (load_int value) ~f:(fun x -> Value.Int x)
  | StringT { padded = true; _ } ->
      fun value -> Ok (String (load_padded_string value))
  | StringT { padded = false; _ } -> fun value -> Ok (String value)
  | FixedT _ -> fun value -> Ok (Fixed (Fixed_point.of_string value))
  | DateT _ -> fun value -> Ok (Date (Date.of_string value))
  | NullT -> fun _ -> Ok Null
  | VoidT | TupleT _ -> fun _ -> Or_error.error_string "Not a value type."

let load_tuples_list s =
  let open Or_error.Let_syntax in
  let loaders = List.map s ~f:load_value |> Array.of_list in
  let nfields = List.length s in

  fun (r : Postgresql.result) ->
    if nfields <> r#nfields then
      Or_error.error "Unexpected tuple width." (r#get_fnames_lst, s)
        [%sexp_of: string list * Prim_type.t list]
    else
      List.init r#ntuples ~f:(fun tidx ->
          List.init nfields ~f:(fun fidx ->
              if r#getisnull tidx fidx then return Value.Null
              else loaders.(fidx) (r#getvalue tidx fidx))
          |> Or_error.all)
      |> Or_error.all

let exec db schema query =
  let open Or_error.Let_syntax in
  let%bind result = psql_exec db query in
  load_tuples_list schema result

let exec_exn db schema query = exec db schema query |> Or_error.ok_exn

let exec1 conn s query =
  let open Or_error.Let_syntax in
  let%map results = exec conn [ s ] query in
  List.map results ~f:(function [ x ] -> x | _ -> assert false)

let all_relations conn =
  let names =
    run1 conn
      "select tablename from pg_catalog.pg_tables where schemaname='public'"
    |> Or_error.ok_exn
  in
  List.map names ~f:(relation conn)

let relation_has_field conn f =
  let rels = all_relations conn in
  List.find rels ~f:(fun r ->
      List.exists (Option.value_exn r.Relation.r_schema) ~f:(fun (n, _) ->
          String.(Name.name n = f)))

let exec_cursor_exn ?(count = 4096) db schema query =
  let declare_query = sprintf "declare cur cursor for %s" query
  and fetch_query = sprintf "fetch %d from cur" count
  and open_trans = "begin"
  and close_trans = "commit" in

  let db = create db.uri in
  psql_exec db open_trans |> Or_error.ok_exn |> command_ok_exn;
  psql_exec db declare_query |> Or_error.ok_exn |> command_ok_exn;
  let loader = load_tuples_list schema in
  Seq.unfold ~init:() ~f:(fun () ->
      let tups =
        psql_exec db fetch_query |> Or_error.ok_exn |> loader |> Or_error.ok_exn
      in
      if List.length tups > 0 then Some (tups, ())
      else (
        psql_exec db close_trans |> Or_error.ok_exn |> command_ok_exn;
        close db;
        None))

let exec_to_file ~fn db schema query =
  Out_channel.with_file fn ~f:(fun ch ->
      exec_cursor_exn db schema query
      |> Seq.concat_map ~f:Seq.of_list
      |> Seq.iter ~f:(fun row ->
             Sexp.output_mach ch @@ [%sexp_of: Value.t list] row))

let exec_from_file ~fn =
  let ch = In_channel.create fn in
  let lexbuf = Lexing.from_channel ch in
  Seq.unfold ~init:() ~f:(fun () ->
      Sexp.scan_sexp_opt lexbuf
      |> Option.map ~f:(fun s -> ([%of_sexp: Value.t list] s, ())))

let check db sql =
  let open Or_error.Let_syntax in
  let name = Fresh.name Global.fresh "check%d" in
  let%bind () = command_ok (db.conn#prepare name sql) in
  command_ok (db.conn#exec (sprintf "deallocate %s" name))

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

    let load_tuples = load_tuples_list schema in

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
            psql_exec db
            @@ sprintf "select pg_cancel_backend(%d);" conn#backend_pid
            |> ignore;
            psql_exec db
            @@ sprintf "select pg_terminate_backend(%d);" conn#backend_pid
            |> ignore;
            fail `Timeout
          in

          let read_results k =
            match conn#get_result with
            | Some r -> (
                match r#status with
                | Tuples_ok ->
                    info (fun m -> m "Got a %d tuple batch" r#ntuples);
                    let%lwt () =
                      strm#push
                        (Result.return (load_tuples r |> Or_error.ok_exn))
                    in
                    k ()
                | Fatal_error -> fail (`Msg r#error)
                | _ -> fail (`Msg "Unexpected status"))
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
      (Schema_types.types r, Sql.of_ralgebra r |> Sql.to_string)
end
