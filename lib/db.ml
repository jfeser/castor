open Base
open Printf
module Pervasives = Caml.Pervasives
open Collections
module Psql = Postgresql
module Stream = Caml.Stream

let () =
  Caml.Printexc.register_printer (function
    | Postgresql.Error e -> Some (Postgresql.string_of_error e)
    | _ -> None )

type t =
  { uri: string
  ; conn: Psql.connection sexp_opaque [@compare.ignore]
  ; fresh: Fresh.t sexp_opaque [@compare.ignore] }
[@@deriving compare, sexp]

let create uri =
  {uri; conn= new Psql.connection ~conninfo:uri (); fresh= Fresh.create ()}

let subst_params params query =
  match params with
  | [] -> query
  | _ ->
      List.foldi params ~init:query ~f:(fun i q v ->
          String.substr_replace_all ~pattern:(Printf.sprintf "$%d" i) ~with_:v q )

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
               if String.(strip nullable_str = "YES") then
                 let nulls =
                   exec1 ~params:[fname; rname] conn
                     "select \"$0\" from \"$1\" where \"$0\" is null limit 1"
                 in
                 List.length nulls > 0
               else false
             in
             let type_ =
               match type_str with
               | "character" | "character varying" | "varchar" | "text" ->
                   StringT {nullable}
               | "interval" | "integer" | "smallint" | "bigint" -> IntT {nullable}
               | "date" -> DateT {nullable}
               | "boolean" -> BoolT {nullable}
               | "numeric" ->
                   let min, max, max_scale =
                     exec3 ~params:[fname; rname] conn
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
                     if fits_in_an_int63 then IntT {nullable} else StringT {nullable}
                   else FixedT {nullable}
               | "real" | "double" -> FixedT {nullable}
               | "timestamp without time zone" -> StringT {nullable}
               | s -> failwith (Printf.sprintf "Unknown dtype %s" s)
             in
             {fname; type_} )
    in
    {rel with fields}

  let from_db' = Core.Memo.general (fun (conn, rname) -> from_db conn rname)

  let from_db conn rname = from_db' (conn, rname)

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
  | StringT _ -> Ok (String value)
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
              else load_value type_ (r#getvalue tidx fidx) |> Or_error.ok_exn ) )
    in
    gen

let exec_cursor_exn =
  let fresh = Fresh.create () in
  fun ?(batch_size = 10000) ?(params = []) db schema query ->
    let db = create db.uri in
    Caml.Gc.finalise (fun db -> try (db.conn)#finish with Failure _ -> ()) db ;
    let query = subst_params params query in
    let cur = Fresh.name fresh "cur%d" in
    let declare_query =
      sprintf "begin transaction; declare %s cursor for %s;" cur query
    in
    let fetch_query = sprintf "fetch %d from %s;" batch_size cur in
    exec db declare_query |> command_ok ;
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
              Some (tups, state) )
        (`Not_done 1)
      |> Gen.flatten
    in
    seq

let check db sql =
  let name = Fresh.name db.fresh "check%d" in
  let r = (db.conn)#prepare name sql in
  match r#status with
  | Command_ok -> Or_error.return ()
  | _ -> Or_error.error "Unexpected query response." r#error [%sexp_of: string]

let schema conn table =
  (Relation.from_db conn table).fields
  |> List.map ~f:(fun f -> Name.create ~relation:table ~type_:f.Field.type_ f.fname)
