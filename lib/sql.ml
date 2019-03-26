open Core
open Printf
open Collections
open Abslayout
open Bos

type ctx = {fresh: Fresh.t; fresh_names: (Name.t * int ref) Hashtbl.M(Name).t}

type select_entry = {pred: pred; alias: string; cast: Type.PrimType.t option}
[@@deriving compare, sexp_of]

type spj =
  { select: select_entry list
  ; distinct: bool
  ; conds: pred list
  ; relations:
      ([`Subquery of t * string | `Table of string * string] * [`Left | `Lateral])
      list
  ; order: (pred * order) list
  ; group: pred list
  ; limit: int option }

and t = Query of spj | Union_all of spj list [@@deriving compare, sexp_of]

let create_ctx ?fresh () =
  { fresh= Option.value fresh ~default:(Fresh.create ())
  ; fresh_names= Hashtbl.create (module Name) }

let create_query ?(distinct = false) ?(conds = []) ?(relations = []) ?(order = [])
    ?(group = []) ?limit select =
  {select; distinct; conds; relations; order; group; limit}

let create_entry ~ctx ?alias ?cast pred =
  match (alias, pred_to_name pred) with
  | Some a, _ -> {pred; alias= a; cast}
  | _, Some n ->
      let orig_n, id =
        Hashtbl.find_or_add ctx.fresh_names n ~default:(fun () -> (n, ref 0))
      in
      incr id ;
      let new_n = sprintf "%s_%d" (Name.to_var orig_n) !id in
      Hashtbl.set ctx.fresh_names ~key:(Name.create new_n) ~data:(orig_n, id) ;
      {pred; alias= new_n; cast}
  | None, None -> {pred; alias= Fresh.name ctx.fresh "x%d"; cast}

let select_entry_name {alias; _} = alias

let to_schema = function
  | Query {select; _} | Union_all ({select; _} :: _) ->
      List.map select ~f:select_entry_name
  | Union_all [] -> failwith "No empty unions."

let to_order = function
  | Union_all _ -> Ok []
  | Query {select; order; _} ->
      List.map order ~f:(fun (p, o) ->
          let alias =
            List.find_map select ~f:(fun {pred; alias; _} ->
                if [%compare.equal: pred] p pred then Some alias else None )
          in
          match alias with
          | Some n -> Ok (Name (Name.create n), o)
          | None ->
              Error
                (Error.create "Order clause depends on hidden expression."
                   (p, select) [%sexp_of: pred * select_entry list]) )
      |> Or_error.all

let to_select = function
  | Union_all qs -> List.concat_map qs ~f:(fun {select; _} -> select)
  | Query {select; _} -> select

let to_group = function Union_all _ -> [] | Query q -> q.group

let to_distinct = function Union_all _ -> false | Query q -> q.distinct

let to_limit = function Union_all _ -> None | Query q -> q.limit

let has_aggregates sql =
  match to_select sql |> List.map ~f:(fun {pred= p; _} -> p) |> select_kind with
  | `Scalar -> false
  | `Agg -> true

(** Convert a query to an SPJ by introducing a subquery. *)
let create_subquery ctx q =
  let alias = Fresh.name ctx.fresh "t%d" in
  let select_list =
    to_schema q |> List.map ~f:(fun n -> create_entry ~ctx (Name (Name.create n)))
  in
  create_query ~relations:[(`Subquery (q, alias), `Left)] select_list

let to_spj ctx = function Query q -> q | Union_all _ as q -> create_subquery ctx q

let make_ctx schema select =
  if List.length schema <> List.length select then
    Error.create "Mismatched schema and select." (schema, select)
      [%sexp_of: Name.t list * select_entry list]
    |> Error.raise ;
  List.fold2_exn schema select
    ~init:(Map.empty (module Name))
    ~f:(fun m n {pred= p; _} ->
      if Map.mem m n then
        Logs.warn (fun m -> m "Duplicate name in schema %a." Name.pp n) ;
      Map.set m ~key:n ~data:p )

let join ctx schema1 schema2 sql1 sql2 pred =
  let needs_subquery =
    to_distinct sql1 || to_distinct sql2
    || List.length (to_group sql1) > 0
    || List.length (to_group sql2) > 0
    || Option.is_some (to_limit sql1)
    || Option.is_some (to_limit sql2)
    || has_aggregates sql1 || has_aggregates sql2
  in
  let spj1 = if needs_subquery then create_subquery ctx sql1 else to_spj ctx sql1 in
  let spj2 = if needs_subquery then create_subquery ctx sql2 else to_spj ctx sql2 in
  (* The where clause is evaluated before the select clause so we can't use the
     aliases in the select clause here. *)
  let select_list = spj1.select @ spj2.select in
  let ctx = make_ctx (schema1 @ schema2) select_list in
  let pred = subst_pred ctx pred in
  Query
    (create_query
       ~conds:(pred :: (spj1.conds @ spj2.conds))
       ~relations:(spj1.relations @ spj2.relations)
       select_list)

let order_by ctx schema sql key =
  let spj = to_spj ctx sql in
  let ctx = make_ctx schema spj.select in
  let key = List.map key ~f:(fun (p, o) -> (subst_pred ctx p, o)) in
  Query {spj with order= key}

let select ?groupby ctx schema sql fields =
  (* Creating a subquery is always safe, but we want to avoid it if possible. We
     don't need a subquery if:
     - This select is aggregation free
     - This select has aggregates but the inner select is aggregation free *)
  let needs_subquery =
    let for_agg =
      match
        ( select_kind fields
        , to_select sql |> List.map ~f:(fun {pred= p; _} -> p) |> select_kind )
      with
      | `Scalar, _ | `Agg, `Scalar -> false
      | `Agg, `Agg -> true
    in
    (* If the inner query is distinct then it must be wrapped. *)
    let for_distinct = to_distinct sql in
    for_agg || for_distinct
  in
  let spj = if needs_subquery then create_subquery ctx sql else to_spj ctx sql in
  let sctx = make_ctx schema spj.select in
  let fields =
    List.map fields ~f:(fun p -> p |> subst_pred sctx |> create_entry ~ctx)
  in
  let spj = {spj with select= fields} in
  let spj =
    match groupby with
    | Some key ->
        let group = List.map key ~f:(subst_pred sctx) in
        {spj with group}
    | None -> spj
  in
  Query spj

let filter ctx schema sql pred =
  (* If the inner query
     contains aggregates, then it must be put in a subquery. *)
  let needs_subquery = has_aggregates sql in
  let spj = if needs_subquery then create_subquery ctx sql else to_spj ctx sql in
  (* The where clause is evaluated before the select clause so we can't use the
     aliases in the select clause here. *)
  let ctx = make_ctx schema spj.select in
  let pred = subst_pred ctx pred in
  Query {spj with conds= pred :: spj.conds}

let lat_join of_ralgebra ctx s1 sql1 q2 =
  let spj1 = to_spj ctx sql1 in
  let sql1_no_order = Query {spj1 with order= []} in
  let sql1_names = to_schema (Query spj1) in
  let q2 =
    let ctx =
      List.zip_exn s1 sql1_names
      |> List.map ~f:(fun (n, n') -> (n, Name Name.(create ?type_:(Name.type_ n) n'))
         )
      |> Map.of_alist_exn (module Name)
    in
    subst ctx q2
  in
  let spj2 = of_ralgebra q2 |> to_spj ctx in
  let sql2 = Query spj2 in
  let sql2_no_order = Query {spj2 with order= []} in
  let select_list =
    let sql2_names = to_schema sql2 in
    List.map (sql1_names @ sql2_names) ~f:(fun n ->
        create_entry ~ctx (Name (Name.create n)) )
  in
  Query
    (create_query
       ~relations:
         [ (`Subquery (sql1_no_order, Fresh.name ctx.fresh "t%d"), `Left)
         ; (`Subquery (sql2_no_order, Fresh.name ctx.fresh "t%d"), `Lateral) ]
       select_list)

let of_ralgebra ctx r =
  let rec f ({node; _} as r) =
    match node with
    | As (_, r) -> f r
    | Dedup r -> Query {(to_spj ctx (f r)) with distinct= true}
    | Scan tbl ->
        let tbl_alias = sprintf "%s_%d" tbl (Fresh.int ctx.fresh) in
        (* Add table alias to all fields to generate a select list. *)
        let select_list =
          List.map
            Meta.(find_exn r schema)
            ~f:(fun n ->
              create_entry ~ctx (Name (Name.copy n ~relation:(Some tbl_alias))) )
        in
        let relations = [(`Table (tbl, tbl_alias), `Left)] in
        Query (create_query ~relations select_list)
    | Filter (pred, r) -> filter ctx Meta.(find_exn r schema) (f r) pred
    | OrderBy {key; rel= r} -> order_by ctx Meta.(find_exn r schema) (f r) key
    | Select (fs, r) -> select ctx Meta.(find_exn r schema) (f r) fs
    | Join {pred; r1; r2} ->
        join ctx
          Meta.(find_exn r1 schema)
          Meta.(find_exn r2 schema)
          (f r1) (f r2) pred
    | GroupBy (ps, key, r) ->
        let key = List.map ~f:(fun n -> Name n) key in
        select ~groupby:key ctx Meta.(find_exn r schema) (f r) ps
    | ATuple ([], _) -> failwith "Empty tuple."
    | ATuple ([r], Cross) -> f r
    | ATuple (r :: rs, Cross) ->
        lat_join f ctx Meta.(find_exn r schema) (f r) (tuple rs Cross)
    | AEmpty -> Query (create_query ~limit:0 [])
    | AScalar p -> Query (create_query [create_entry ~ctx p])
    | ATuple (_, (Concat | Zip)) ->
        Error.(create "Unsupported." r [%sexp_of: Abslayout.t] |> raise)
    | AList (rk, rv) ->
        let kschema = Meta.(find_exn rk schema) in
        let vschema = Meta.(find_exn rv schema) in
        let schema = kschema @ vschema in
        select ctx schema
          (lat_join f ctx Meta.(find_exn rk schema) (f rk) rv)
          (List.map vschema ~f:(fun n -> Name n))
    | AHashIdx (rk, rv, {lookup; _}) ->
        let kschema = Meta.(find_exn rk schema) in
        let ksql = f rk in
        let ksql =
          let p =
            List.map2_exn kschema lookup ~f:(fun n p -> Binop (Eq, Name n, p))
            |> Pred.conjoin
          in
          filter ctx kschema ksql p
        in
        lat_join f ctx kschema ksql rv
    | AOrderedIdx (rk, rv, {lookup_low; lookup_high; _}) ->
        let kschema = Meta.(find_exn rk schema) in
        let vschema = Meta.(find_exn rv schema) in
        let n = List.hd_exn kschema in
        let schema = kschema @ vschema in
        let kvsql = lat_join f ctx kschema (f rk) rv in
        let p =
          Binop
            (And, Binop (Lt, lookup_low, Name n), Binop (Lt, Name n, lookup_high))
        in
        filter ctx schema kvsql p
  in
  f r

let rec pred_to_sql ctx p =
  let p2s = pred_to_sql ctx in
  match p with
  | As_pred (p, _) -> p2s p
  | Name n -> sprintf "%s" (Name.to_sql n)
  | Int x -> Int.to_string x
  | Fixed x -> Fixed_point.to_string x
  | Date x -> sprintf "date('%s')" (Core.Date.to_string x)
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> sprintf "'%s'" s
  | Null -> "null"
  | Unop (op, p) -> (
      let s = sprintf "(%s)" (p2s p) in
      match op with
      | Not -> sprintf "not (%s)" s
      | Year -> sprintf "interval '%s year'" s
      | Month -> sprintf "interval '%s month'" s
      | Day -> sprintf "interval '%s day'" s
      | Strlen -> sprintf "char_length(%s)" s
      | ExtractY -> sprintf "cast(date_part('year', %s) as integer)" s
      | ExtractM -> sprintf "cast(date_part('month', %s) as integer)" s
      | ExtractD -> sprintf "cast(date_part('day', %s) as integer)" s )
  | Binop (op, p1, p2) -> (
      let s1 = sprintf "(%s)" (p2s p1) in
      let s2 = sprintf "(%s)" (p2s p2) in
      match op with
      | Eq -> sprintf "%s = %s" s1 s2
      | Lt -> sprintf "%s < %s" s1 s2
      | Le -> sprintf "%s <= %s" s1 s2
      | Gt -> sprintf "%s > %s" s1 s2
      | Ge -> sprintf "%s >= %s" s1 s2
      | And -> sprintf "%s and %s" s1 s2
      | Or -> sprintf "%s or %s" s1 s2
      | Add -> sprintf "%s + %s" s1 s2
      | Sub -> sprintf "%s - %s" s1 s2
      | Mul -> sprintf "%s * %s" s1 s2
      | Div -> sprintf "%s / %s" s1 s2
      | Mod -> sprintf "%s %% %s" s1 s2
      | Strpos -> sprintf "strpos(%s, %s)" s1 s2 )
  | If (p1, p2, p3) ->
      sprintf "case when %s then %s else %s end" (p2s p1) (p2s p2) (p2s p3)
  | Exists r ->
      let sql = of_ralgebra ctx r |> to_string ctx in
      sprintf "exists (%s)" sql
  | First r ->
      let sql = of_ralgebra ctx r |> to_string ctx in
      sprintf "(%s)" sql
  | Substring (p1, p2, p3) ->
      sprintf "substring(%s from %s for %s)" (p2s p1) (p2s p2) (p2s p3)
  | Count -> "count(*)"
  | Sum n -> sprintf "sum(%s)" (p2s n)
  | Avg n -> sprintf "avg(%s)" (p2s n)
  | Min n -> sprintf "min(%s)" (p2s n)
  | Max n -> sprintf "max(%s)" (p2s n)

and spj_to_sql ctx {select; distinct; order; group; relations; conds; limit} =
  let select =
    (* If there is no grouping key and there are aggregates in the select list,
       then we need to deal with any non-aggregates in the select. *)
    if List.is_empty group then
      match select_kind (List.map select ~f:(fun {pred= p; _} -> p)) with
      | `Agg ->
          List.map select ~f:(fun ({pred= p; _} as entry) ->
              { entry with
                pred= (match pred_kind p with `Agg -> p | `Scalar -> Min p) } )
      | `Scalar -> select
    else select
  in
  let select_sql =
    let select_list =
      List.map select ~f:(fun {pred= p; alias= n; cast= t} ->
          let alias_sql =
            match p with
            | Name n' when Name.O.(Name.create n = n') -> ""
            | _ -> sprintf "as \"%s\"" n
          in
          match Option.map t ~f:Type.PrimType.to_sql with
          | Some t_sql -> sprintf "%s::%s %s" (pred_to_sql ctx p) t_sql alias_sql
          | None -> sprintf "%s %s" (pred_to_sql ctx p) alias_sql )
      |> String.concat ~sep:", "
    in
    let distinct_sql = if distinct then "distinct" else "" in
    sprintf "select %s %s" distinct_sql select_list
  in
  let relation_sql =
    if List.is_empty relations then ""
    else
      List.map relations ~f:(fun (rel, join_type) ->
          let rel_str =
            match rel with
            | `Subquery (q, alias) ->
                sprintf "(%s) as \"%s\"" (to_string ctx q) alias
            | `Table (t, alias) ->
                if String.(t = alias) then sprintf "\"%s\"" t
                else sprintf "\"%s\" as \"%s\"" t alias
          in
          let join_str =
            match join_type with `Left -> "" | `Lateral -> "lateral"
          in
          sprintf "%s %s" join_str rel_str )
      |> String.concat ~sep:", " |> sprintf "from %s"
  in
  let cond_sql =
    if List.is_empty conds then ""
    else
      List.map conds ~f:(fun p -> p |> pred_to_sql ctx |> sprintf "(%s)")
      |> String.concat ~sep:" and " |> sprintf "where %s"
  in
  let group_sql =
    if List.is_empty group then ""
    else
      let group_keys =
        List.map group ~f:(pred_to_sql ctx) |> String.concat ~sep:", "
      in
      sprintf "group by (%s)" group_keys
  in
  let order_sql =
    if List.is_empty order then ""
    else
      let order_keys =
        List.map order ~f:(fun (p, dir) ->
            let dir_sql =
              match dir with
              | Asc -> (* Asc is the default order *) ""
              | Desc -> "desc"
            in
            sprintf "%s %s" (pred_to_sql ctx p) dir_sql )
        |> String.concat ~sep:", "
      in
      sprintf "order by %s" order_keys
  in
  let limit_sql = match limit with Some l -> sprintf "limit %d" l | None -> "" in
  sprintf "%s %s %s %s %s %s" select_sql relation_sql cond_sql group_sql order_sql
    limit_sql
  |> String.strip

and to_string ctx = function
  | Query q -> spj_to_sql ctx q
  | Union_all qs ->
      List.map qs ~f:(fun q -> sprintf "(%s)" (spj_to_sql ctx q))
      |> String.concat ~sep:" union all "

let to_string_hum ctx sql =
  let sql_str = to_string ctx sql in
  let inp = OS.Cmd.in_string sql_str in
  let out = OS.Cmd.run_io Cmd.(v "pg_format") inp in
  match OS.Cmd.to_string ~trim:true out with
  | Ok sql' -> sql'
  | Error msg ->
      Logs.warn (fun m -> m "Formatting sql failed: %a." Rresult.R.pp_msg msg) ;
      sql_str
