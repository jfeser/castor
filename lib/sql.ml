open! Core
open Collections
open Abslayout
open Bos

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

let create_query ?(distinct = false) ?(conds = []) ?(relations = []) ?(order = [])
    ?(group = []) ?limit select =
  {select; distinct; conds; relations; order; group; limit}

let create_entry ?alias ?cast pred =
  match (alias, Pred.to_name pred) with
  | Some a, _ -> {pred; alias= a; cast}
  | _, Some n ->
      let fmt = Scanf.format_from_string (Name.to_var n ^ "_%d") "%d" in
      let new_n = Fresh.name Global.fresh fmt in
      {pred; alias= new_n; cast}
  | None, None -> {pred; alias= Fresh.name Global.fresh "x%d"; cast}

let select_entry_name {alias; _} = alias

let to_schema = function
  | Query {select; _} | Union_all ({select; _} :: _) ->
      List.map select ~f:select_entry_name
  | Union_all [] -> failwith "No empty unions."

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
let create_subquery q =
  let alias = Fresh.name Global.fresh "t%d" in
  let select_list =
    to_schema q |> List.map ~f:(fun n -> create_entry (Name (Name.create n)))
  in
  create_query ~relations:[(`Subquery (q, alias), `Left)] select_list

let to_spj = function Query q -> q | Union_all _ as q -> create_subquery q

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

let join schema1 schema2 sql1 sql2 pred =
  let needs_subquery =
    to_distinct sql1 || to_distinct sql2
    || List.length (to_group sql1) > 0
    || List.length (to_group sql2) > 0
    || Option.is_some (to_limit sql1)
    || Option.is_some (to_limit sql2)
    || has_aggregates sql1 || has_aggregates sql2
  in
  let spj1 = if needs_subquery then create_subquery sql1 else to_spj sql1 in
  let spj2 = if needs_subquery then create_subquery sql2 else to_spj sql2 in
  (* The where clause is evaluated before the select clause so we can't use the
     aliases in the select clause here. *)
  let select_list = spj1.select @ spj2.select in
  let ctx = make_ctx (schema1 @ schema2) select_list in
  let pred = Pred.subst ctx pred in
  Query
    (create_query
       ~conds:(pred :: (spj1.conds @ spj2.conds))
       ~relations:(spj1.relations @ spj2.relations)
       select_list)

let order_by of_ralgebra key r =
  let spj = to_spj (of_ralgebra r) in
  let key =
    let ctx = make_ctx (schema_exn r) spj.select in
    List.map key ~f:(fun (p, o) -> (Pred.subst ctx p, o))
  in
  Query {spj with order= key}

let select ?groupby of_ralgebra ps r =
  let sql = of_ralgebra r in
  (* Creating a subquery is always safe, but we want to avoid it if possible. We
     don't need a subquery if:
     - This select is aggregation free
     - This select has aggregates but the inner select is aggregation free *)
  let needs_subquery =
    let for_agg =
      match
        ( select_kind ps
        , to_select sql |> List.map ~f:(fun {pred= p; _} -> p) |> select_kind )
      with
      | `Scalar, _ | `Agg, `Scalar -> false
      | `Agg, `Agg -> true
    in
    (* If the inner query is distinct then it must be wrapped. *)
    let for_distinct = to_distinct sql in
    for_agg || for_distinct
  in
  let spj = if needs_subquery then create_subquery sql else to_spj sql in
  let sctx = make_ctx (schema_exn r) spj.select in
  let fields = List.map ps ~f:(fun p -> p |> Pred.subst sctx |> create_entry) in
  let spj = {spj with select= fields} in
  let spj =
    match groupby with
    | Some key ->
        let group = List.map key ~f:(Pred.subst sctx) in
        {spj with group}
    | None -> spj
  in
  Query spj

let filter of_ralgebra pred r =
  let sql = of_ralgebra r in
  (* If the inner query
     contains aggregates, then it must be put in a subquery. *)
  let needs_subquery = has_aggregates sql in
  let spj = if needs_subquery then create_subquery sql else to_spj sql in
  (* The where clause is evaluated before the select clause so we can't use the
     aliases in the select clause here. *)
  let pred =
    let ctx = make_ctx (schema_exn r) spj.select in
    Pred.subst ctx pred
  in
  Query {spj with conds= pred :: spj.conds}

let dep_join of_ralgebra q1 scope q2 =
  let sql1 = Query {(to_spj (of_ralgebra q1)) with order= []} in
  let sql1_names = to_schema sql1 in
  let q2 =
    (* Create a context that maps the names emitted by q1 (scoped) to the
       names emitted by the q1 sql. *)
    let ctx =
      List.zip_exn (schema_exn q1 |> Schema.scoped scope) sql1_names
      |> List.map ~f:(fun (n, n') -> (n, Name Name.(create ?type_:(Name.type_ n) n'))
         )
      |> Map.of_alist_exn (module Name)
    in
    subst ctx q2
  in
  let sql2 = Query {(of_ralgebra q2 |> to_spj) with order= []} in
  let sql2_names = to_schema sql2 in
  let select_list =
    List.map sql2_names ~f:(fun n -> create_entry (Name (Name.create n)))
  in
  Query
    (create_query
       ~relations:
         [ (`Subquery (sql1, Fresh.name Global.fresh "t%d"), `Left)
         ; (`Subquery (sql2, Fresh.name Global.fresh "t%d"), `Lateral) ]
       select_list)

let of_ralgebra r =
  let rec f r =
    match r.node with
    | As _ -> failwith "Unexpected as."
    | Dedup r -> Query {(to_spj (f r)) with distinct= true}
    | Relation {r_name= tbl; _} ->
        let tbl_alias = tbl ^ Fresh.name Global.fresh "_%d" in
        (* Add table alias to all fields to generate a select list. *)
        let select_list =
          List.map (schema_exn r) ~f:(fun n ->
              create_entry (Name (Name.copy n ~scope:(Some tbl_alias))) )
        in
        let relations = [(`Table (tbl, tbl_alias), `Left)] in
        Query (create_query ~relations select_list)
    | Filter (pred, r) -> filter f pred r
    | OrderBy {key; rel= r} -> order_by f key r
    | Select (fs, r) -> select f fs r
    | DepJoin {d_lhs; d_rhs; d_alias} -> dep_join f d_lhs d_alias d_rhs
    | Join {pred; r1; r2} -> join (schema_exn r1) (schema_exn r2) (f r1) (f r2) pred
    | GroupBy (ps, key, r) ->
        let key = List.map ~f:(fun n -> Name n) key in
        select ~groupby:key f ps r
    | ATuple ([], _) -> failwith "Empty tuple."
    | ATuple ([r], (Cross | Concat)) -> f r
    | ATuple (r :: rs, Cross) ->
        let q1 = r in
        let q2 = tuple rs Cross in
        join (schema_exn q1) (schema_exn q2) (f q1) (f q2) (Bool true)
    | ATuple (rs, Concat) -> Union_all (List.map ~f:(fun r -> to_spj (f r)) rs)
    | AEmpty -> Query (create_query ~limit:0 [])
    | AScalar p -> Query (create_query [create_entry p])
    | ATuple (_, Zip) ->
        Error.(create "Unsupported." r [%sexp_of: Abslayout.t] |> raise)
    | AList (rk, rv) -> f (list_to_depjoin rk rv)
    | AHashIdx (rk, rv, m) -> f (hash_idx_to_depjoin rk rv m)
    | AOrderedIdx (rk, rv, m) -> f (ordered_idx_to_depjoin rk rv m)
  in
  ensure_alias r |> f

let rec pred_to_sql p =
  let p2s = pred_to_sql in
  match p with
  | As_pred (p, _) -> p2s p
  | Name n -> sprintf "%s" (Name.to_sql n)
  | Int x -> Int.to_string x
  | Fixed x -> Fixed_point.to_string x
  | Date x -> sprintf "date('%s')" (Date.to_string x)
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> sprintf "'%s'" s
  | Null None -> "null"
  | Null (Some t) -> sprintf "(null::%s)" (Type.PrimType.to_sql t)
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
      let sql = of_ralgebra r |> to_string in
      sprintf "exists (%s)" sql
  | First r ->
      let sql = of_ralgebra r |> to_string in
      sprintf "(%s)" sql
  | Substring (p1, p2, p3) ->
      sprintf "substring(%s from %s for %s)" (p2s p1) (p2s p2) (p2s p3)
  | Count -> "count(*)"
  | Sum n -> sprintf "sum(%s)" (p2s n)
  | Avg n -> sprintf "avg(%s)" (p2s n)
  | Min n -> sprintf "min(%s)" (p2s n)
  | Max n -> sprintf "max(%s)" (p2s n)

and spj_to_sql {select; distinct; order; group; relations; conds; limit} =
  let select =
    (* If there is no grouping key and there are aggregates in the select list,
       then we need to deal with any non-aggregates in the select. *)
    if List.is_empty group then
      match select_kind (List.map select ~f:(fun {pred= p; _} -> p)) with
      | `Agg ->
          List.map select ~f:(fun ({pred= p; _} as entry) ->
              { entry with
                pred= (match Pred.kind p with `Agg -> p | `Scalar -> Min p) } )
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
          | Some t_sql -> sprintf "%s::%s %s" (pred_to_sql p) t_sql alias_sql
          | None -> sprintf "%s %s" (pred_to_sql p) alias_sql )
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
            | `Subquery (q, alias) -> sprintf "(%s) as \"%s\"" (to_string q) alias
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
      List.map conds ~f:(fun p -> p |> pred_to_sql |> sprintf "(%s)")
      |> String.concat ~sep:" and " |> sprintf "where %s"
  in
  let group_sql =
    if List.is_empty group then ""
    else
      let group_keys = List.map group ~f:pred_to_sql |> String.concat ~sep:", " in
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
            sprintf "%s %s" (pred_to_sql p) dir_sql )
        |> String.concat ~sep:", "
      in
      sprintf "order by %s" order_keys
  in
  let limit_sql = match limit with Some l -> sprintf "limit %d" l | None -> "" in
  sprintf "%s %s %s %s %s %s" select_sql relation_sql cond_sql group_sql order_sql
    limit_sql
  |> String.strip

and to_string = function
  | Query q -> spj_to_sql q
  | Union_all qs ->
      List.map qs ~f:(fun q -> sprintf "(%s)" (spj_to_sql q))
      |> String.concat ~sep:" union all "

let to_string_hum sql =
  let sql_str = to_string sql in
  let inp = OS.Cmd.in_string sql_str in
  let out = OS.Cmd.run_io Cmd.(v "pg_format") inp in
  match OS.Cmd.to_string ~trim:true out with
  | Ok sql' -> sql'
  | Error msg ->
      Logs.warn (fun m -> m "Formatting sql failed: %a." Rresult.R.pp_msg msg) ;
      sql_str
