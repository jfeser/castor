open Base
open Stdio
open Printf
open Collections
open Abslayout

type order = (pred * [`Asc | `Desc]) list [@@deriving compare, sexp_of]

type select = (pred * string * Type.PrimType.t option) list
[@@deriving compare, sexp_of]

type spj =
  { select: select
  ; distinct: bool
  ; conds: pred list
  ; relations:
      ([`Subquery of t * string | `Table of string] * [`Left | `Lateral]) list
  ; order: order
  ; group: pred list
  ; limit: int option }

and t = Query of spj | Union_all of spj list [@@deriving compare, sexp_of]

let create_query ?(distinct = false) ?(conds = []) ?(relations = []) ?(order = [])
    ?(group = []) ?limit select =
  {select; distinct; conds; relations; order; group; limit}

let rec to_schema = function
  | Query {select; _} -> List.map select ~f:(fun (_, n, _) -> n)
  | Union_all [] -> failwith "No empty unions."
  | Union_all (q :: _) -> to_schema (Query q)

let global_fresh = Fresh.create ()

let to_order = function
  | Union_all _ -> Ok []
  | Query {select; order; _} ->
      List.map order ~f:(fun (p, o) ->
          let alias =
            List.find_map select ~f:(fun (p', a, _) ->
                if [%compare.equal: pred] p p' then Some a else None )
          in
          match alias with
          | Some a -> Ok (Name (Name.create a), o)
          | None ->
              Error
                (Error.create "Order clause depends on hidden expression."
                   (p, select) [%sexp_of: pred * select]) )
      |> Or_error.all

let to_spj ?(fresh = global_fresh) = function
  | Query q -> q
  | Union_all _ as q ->
      let alias = Fresh.name fresh "t%d" in
      let select_list =
        to_schema q |> List.map ~f:(fun n -> (Name (Name.create n), n, None))
      in
      create_query ~relations:[(`Subquery (q, alias), `Left)] select_list

let add_pred_alias ?(fresh = global_fresh) p =
  let pred_to_name = function Name n -> Some n | _ -> None in
  let fresh_sql_name n =
    match n.Name.relation with
    | Some r -> sprintf "%s_%s_%d" r n.name (Fresh.int fresh)
    | None -> sprintf "%s_%d" n.name (Fresh.int fresh)
  in
  let alias =
    match pred_to_name p with
    | Some n -> fresh_sql_name n
    | None -> Fresh.name fresh "x%d"
  in
  (p, alias, None)

let subst_ctx sql schema =
  List.zip_exn schema (to_schema sql)
  |> List.map ~f:(fun (n, n') -> (n, Name (Name.create n')))
  |> Map.of_alist_exn (module Name.Compare_no_type)

let join ?(fresh = global_fresh) s1 s2 sql1 sql2 pred =
  let a1 = Fresh.name fresh "t%d" in
  let a2 = Fresh.name fresh "t%d" in
  let ctx1 = subst_ctx sql1 s1 in
  let ctx2 = subst_ctx sql2 s2 in
  let pred = subst_pred (Map.merge_exn ctx1 ctx2) pred in
  let spj1 = to_spj sql1 in
  let spj2 = to_spj sql2 in
  let select_list =
    List.map (spj1.select @ spj2.select) ~f:(fun (_, n, _) ->
        (Name (Name.create n), n, None) )
  in
  Query
    (create_query ~conds:[pred]
       ~relations:[(`Subquery (sql1, a1), `Left); (`Subquery (sql2, a2), `Left)]
       select_list)

let order_by schema sql key order =
  let spj = to_spj sql in
  let ctx =
    List.map2_exn schema spj.select ~f:(fun n (p, _, _) -> (n, p))
    |> Map.of_alist_exn (module Name.Compare_no_type)
  in
  let key = List.map key ~f:(fun p -> (subst_pred ctx p, order)) in
  Query {spj with order= key}

let select ?(fresh = global_fresh) schema sql fields =
  let spj = to_spj sql in
  let ctx =
    List.map2_exn schema spj.select ~f:(fun n (p, _, _) -> (n, p))
    |> Map.of_alist_exn (module Name.Compare_no_type)
  in
  let fields =
    List.map fields ~f:(fun p -> p |> subst_pred ctx |> add_pred_alias ~fresh)
  in
  Query {(to_spj sql) with select= fields}

let filter schema sql pred =
  let spj = to_spj sql in
  (* The where clause is evaluated before the select clause so we can't use the
     aliases in the select clause here. *)
  let ctx =
    List.zip_exn schema spj.select
    |> List.map ~f:(fun (n, (p, _, _)) -> (n, p))
    |> Map.of_alist_exn (module Name.Compare_no_type)
  in
  let pred = subst_pred ctx pred in
  Query {spj with conds= pred :: spj.conds}

let of_ralgebra ?(fresh = global_fresh) r =
  let rec f ({node; _} as r) =
    match node with
    | As (_, r) -> f r
    | Dedup r -> Query {(to_spj (f r)) with distinct= true}
    | Scan tbl ->
        Query
          (create_query
             ~relations:[(`Table tbl, `Left)]
             (List.map
                Meta.(find_exn r schema)
                ~f:(fun n ->
                  let p = Name n in
                  add_pred_alias ~fresh p )))
    | Filter (pred, r) -> filter Meta.(find_exn r schema) (f r) pred
    | OrderBy {key; order; rel= r} ->
        order_by Meta.(find_exn r schema) (f r) key order
    | Select (fs, r) -> select ~fresh Meta.(find_exn r schema) (f r) fs
    | Join {pred; r1; r2} ->
        join ~fresh
          Meta.(find_exn r1 schema)
          Meta.(find_exn r2 schema)
          (f r1) (f r2) pred
    | GroupBy (ps, key, r) ->
        let sql = f r in
        let ctx = subst_ctx sql Meta.(find_exn r schema) in
        let key = List.map key ~f:(fun p -> subst_pred ctx (Name p)) in
        let preds =
          List.map ps ~f:(fun p -> p |> subst_pred ctx |> add_pred_alias ~fresh)
        in
        let alias = Fresh.name fresh "t%d" in
        Query
          (create_query
             ~relations:[(`Subquery (sql, alias), `Left)]
             ~group:key preds)
    | AEmpty | AScalar _ | AList _ | ATuple _ | AHashIdx _ | AOrderedIdx _ ->
        Error.of_string "Only relational algebra constructs allowed." |> Error.raise
  in
  f r

let rec pred_to_sql = function
  | As_pred (p, _) -> pred_to_sql p
  | Name n -> sprintf "%s" (Name.to_sql n)
  | Int x -> Int.to_string x
  | Fixed x -> Fixed_point.to_string x
  | Date x -> sprintf "date('%s')" (Core.Date.to_string x)
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> sprintf "'%s'" s
  | Null -> "null"
  | Unop (op, p) -> (
      let s = sprintf "(%s)" (pred_to_sql p) in
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
      let s1 = sprintf "(%s)" (pred_to_sql p1) in
      let s2 = sprintf "(%s)" (pred_to_sql p2) in
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
      sprintf "case when %s then %s else %s end" (pred_to_sql p1) (pred_to_sql p2)
        (pred_to_sql p3)
  | Exists r ->
      let sql = of_ralgebra r |> to_string in
      sprintf "exists (%s)" sql
  | First r ->
      let sql = of_ralgebra r |> to_string in
      sprintf "(%s)" sql
  | Substring (p1, p2, p3) ->
      sprintf "substring(%s from %s for %s)" (pred_to_sql p1) (pred_to_sql p2)
        (pred_to_sql p3)
  | Count -> "count(*)"
  | Sum n -> sprintf "sum(%s)" (pred_to_sql n)
  | Avg n -> sprintf "avg(%s)" (pred_to_sql n)
  | Min n -> sprintf "min(%s)" (pred_to_sql n)
  | Max n -> sprintf "max(%s)" (pred_to_sql n)

and spj_to_sql {select; distinct; order; group; relations; conds; limit} =
  let select =
    (* If there is no grouping key and there are aggregates in the select list,
       then we need to deal with any non-aggregates in the select. *)
    if List.is_empty group then
      match select_kind (List.map select ~f:(fun (p, _, _) -> p)) with
      | `Agg ->
          List.map select ~f:(fun (p, n, t) ->
              let p' = match pred_kind p with `Agg -> p | `Scalar -> Min p in
              (p', n, t) )
      | `Scalar -> select
    else select
  in
  let select_sql =
    let select_list =
      List.map select ~f:(fun (p, n, t) ->
          let alias_sql =
            match p with
            | Name n' when Name.Compare_no_type.(Name.create n = n') -> ""
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
            | `Table t -> t
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
      List.map conds ~f:pred_to_sql
      |> String.concat ~sep:" and " |> sprintf "where %s"
  in
  let group_sql =
    if List.is_empty group then ""
    else
      let group_keys =
        List.map group ~f:(fun p -> pred_to_sql p) |> String.concat ~sep:", "
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
              | `Asc -> (* Asc is the default order *) ""
              | `Desc -> "desc"
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
  let in_ch, out_ch = Unix.open_process "pg_format" in
  Out_channel.output_string out_ch (to_string sql) ;
  Out_channel.close out_ch ;
  In_channel.input_all in_ch
