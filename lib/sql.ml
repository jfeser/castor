open Base
open Stdio
open Printf
open Collections
open Abslayout

type ctx = {fresh: Fresh.t}

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

let create_ctx ?fresh () = {fresh= Option.value fresh ~default:(Fresh.create ())}

let create_query ?(distinct = false) ?(conds = []) ?(relations = []) ?(order = [])
    ?(group = []) ?limit select =
  {select; distinct; conds; relations; order; group; limit}

let to_schema = function
  | Query {select; _} | Union_all ({select; _} :: _) ->
      List.map select ~f:(fun (_, n, _) -> n)
  | Union_all [] -> failwith "No empty unions."

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

let to_select = function
  | Union_all qs -> List.concat_map qs ~f:(fun {select; _} -> select)
  | Query {select; _} -> select

(** Convert a query to an SPJ by introducing a subquery. *)
let create_subquery ctx q =
  let alias = Fresh.name ctx.fresh "t%d" in
  let select_list =
    to_schema q |> List.map ~f:(fun n -> (Name (Name.create n), n, None))
  in
  create_query ~relations:[(`Subquery (q, alias), `Left)] select_list

let to_spj ctx = function Query q -> q | Union_all _ as q -> create_subquery ctx q

let add_pred_alias ctx p =
  let pred_to_name = function Name n -> Some n | _ -> None in
  let fresh_sql_name n =
    match n.Name.relation with
    | Some r -> sprintf "%s_%s_%d" r n.name (Fresh.int ctx.fresh)
    | None -> sprintf "%s_%d" n.name (Fresh.int ctx.fresh)
  in
  let alias =
    match pred_to_name p with
    | Some n -> fresh_sql_name n
    | None -> Fresh.name ctx.fresh "x%d"
  in
  (p, alias, None)

let subst_ctx sql schema =
  List.zip_exn schema (to_schema sql)
  |> List.map ~f:(fun (n, n') -> (n, Name (Name.create n')))
  |> Map.of_alist_exn (module Name.Compare_no_type)

let join ctx s1 s2 sql1 sql2 pred =
  let a1 = Fresh.name ctx.fresh "t%d" in
  let a2 = Fresh.name ctx.fresh "t%d" in
  let ctx1 = subst_ctx sql1 s1 in
  let ctx2 = subst_ctx sql2 s2 in
  let pred = subst_pred (Map.merge_exn ctx1 ctx2) pred in
  let spj1 = to_spj ctx sql1 in
  let spj2 = to_spj ctx sql2 in
  let select_list =
    List.map (spj1.select @ spj2.select) ~f:(fun (_, n, _) ->
        (Name (Name.create n), n, None) )
  in
  Query
    (create_query ~conds:[pred]
       ~relations:[(`Subquery (sql1, a1), `Left); (`Subquery (sql2, a2), `Left)]
       select_list)

let order_by ctx schema sql key order =
  let spj = to_spj ctx sql in
  let ctx =
    List.map2_exn schema spj.select ~f:(fun n (p, _, _) -> (n, p))
    |> Map.of_alist_exn (module Name.Compare_no_type)
  in
  let key = List.map key ~f:(fun p -> (subst_pred ctx p, order)) in
  Query {spj with order= key}

let select ctx schema sql fields =
  (* Creating a subquery is always safe, but we want to avoid it if possible. We
     don't need a subquery if:
     - This select is aggregation free
     - This select has aggregates but the inner select is aggregation free *)
  let needs_subquery =
    match
      ( select_kind fields
      , to_select sql |> List.map ~f:(fun (p, _, _) -> p) |> select_kind )
    with
    | `Scalar, _ | `Agg, `Scalar -> false
    | `Agg, `Agg -> true
  in
  let spj = if needs_subquery then create_subquery ctx sql else to_spj ctx sql in
  let sctx =
    List.map2_exn schema spj.select ~f:(fun n (p, _, _) -> (n, p))
    |> Map.of_alist_exn (module Name.Compare_no_type)
  in
  let fields =
    List.map fields ~f:(fun p -> p |> subst_pred sctx |> add_pred_alias ctx)
  in
  Query {spj with select= fields}

let filter ctx schema sql pred =
  (* If the inner query
     contains aggregates, then it must be put in a subquery. *)
  let needs_subquery =
    match to_select sql |> List.map ~f:(fun (p, _, _) -> p) |> select_kind with
    | `Scalar -> false
    | `Agg -> true
  in
  let spj = if needs_subquery then create_subquery ctx sql else to_spj ctx sql in
  (* The where clause is evaluated before the select clause so we can't use the
     aliases in the select clause here. *)
  let ctx =
    List.zip_exn schema spj.select
    |> List.map ~f:(fun (n, (p, _, _)) -> (n, p))
    |> Map.of_alist_exn (module Name.Compare_no_type)
  in
  let pred = subst_pred ctx pred in
  Query {spj with conds= pred :: spj.conds}

let of_ralgebra ctx r =
  let rec f ({node; _} as r) =
    match node with
    | As (_, r) -> f r
    | Dedup r -> Query {(to_spj ctx (f r)) with distinct= true}
    | Scan tbl ->
        Query
          (create_query
             ~relations:[(`Table tbl, `Left)]
             (List.map
                Meta.(find_exn r schema)
                ~f:(fun n ->
                  let p = Name n in
                  add_pred_alias ctx p )))
    | Filter (pred, r) -> filter ctx Meta.(find_exn r schema) (f r) pred
    | OrderBy {key; order; rel= r} ->
        order_by ctx Meta.(find_exn r schema) (f r) key order
    | Select (fs, r) -> select ctx Meta.(find_exn r schema) (f r) fs
    | Join {pred; r1; r2} ->
        join ctx
          Meta.(find_exn r1 schema)
          Meta.(find_exn r2 schema)
          (f r1) (f r2) pred
    | GroupBy (ps, key, r) ->
        let sql = f r in
        let sctx = subst_ctx sql Meta.(find_exn r schema) in
        let key = List.map key ~f:(fun p -> subst_pred sctx (Name p)) in
        let preds =
          List.map ps ~f:(fun p -> p |> subst_pred sctx |> add_pred_alias ctx)
        in
        let alias = Fresh.name ctx.fresh "t%d" in
        Query
          (create_query
             ~relations:[(`Subquery (sql, alias), `Left)]
             ~group:key preds)
    | AEmpty | AScalar _ | AList _ | ATuple _ | AHashIdx _ | AOrderedIdx _ ->
        Error.of_string "Only relational algebra constructs allowed." |> Error.raise
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
      List.map conds ~f:(pred_to_sql ctx)
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
              | `Asc -> (* Asc is the default order *) ""
              | `Desc -> "desc"
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
  let in_ch, out_ch = Unix.open_process "pg_format" in
  Out_channel.output_string out_ch (to_string ctx sql) ;
  Out_channel.close out_ch ;
  In_channel.input_all in_ch
