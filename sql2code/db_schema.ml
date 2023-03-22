open Core
open Castor
open Sqlgg
open Sql.Col_name

let rec iter_selects (Sql.Clause (select, rest)) k =
  k select;
  Option.iter rest ~f:(fun (_, cs) -> iter_selects cs k)

let iter_join_conds (_, rest) k = List.iter rest ~f:(fun (_, cond) -> k cond)

let rec iter_expr (expr : _ Sql.expr) k =
  match expr with
  | Value _ | Param _ | Inserted _ | Column _ -> k expr
  | Fun (_, args) ->
      k expr;
      List.iter args ~f:(fun e -> iter_expr e k)
  | Sequence _ | Choices _ | Case _ | Subquery _ ->
      raise_s [%message "unsupported" (expr : Sql.op Sql.expr)]

let query_predicates queries =
  Iter.of_list queries
  |> Iter.map (fun (query : _ Sql.query) ->
         iter_selects query.clauses
         |> Iter.map (fun (select : _ Sql.select) ->
                let join_cond_exprs =
                  Iter.of_opt select.from |> Iter.map iter_join_conds
                  |> Iter.concat
                  |> Iter.map (function
                       | `Search expr -> iter_expr expr
                       | _ -> Iter.empty)
                  |> Iter.concat
                in
                let where_exprs =
                  Iter.of_opt select.where |> Iter.map iter_expr |> Iter.concat
                in
                Iter.append join_cond_exprs where_exprs))
  |> Iter.concat |> Iter.concat |> Iter.persistent

type t = {
  tables :
    (string * Sql.Type.t * [ `Primary_key | `Foreign_key of string | `None ])
    list
    Map.M(String).t;
  indexes : (Sql.Col_name.t * [ `Hash | `Ordered ] * string * bool ref) list;
}
[@@deriving compare, sexp]

let attrs schema table =
  Db.Schema.attrs schema table
  |> List.map ~f:(fun { Db.Schema.attr_name = cname; _ } ->
         { Sql.Col_name.cname; tname = Some table })

let col_attrs schema col =
  let open Option.Let_syntax in
  let%map tname = col.tname in
  Db.Schema.attrs schema tname
  |> List.find_exn ~f:(fun (attr : Db.Schema.attr) ->
         [%equal: string] attr.attr_name col.cname)

let col_type schema col =
  Option.map (col_attrs schema col) ~f:(fun (a : Db.Schema.attr) -> a.type_)

let select_indexes queries =
  let preds = query_predicates queries in
  let eq_columns =
    Iter.map
      (function
        | Sql.Fun (`Eq, [ Column lhs; Column rhs ]) -> Iter.of_list [ lhs; rhs ]
        | Sql.Fun (`Eq, ([ Column c; _ ] | [ _; Column c ])) -> Iter.singleton c
        | _ -> Iter.empty)
      preds
    |> Iter.concat
    |> Iter.sort_uniq ~cmp:[%compare: Sql.Col_name.t]
    |> Iter.to_list
  in
  let cmp_columns =
    Iter.map
      (function
        | Sql.Fun ((`Ge | `Le | `Gt | `Lt), [ Column lhs; Column rhs ]) ->
            Iter.of_list [ lhs; rhs ]
        | Sql.Fun ((`Ge | `Le | `Gt | `Lt), ([ Column c; _ ] | [ _; Column c ]))
          ->
            Iter.singleton c
        | _ -> Iter.empty)
      preds
    |> Iter.concat
    |> Iter.sort_uniq ~cmp:[%compare: Sql.Col_name.t]
    |> Iter.to_list
  in
  (eq_columns, cmp_columns)

let find_col_eq_index db_schema col =
  List.filter_map db_schema.indexes ~f:(fun (col', kind, name, _) ->
      if [%equal: Sql.Col_name.t] col col' then
        match kind with `Hash -> Some name | `Ordered -> None
      else None)
  |> List.hd

let use_col_eq_index db_schema name =
  List.iter db_schema.indexes ~f:(fun (_, kind, name', uses) ->
      if [%equal: string] name name' then
        match kind with `Hash -> uses := true | `Ordered -> ())

let of_queries queries =
  let selects =
    List.filter_map queries ~f:(fun (query, pos) ->
        match query with Sql.Select x -> Some (x, pos) | _ -> None)
  in
  let eq, cmp = select_indexes @@ List.map ~f:fst selects in
  let indexes =
    let open Option.Let_syntax in
    let idx_name (c : Sql.Col_name.t) =
      let%map tname = c.tname in
      sprintf "%s_by_%s" tname c.cname
    in
    List.filter_map eq ~f:(fun c ->
        let%map name = idx_name c in
        (c, `Hash, name, ref false))
    @ List.filter_map cmp ~f:(fun c ->
          let%map name = idx_name c in
          (c, `Ordered, name, ref false))
  in
  let tables =
    List.filter_map queries ~f:(fun (query, _) ->
        match query with
        | Create c ->
            Some
              ( c.name,
                List.map c.schema ~f:(fun attr ->
                    let kind =
                      if Set.mem attr.extra PrimaryKey then `Primary_key
                      else
                        match
                          Set.find_map attr.extra ~f:(function
                            | ForeignKey tbl -> Some tbl
                            | _ -> None)
                        with
                        | Some tbl -> `Foreign_key tbl
                        | None -> `None
                    in
                    (attr.name, attr.domain, kind)) )
        | _ -> None)
    |> Map.of_alist_exn (module String)
  in
  { tables; indexes }

let is_primary_key_of db col tbl =
  match col_attrs db col with
  | Some { constraints = `Primary_key; _ } ->
      [%equal: string] tbl (Option.value_exn col.tname)
  | _ -> false

let is_foreign_key_of db col tbl =
  match col_attrs db col with
  | Some { constraints = `Foreign_key tbl'; _ } -> [%equal: string] tbl tbl'
  | _ -> false

let foreign_key db col =
  match col_attrs db col with
  | Some { constraints = `Foreign_key t; _ } -> Some t
  | _ -> None

let is_foreign_key db col =
  match col_attrs db col with
  | Some { constraints = `Foreign_key _; _ } -> true
  | _ -> false

let foreign_key_table db col =
  col_attrs db col
  |> Option.bind ~f:(function
       | { Db.Schema.constraints = `Foreign_key tbl; _ } -> Some tbl
       | _ -> None)

let referrers db t =
  Db.Schema.relation_names db
  |> List.filter ~f:(fun table ->
         Db.Schema.attrs db table
         |> List.exists ~f:(fun (attr : Db.Schema.attr) ->
                match attr.constraints with
                | `Foreign_key t'' -> [%equal: string] t t''
                | _ -> false))
  |> List.dedup_and_sort ~compare:[%compare: string]
