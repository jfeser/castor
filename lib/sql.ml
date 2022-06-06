open Core
open Collections
open Ast
open Schema
module V = Visitors
module A = Constructors.Annot
module P = Pred.Infix
include (val Log.make "castor.sql")

type select_entry = { pred : Pred.t; alias : string; cast : Prim_type.t option }
[@@deriving compare, sexp_of]

type 'r spj = {
  select : select_entry list;
  distinct : bool;
  conds : Pred.t list;
  relations : 'r list;
  order : (Pred.t * order) list;
  group : Pred.t list;
  limit : int option;
}
[@@deriving compare, sexp_of]

type 'q compound_relation =
  [ `Subquery of 'q | `Table of Relation.t | `Series of Pred.t * Pred.t ]
  * string
  * [ `Left | `Lateral ]
[@@deriving compare, sexp_of]

type t =
  [ `Query of t compound_relation spj
  | `Union_all of t compound_relation spj list ]
[@@deriving compare, sexp_of]

let create_spj ?(distinct = false) ?(conds = []) ?(relations = []) ?(order = [])
    ?(group = []) ?limit select =
  { select; distinct; conds; relations; order; group; limit }

let create_entry ?alias ?cast pred =
  match (alias, pred) with
  | Some a, _ -> { pred; alias = a; cast }
  | None, `Name n -> { pred; alias = Name.name n; cast }
  | _ -> failwith "expected an alias or name"

let create_entry_n ?alias ?cast name = create_entry ?alias ?cast (`Name name)

let create_entry_s ?alias ?cast name =
  create_entry_n ?alias ?cast (Name.create name)

let create_entries_s names = List.map names ~f:create_entry_s
let select_entry_name { alias; _ } = alias

let to_schema = function
  | `Query { select; _ } | `Union_all ({ select; _ } :: _) ->
      List.map select ~f:select_entry_name
  | `Union_all [] -> failwith "No empty unions."

let to_select = function
  | `Union_all qs -> List.concat_map qs ~f:(fun { select; _ } -> select)
  | `Query { select; _ } -> select

let to_group = function `Union_all _ -> [] | `Query q -> q.group
let to_distinct = function `Union_all _ -> false | `Query q -> q.distinct
let to_limit = function `Union_all _ -> None | `Query q -> q.limit

let preds_has_aggregates =
  Iter.exists (fun p ->
      match Pred.kind p with `Agg -> true | `Window | `Scalar -> false)

let select_has_aggregates s =
  Iter.of_list s |> Iter.map (fun { pred = p; _ } -> p) |> preds_has_aggregates

let has_aggregates sql = to_select sql |> select_has_aggregates

let preds_has_windows =
  Iter.exists (fun p ->
      match Pred.kind p with `Window -> true | `Agg | `Scalar -> false)

let select_has_windows s =
  Iter.of_list s |> Iter.map (fun { pred = p; _ } -> p) |> preds_has_windows

let has_windows sql = to_select sql |> select_has_windows

(** Convert a query to an SPJ by introducing a subquery. *)
let create_subquery q =
  let alias = Fresh.name Global.fresh "t%d" in
  let select_list =
    List.map (to_schema q) ~f:(fun n ->
        create_entry_n { type_ = None; name = Attr (alias, n) })
  in
  create_spj ~relations:[ (`Subquery q, alias, `Left) ] select_list

let to_spj = function `Query q -> q | `Union_all _ as q -> create_subquery q

let make_ctx schema select =
  let pairs, _ = List.zip_with_remainder schema select in
  List.fold pairs
    ~init:(Map.empty (module Name))
    ~f:(fun m (n, { pred = p; _ }) ->
      if Map.mem m n then
        warn (fun m -> m "Duplicate name in schema %a." Name.pp n);
      Map.set m ~key:n ~data:p)

let join schema1 schema2 sql1 sql2 pred =
  let needs_subquery =
    to_distinct sql1 || to_distinct sql2
    || List.length (to_group sql1) > 0
    || List.length (to_group sql2) > 0
    || Option.is_some (to_limit sql1)
    || Option.is_some (to_limit sql2)
    || has_aggregates sql1 || has_aggregates sql2 || has_windows sql1
    || has_windows sql2
  in
  let spj1 = if needs_subquery then create_subquery sql1 else to_spj sql1 in
  let spj2 = if needs_subquery then create_subquery sql2 else to_spj sql2 in

  let dup_subst =
    List.map (spj1.relations @ spj2.relations) ~f:(fun (_, a, _) -> a)
    |> List.find_all_dups ~compare:[%compare: string]
    |> List.map ~f:(fun r -> (r, sprintf "%s_%d" r (Fresh.int Global.fresh)))
    |> Map.of_alist_exn (module String)
  in

  let rec subst_rel ctx = function
    | `Name (Name.{ name = Attr (rel, attr); _ } as n) as this -> (
        match Map.find ctx rel with
        | Some rel' -> `Name { n with name = Attr (rel', attr) }
        | None -> this)
    | p -> V.Map.pred (subst_rel_annot ctx) (subst_rel ctx) p
  and subst_rel_annot ctx r =
    V.Map.annot (V.Map.query (subst_rel_annot ctx) (subst_rel ctx)) r
  in

  let spj2 =
    {
      spj2 with
      conds = List.map spj2.conds ~f:(subst_rel dup_subst);
      group = List.map spj2.group ~f:(subst_rel dup_subst);
      order = List.map spj2.order ~f:(fun (p, o) -> (subst_rel dup_subst p, o));
      relations =
        List.map spj2.relations ~f:(fun (r, a, j) ->
            let a' = Map.find dup_subst a |> Option.value ~default:a in
            (r, a', j));
      select =
        List.map spj2.select ~f:(fun e ->
            { e with pred = subst_rel dup_subst e.pred });
    }
  in

  (* The where clause is evaluated before the select clause so we can't use the
     aliases in the select clause here. *)
  let select_list = spj1.select @ spj2.select in
  let ctx = make_ctx (schema1 @ schema2) select_list in
  let pred = Pred.subst ctx pred in
  `Query
    (create_spj
       ~conds:(pred :: (spj1.conds @ spj2.conds))
       ~relations:(spj1.relations @ spj2.relations)
       select_list)

let order_by of_ralgebra key r =
  let spj = to_spj (of_ralgebra r) in
  let key =
    let ctx = make_ctx (schema r) spj.select in
    let preds = List.map key ~f:(fun (p, o) -> (Pred.subst ctx p, o)) in
    (* Check that all predicates are scalar *)
    let non_scalar =
      List.find preds ~f:(fun (p, _) -> Poly.(Pred.kind p <> `Scalar))
    in
    Option.iter non_scalar ~f:(fun (p, _) ->
        err (fun m -> m "Non-scalar predicate in order-by: %a" Pred.pp p));
    preds
  in
  (* Remove constant integer predicates (sql interprets these specially). *)
  let key =
    List.filter key ~f:(fun (p, _) ->
        match p with `Int _ -> false | _ -> true)
  in
  `Query { spj with order = key }

let scoped_names ns p =
  let visitor =
    object (self)
      inherit [_] V.reduce
      inherit [_] Util.set_monoid (module Name)

      method! visit_Name () n =
        if Set.mem ns n then Set.empty (module Name)
        else Set.singleton (module Name) n

      method! visit_Exists () _ = self#zero
      method! visit_First () _ = self#zero
    end
  in
  visitor#visit_pred () p

let select ?groupby of_ralgebra ps r =
  let sql = of_ralgebra r in
  let preds = Select_list.preds ps in
  let has_aggs = preds_has_aggregates preds in
  let spj =
    let needs_subquery =
      (* Creating a subquery is always safe, but we want to avoid it if
         possible.

         We conservatively create a subquery whenever this selection list has
         aggregates.

         `If the inner query is distinct then it must be wrapped. *)
      has_aggs || preds_has_windows preds || to_distinct sql
    in
    if needs_subquery then create_subquery sql else to_spj sql
  in
  (* `If this select has aggregates and uses names from a containing scope, then
     those names must be selected first by the subquery. `If there are extra
     fields to select, add them then wrap them in a subquery. *)
  let spj =
    (* Names that must have come from an outer scope because they are not in the
       schema of the child. *)
    let scoped =
      List.map ps ~f:(fun (p, _) ->
          scoped_names (schema r |> Set.of_list (module Name)) p)
      |> Set.union_list (module Name)
    in
    if has_aggs && not (Set.is_empty scoped) then
      let extra_fields = Set.to_list scoped |> List.map ~f:create_entry_n in
      create_subquery (`Query { spj with select = spj.select @ extra_fields })
    else spj
  in
  let sctx = make_ctx (schema r) spj.select in
  let fields =
    List.map ps ~f:(fun (p, n) -> create_entry (Pred.subst sctx p) ~alias:n)
  in
  let spj = { spj with select = fields } in
  let spj =
    match groupby with
    | Some key ->
        let group = List.map key ~f:(Pred.subst sctx) in
        { spj with group }
    | None -> spj
  in
  `Query spj

let filter of_ralgebra pred r =
  let sql = of_ralgebra r in
  (* `If the inner query
     contains aggregates, then it must be put in a subquery. *)
  let needs_subquery = has_aggregates sql || has_windows sql in
  let spj = if needs_subquery then create_subquery sql else to_spj sql in
  (* The where clause is evaluated before the select clause so we can't use the
     aliases in the select clause here. *)
  let pred =
    let ctx = make_ctx (schema r) spj.select in
    Pred.subst ctx pred
  in
  `Query { spj with conds = pred :: spj.conds }

let dep_join of_ralgebra q1 q2 =
  let sql1 = `Query { (to_spj (of_ralgebra q1)) with order = [] } in
  let sql1_names = to_schema sql1 in
  let sql1_alias = Fresh.name Global.fresh "t%d" in
  let q2 =
    (* Create a context that maps the names emitted by q1 (scoped) to the
       names emitted by the q1 sql. *)
    let ctx =
      List.zip_exn (List.map (schema q1) ~f:Name.zero) sql1_names
      |> List.map ~f:(fun (n, n') ->
             ( n,
               P.name
                 { Name.type_ = Name.type_ n; name = Attr (sql1_alias, n') } ))
      |> Map.of_alist_exn (module Name)
    in
    Abslayout.subst ctx q2
  in
  let sql2 = `Query { (of_ralgebra q2 |> to_spj) with order = [] } in
  let sql2_names = to_schema sql2 in
  let sql2_alias = Fresh.name Global.fresh "t%d" in
  let select_list =
    List.map sql2_names ~f:(fun n ->
        create_entry_n { type_ = None; name = Attr (sql2_alias, n) })
  in
  `Query
    (create_spj
       ~relations:
         [
           (`Subquery sql1, sql1_alias, `Left);
           (`Subquery sql2, sql2_alias, `Lateral);
         ]
       select_list)

let relation Relation.({ r_name = tbl; _ } as rel) =
  (* Add table alias to all fields to generate a select list. *)
  let select_list =
    List.map (Option.value_exn rel.r_schema) ~f:(fun (n, t) ->
        create_entry_n { type_ = Some t; name = Attr (tbl, n) })
  in
  let relations = [ (`Table rel, tbl, `Left) ] in
  `Query (create_spj ~relations select_list)

let of_ralgebra_open f r =
  match r.node with
  | Range (p, p') ->
      let alias = Fresh.name Global.fresh "t%d" in
      `Query
        (create_spj
           ~relations:[ (`Series (p, p'), alias, `Left) ]
           [ create_entry_s "range" ])
  | Dedup r -> `Query { (to_spj (f r)) with distinct = true }
  | Relation r -> relation r
  | Filter (pred, r) -> filter f pred r
  | OrderBy { key; rel = r } -> order_by f key r
  | Select (fs, r) -> select f fs r
  | DepJoin { d_lhs; d_rhs } -> dep_join f d_lhs d_rhs
  | Join { pred; r1; r2 } -> join (schema r1) (schema r2) (f r1) (f r2) pred
  | GroupBy (ps, key, r) ->
      let key = List.map ~f:P.name key in
      select ~groupby:key f ps r
  | ATuple ([ r ], (Cross | Concat)) -> f r
  | ATuple (r :: rs, Cross) ->
      let q1 = r in
      let q2 = A.tuple rs Cross in
      join (schema q1) (schema q2) (f q1) (f q2) (`Bool true)
  | ATuple (rs, Concat) -> `Union_all (List.map ~f:(fun r -> to_spj (f r)) rs)
  | ATuple ([], _) | AEmpty -> `Query (create_spj ~limit:0 [])
  | AScalar p -> `Query (create_spj [ create_entry p.s_pred ~alias:p.s_name ])
  | ATuple (_, Zip) ->
      Error.(create "Unsupported." r [%sexp_of: _ annot] |> raise)
  | AList l -> f @@ A.dep_join' (Layout_to_depjoin.list l)
  | AHashIdx h -> f @@ A.dep_join' (Layout_to_depjoin.hash_idx h)
  | AOrderedIdx o -> f @@ A.dep_join' (Layout_to_depjoin.ordered_idx o)
  | _ -> failwith "unsupported"

let of_ralgebra x =
  let rec f x = of_ralgebra_open f x in
  f (strip_meta x)

let name_to_sql (n : Name.t) =
  match n.name with
  | Simple n -> Fmt.str "\"%s\"" n
  | Attr (r, x) -> Fmt.str "\"%s\".\"%s\"" r x
  | Bound _ -> failwith "unexpected bound name"

let rec pred_to_sql p =
  let p2s = pred_to_sql in
  match p with
  | `Name n -> sprintf "%s" (name_to_sql n)
  | `Int x -> Int.to_string x
  | `Fixed x -> Fixed_point.to_string x
  | `Date x -> sprintf "date('%s')" (Date.to_string x)
  | `Bool true -> "true"
  | `Bool false -> "false"
  | `String s -> sprintf "'%s'" s
  | `Null None -> "null"
  | `Null (Some t) -> sprintf "(null::%s)" (Prim_type.to_sql t)
  | `Unop (op, p) -> (
      let s = sprintf "(%s)" (p2s p) in
      match op with
      | Unop.Not -> sprintf "not (%s)" s
      | Year -> sprintf "%s * interval '1 year'" s
      | Month -> sprintf "%s * interval '1 month'" s
      | Day -> sprintf "%s * interval '1 day'" s
      | Strlen -> sprintf "char_length(%s)" s
      | ExtractY -> sprintf "cast(date_part('year', %s) as integer)" s
      | ExtractM -> sprintf "cast(date_part('month', %s) as integer)" s
      | ExtractD -> sprintf "cast(date_part('day', %s) as integer)" s)
  | `Binop (Binop.Add, p, `Unop (Unop.Month, p'))
    when !Global.enable_redshift_dates ->
      sprintf "dateadd(month, %s, %s)" (p2s p') (p2s p)
  | `Binop (op, p1, p2) -> (
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
      | Strpos -> sprintf "strpos(%s, %s)" s1 s2)
  | `If (p1, p2, p3) ->
      sprintf "case when %s then %s else %s end" (p2s p1) (p2s p2) (p2s p3)
  | `Exists r ->
      let sql = of_ralgebra r |> to_string in
      sprintf "exists (%s)" sql
  | `First r ->
      let sql = of_ralgebra r |> to_string in
      sprintf "(%s)" sql
  | `Substring (p1, p2, p3) ->
      sprintf "substring(%s from %s for %s)" (p2s p1) (p2s p2) (p2s p3)
  | `Count -> "count(*)"
  | `Sum n -> sprintf "sum(%s)" (p2s n)
  | `Avg n -> sprintf "avg(%s)" (p2s n)
  | `Min n -> sprintf "min(%s)" (p2s n)
  | `Max n -> sprintf "max(%s)" (p2s n)
  | `Row_number -> "row_number() over ()"

and spj_to_sql { select; distinct; order; group; relations; conds; limit } =
  let select =
    (* `If there is no grouping key and there are aggregates in the select list,
       then we need to deal with any non-aggregates in the select. *)
    if List.is_empty group && select_has_aggregates select then
      List.map select ~f:(fun ({ pred = p; _ } as entry) ->
          let pred = match Pred.kind p with `Scalar -> `Min p | _ -> p in
          { entry with pred })
    else select
  in
  let select_sql =
    let select_list =
      List.map select ~f:(fun { pred = p; alias = n; cast = t } ->
          let alias_sql =
            match p with
            | `Name n' when Name.O.(Name.create n = n') -> ""
            | _ -> sprintf "as \"%s\"" n
          in
          match Option.map t ~f:Prim_type.to_sql with
          | Some t_sql -> sprintf "%s::%s %s" (pred_to_sql p) t_sql alias_sql
          | None -> sprintf "%s %s" (pred_to_sql p) alias_sql)
      |> String.concat ~sep:", "
    in
    let distinct_sql = if distinct then "distinct" else "" in
    sprintf "select %s %s" distinct_sql select_list
  in
  let relation_sql =
    if List.is_empty relations then ""
    else
      List.map relations ~f:(fun (rel, alias, join_type) ->
          let rel_str =
            match rel with
            | `Subquery q -> sprintf "(%s) as \"%s\"" (to_string q) alias
            | `Table (t : Relation.t) ->
                if String.(t.r_name = alias) then sprintf "\"%s\"" t.r_name
                else sprintf "\"%s\" as \"%s\"" t.r_name alias
            | `Series (p, p') -> (
                let t = Prim_type.unify (Pred.to_type p) (Pred.to_type p') in
                match t with
                | DateT _ ->
                    sprintf
                      {|
   (select range::date from
   generate_series(%s :: timestamp, %s :: timestamp, '1 day' :: interval) range) as "%s"|}
                      (pred_to_sql p) (pred_to_sql p') alias
                | IntT _ ->
                    sprintf "generate_series(%s, %s) as %s(range)"
                      (pred_to_sql p) (pred_to_sql p') alias
                | _ -> failwith "Unexpected series type.")
          in
          let join_str =
            match join_type with `Left -> "" | `Lateral -> "lateral"
          in
          sprintf "%s %s" join_str rel_str)
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
      let group_keys =
        List.map group ~f:pred_to_sql |> String.concat ~sep:", "
      in
      sprintf "group by %s" group_keys
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
            sprintf "%s %s" (pred_to_sql p) dir_sql)
        |> String.concat ~sep:", "
      in
      sprintf "order by %s" order_keys
  in
  let limit_sql =
    match limit with Some l -> sprintf "limit %d" l | None -> ""
  in
  sprintf "%s %s %s %s %s %s" select_sql relation_sql cond_sql group_sql
    order_sql limit_sql
  |> String.strip

and to_string = function
  | `Query q -> spj_to_sql q
  | `Union_all qs ->
      List.map qs ~f:(fun q -> sprintf "(%s)" (spj_to_sql q))
      |> String.concat ~sep:" union all "

let format sql =
  let proc = Core_unix.open_process "sqlformat -k upper -r -" in
  let stdout, stdin = proc in
  Out_channel.output_string stdin sql;
  Out_channel.close stdin;
  let out = In_channel.input_all stdout in
  Core_unix.close_process proc
  |> Core_unix.Exit_or_signal.or_error |> Or_error.ok_exn;
  String.strip out

let to_string_hum sql = to_string sql |> format

let sample n s =
  sprintf "select * from (%s) as w order by random() limit %d" s n

let trash_sample n s = sprintf "select * from (%s) as w limit %d" s n
