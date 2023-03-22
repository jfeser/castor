open Core
open Ast
module P = Pred.Infix
module V = Visitors

let conv_binop : _ -> Pred.Binop.t =
  let open Pred.Binop in
  function
  | `And -> And
  | `Or -> Or
  | `Add -> Add
  | `Sub -> Sub
  | `Mul -> Mul
  | `Div -> Div
  | `Mod -> Mod
  | `Eq -> Eq
  | `Lt -> Lt
  | `Le -> Le
  | `Gt -> Gt
  | `Ge -> Ge

let conv_unop : _ -> Pred.Unop.t =
  let open Pred.Unop in
  function `Not -> Not | `Day -> Day | `Year -> Year

let conv_alias = sprintf "%s_%s"

let conv_sql db_schema =
  let open Sqlgg in
  let open Constructors.Annot in
  let aliases = ref (Set.empty (module String)) in
  let alias_of_name = ref (Map.empty (module String)) in

  let conv_order _ q =
    Log.warn (fun m -> m "Dropping orderby clause.");
    q
  in
  let conv_limit l q =
    if Option.is_some l then Log.warn (fun m -> m "Dropping limit clause.");
    q
  in
  let conv_distinct d q = if d then dedup q else q in

  let rec conv_filter f q =
    match f with Some e -> filter (conv_expr e) q | None -> q
  and conv_source (s, alias) =
    let q =
      match s with
      | `Subquery s -> conv_query s
      | `Table t -> relation (Db.Schema.relation db_schema t)
      | `Nested n -> conv_nested n
    in
    match alias with
    | Some a ->
        aliases := Set.add !aliases a;
        alias_of_name :=
          List.fold_left (Schema.schema q) ~init:!alias_of_name ~f:(fun m n ->
              Map.set m ~key:(Name.name n) ~data:a);
        let select_list =
          Schema.schema q
          |> List.map ~f:(fun n -> P.(name n, sprintf "%s_%s" a (Name.name n)))
        in
        select select_list q
    | None -> q
  and conv_nested (q, qs) =
    match qs with
    | [] -> conv_source q
    | (q', j) :: qs' -> (
        match j with
        | `Cross | `Default ->
            join (bool true) (conv_source q) (conv_nested (q', qs'))
        | `Search e ->
            join (conv_expr e) (conv_source q) (conv_nested (q', qs'))
        | `Using _ | `Natural -> failwith "Join type not supported")
  and conv_subquery q = conv_query q
  and conv_column (c : Sql.Col_name.t) =
    let n =
      match c.tname with
      | Some a -> if Set.mem !aliases a then conv_alias a c.cname else c.cname
      | None -> (
          match Map.find !alias_of_name c.cname with
          | Some a -> conv_alias a c.cname
          | None -> c.cname)
    in
    Name.create n
  and conv_expr e =
    let open Pred in
    match e with
    | Sql.Value v -> (
        match v with
        | Int x -> `Int x
        | Date s -> `Date (Date.of_string s)
        | String s -> `String s
        | Bool x -> `Bool x
        | Float x -> `Fixed (Fixed_point.of_float x)
        | Null -> `Null None)
    | Param _ | Choices (_, _) | Inserted _ | Sequence _ ->
        failwith "unsupported"
    | Case (branches, else_) ->
        let else_ =
          Option.map else_ ~f:conv_expr |> Option.value ~default:(`Null None)
        in
        let rec to_pred = function
          | [] -> failwith "Empty case"
          | [ (p, x) ] -> `If (conv_expr p, conv_expr x, else_)
          | (p, x) :: bs -> `If (conv_expr p, conv_expr x, to_pred bs)
        in
        to_pred branches
    | Fun (op, args) -> (
        match (op, args) with
        | `In, [ x; Sequence vs ] ->
            let x = conv_expr x in
            let rec to_pred = function
              | [] -> `Bool false
              | [ v ] -> Infix.(x = conv_expr v)
              | v :: vs -> Infix.(x = conv_expr v || to_pred vs)
            in
            to_pred vs
        | `IsNull, [ x ] -> Infix.(conv_expr x = null None)
        | `In, [ x; Subquery (s, _) ] ->
            let q = conv_subquery s in
            let f = Schema.schema q |> List.hd_exn in
            exists @@ filter Infix.(conv_expr x = name f) q
        | ( (( `Add | `And | `Div | `Eq | `Ge | `Gt | `Le | `Lt | `Mod | `Mul
             | `Or | `Sub ) as op),
            [ e; e' ] ) ->
            binop (conv_binop op, conv_expr e, conv_expr e')
        | `Neq, [ e; e' ] -> Infix.(not (conv_expr e = conv_expr e'))
        | ((`Not | `Day | `Year) as op), [ e ] ->
            unop (conv_unop op, conv_expr e)
        | `Count, _ -> `Count
        | `Min, [ e ] -> `Min (conv_expr e)
        | `Max, [ e ] -> `Max (conv_expr e)
        | `Avg, [ e ] -> `Avg (conv_expr e)
        | `Sum, [ e ] -> `Sum (conv_expr e)
        | `Substring, [ e1; e2; e3 ] ->
            `Substring (conv_expr e1, conv_expr e2, conv_expr e3)
        | (`Min | `Max | `Avg | `Sum), _ ->
            failwith "Unexpected aggregate arguments"
        | `Between, [ e1; e2; e3 ] ->
            let e2 = conv_expr e2 in
            Infix.(conv_expr e1 <= e2 && e2 <= conv_expr e3)
        | #Sql.bit_op, _ -> failwith "Bit ops not supported"
        | op, _ ->
            Error.create "Unsupported op" op [%sexp_of: Sql.op] |> Error.raise)
    | Subquery (s, `Exists) -> `Exists (conv_subquery s)
    | Subquery (s, `AsValue) -> `First (conv_subquery s)
    | Column c -> `Name (conv_column c)
  and conv_select s =
    (* Build query in order. First, FROM *)
    let query =
      match s.Sql.from with
      | Some from -> conv_nested from
      | None -> scalar (P.int 0) "x"
    in
    (* WHERE *)
    let query = conv_filter s.where query in
    (* GROUP_BY & SELECT *)
    let select_list =
      List.filter_map s.Sql.columns ~f:(function
        | All | AllOf _ ->
            Log.warn (fun m -> m "All and AllOf unsupported.");
            None
        | Expr (e, None) -> Some (conv_expr e, "x")
        | Expr (e, Some a) -> Some (conv_expr e, a))
    in
    let query =
      if List.is_empty s.group then select select_list query
      else
        let group_key =
          List.map s.group ~f:(function
            | Sql.Column c -> conv_column c
            | _ -> failwith "Unexpected grouping key.")
        in
        group_by select_list group_key query
    in
    (* HAVING *)
    let query = conv_filter s.having query in
    (* DISTINCT *)
    let query = conv_distinct s.distinct query in
    query
  and conv_clauses (Sql.Clause (s, ss)) =
    let q = conv_select s in
    match ss with
    | None -> q
    | Some (op, c) -> (
        match op with
        | `UnionAll -> tuple [ q; conv_clauses c ] Concat
        | `Union -> dedup (tuple [ q; conv_clauses c ] Concat)
        | `Intersect | `Except -> failwith "Unsupported compound op.")
  and conv_query q =
    conv_clauses q.Sql.clauses |> conv_order q.Sql.order
    |> conv_limit q.Sql.limit
  in
  conv_query
