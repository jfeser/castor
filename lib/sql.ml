open Base
open Printf
open Collections
open Abslayout

let rec pred_to_sql = function
  | As_pred (p, n) -> sprintf "%s as %s" (pred_to_sql p) n
  | Name n -> sprintf "%s" (Name.to_sql n)
  | Int x -> Int.to_string x
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> sprintf "'%s'" s
  | Null -> "null"
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
      | Mod -> sprintf "%s %% %s" s1 s2 )
  | Count -> "count(*)"
  | Sum n -> sprintf "sum(%s)" (pred_to_sql n)
  | Avg n -> sprintf "avg(%s)" (pred_to_sql n)
  | Min n -> sprintf "min(%s)" (pred_to_sql n)
  | Max n -> sprintf "max(%s)" (pred_to_sql n)
  | If (p1, p2, p3) ->
      sprintf "case when %s then %s else %s end" (pred_to_sql p1) (pred_to_sql p2)
        (pred_to_sql p3)

type query = {schema: Name.t list; sql: [`Subquery of string | `Scan of string]}

let to_subquery fresh {sql; schema} =
  let alias = Fresh.name fresh "t%d" in
  match sql with
  | `Subquery q ->
      ( sprintf "(%s) as %s" q alias
      , List.map schema ~f:(fun n -> {n with relation= Some alias}) )
  | `Scan tbl -> (tbl, schema)

let to_query = function
  | `Scan tbl -> sprintf "select * from %s" tbl
  | `Subquery q -> q

let ralgebra_to_sql_helper ~fresh r =
  let rec f ({node; _} as r) =
    match node with
    | Scan tbl -> {sql= `Scan tbl; schema= Meta.(find_exn r schema)}
    | Select ([], _) -> failwith "No empty selects."
    | Select (fs, r) ->
        let sql, schema = to_subquery fresh (f r) in
        let fields =
          let ctx =
            List.zip_exn Meta.(find_exn r schema) schema
            |> List.map ~f:(fun (n, n') -> (n, Name n'))
            |> Map.of_alist_exn (module Name.Compare_no_type)
          in
          List.map fs ~f:(subst_pred ctx)
        in
        let fields_str =
          fields |> List.map ~f:pred_to_sql |> String.concat ~sep:", "
        in
        let new_schema = List.map fields ~f:pred_to_schema in
        let new_query = sprintf "select %s from %s" fields_str sql in
        {sql= `Subquery new_query; schema= new_schema}
    | Filter (pred, r) ->
        let sql, schema = to_subquery fresh (f r) in
        let pred =
          let ctx =
            List.zip_exn Meta.(find_exn r schema) schema
            |> List.map ~f:(fun (n, n') -> (n, Name n'))
            |> Map.of_alist_exn (module Name.Compare_no_type)
          in
          subst_pred ctx pred |> pred_to_sql
        in
        let new_query = sprintf "select * from %s where %s" sql pred in
        {sql= `Subquery new_query; schema}
    | Join {pred; r1; r2} -> (
      match (f r1, f r2) with {sql= sql1; schema= s1}, {sql= sql2; schema= s2} ->
        let q1 =
          match sql1 with `Subquery q -> sprintf "(%s)" q | `Scan tbl -> tbl
        in
        let q2 =
          match sql2 with `Subquery q -> sprintf "(%s)" q | `Scan tbl -> tbl
        in
        let a1 = Fresh.name fresh "t%d" in
        let a2 = Fresh.name fresh "t%d" in
        let new_s1 = List.map s1 ~f:(fun n -> Name.create ~relation:a1 n.name) in
        let new_s2 = List.map s2 ~f:(fun n -> Name.create ~relation:a2 n.name) in
        let new_schema = new_s1 @ new_s2 in
        let join_schema = Meta.(find_exn r1 schema) @ Meta.(find_exn r2 schema) in
        if List.length new_schema <> List.length join_schema then
          Error.create "Bug: schema mismatch" (new_schema, join_schema)
            [%sexp_of: Name.t list * Name.t list]
          |> Error.raise ;
        let pred =
          let ctx =
            List.zip_exn
              (Meta.(find_exn r1 schema) @ Meta.(find_exn r2 schema))
              (new_s1 @ new_s2)
            |> List.map ~f:(fun (n, n') -> (n, Name n'))
            |> Map.of_alist_exn (module Name.Compare_no_type)
          in
          subst_pred ctx pred |> pred_to_sql
        in
        let new_query =
          sprintf "select * from %s as %s, %s as %s where %s" q1 a1 q2 a2 pred
        in
        {sql= `Subquery new_query; schema= new_schema} )
    | Dedup r ->
        let sql, schema = to_subquery fresh (f r) in
        let query = sprintf "select distinct * from %s" sql in
        {sql= `Subquery query; schema}
    | As (_, r) -> f r
    | OrderBy {key; order; rel= r} ->
        let sql, schema = to_subquery fresh (f r) in
        let ctx =
          List.zip_exn Meta.(find_exn r schema) schema
          |> List.map ~f:(fun (n, n') -> (n, Name n'))
          |> Map.of_alist_exn (module Name.Compare_no_type)
        in
        let sql_order = match order with `Asc -> "asc" | `Desc -> "desc" in
        let sql_key =
          List.map key ~f:(fun p -> subst_pred ctx p |> pred_to_sql)
          |> String.concat ~sep:", "
        in
        let new_query =
          sprintf "select * from %s order by (%s) %s" sql sql_key sql_order
        in
        {sql= `Subquery new_query; schema}
    | GroupBy (_, _, _) -> Error.of_string "Unsupported." |> Error.raise
    | AEmpty | AScalar _ | AList _ | ATuple _ | AHashIdx _ | AOrderedIdx _ ->
        Error.of_string "Only relational algebra constructs allowed." |> Error.raise
  in
  f r

let ralgebra_to_sql r =
  to_query (ralgebra_to_sql_helper ~fresh:(Fresh.create ()) r).sql

let ralgebra_foreach q1 q2 =
  let fresh = Fresh.create () in
  let sql1, s1 = to_subquery fresh (ralgebra_to_sql_helper ~fresh q1) in
  let q2 =
    let ctx =
      List.zip_exn s1 Meta.(find_exn q1 schema)
      |> List.map ~f:(fun (n, n') -> (n, Name n'))
      |> Map.of_alist_exn (module Name.Compare_no_type)
    in
    subst ctx q2
  in
  let sql2, s2 = to_subquery fresh (ralgebra_to_sql_helper ~fresh q2) in
  let q1_fields = List.map s1 ~f:Name.to_sql in
  let q2_fields = List.map s2 ~f:Name.to_sql in
  let fields_str = String.concat ~sep:", " (q1_fields @ q2_fields) in
  let q1_fields_str =
    List.init (List.length q1_fields) ~f:(fun i -> sprintf "%d" (i + 1))
    |> String.concat ~sep:", "
  in
  sprintf "select %s from %s, lateral %s order by (%s)" fields_str sql1 sql2
    q1_fields_str