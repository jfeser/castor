open Core
open Castor
open Abslayout

type castor_binop =
  [ `Add
  | `And
  | `Div
  | `Eq
  | `Ge
  | `Gt
  | `Le
  | `Lt
  | `Mod
  | `Mul
  | `Or
  | `Sub ]

type castor_unop = [ `Not ]

let conv_binop = function
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

let conv_unop = function `Not -> Not

let rec conv_stmt s =
  let module Sql = Sqlgg.Sql in
  if Option.is_some s.Sql.limit then
    Log.warn (fun m -> m "Limit clauses not supported. Dropping.");
  let rec conv_order o q =
    if o = [] then q
    else
      let order =
        List.map o ~f:(fun (e, d) ->
            let dir =
              match d with
              | Some `Asc -> Asc
              | Some `Desc -> Desc
              | None -> Asc
            in
            (conv_expr e, dir))
      in
      order_by order q
  and conv_filter f q =
    match f with Some e -> filter (conv_expr e) q | None -> q
  and conv_source (s, _) =
    match s with
    | `Select s -> conv_stmt s
    | `Table t -> relation { r_name = t; r_schema = None }
    | `Nested n -> conv_nested n
  and conv_nested ((q, qs) : _ Sql.nested) =
    let open Pred in
    match qs with
    | [] -> conv_source q
    | (q', j) :: qs' -> (
        match j with
        | `Cross | `Default ->
            join (bool true) (conv_source q) (conv_nested (q', qs'))
        | `Search e ->
            join (conv_expr e) (conv_source q) (conv_nested (q', qs'))
        | `Using _ | `Natural -> failwith "Join type not supported" )
  and conv_expr e =
    match e with
    | Sql.Value v -> (
        match v with
        | Int x -> Int x
        | Date s -> Date (Date.of_string s)
        | String s -> String s
        | Bool x -> Bool x
        | Float x -> Fixed (Fixed_point.of_float x)
        | Null -> Null None )
    | Param _ | Choices (_, _) | Inserted _ | Sequence _ ->
        failwith "unsupported"
    | Fun (op, args) -> (
        let open Pred in
        match (op, args) with
        | `In, [ x; Sequence vs ] ->
            let x = conv_expr x in
            let rec to_pred = function
              | [] -> Bool false
              | [ v ] -> binop (Eq, x, conv_expr v)
              | v :: vs -> binop (Or, binop (Eq, x, conv_expr v), to_pred vs)
            in
            to_pred vs
        | `In, [ x; Select (s, _) ] ->
            let q = conv_stmt s in
            let f = Schema.schema q |> List.hd_exn in
            Exists (filter (binop (Eq, conv_expr x, Name f)) q)
        | (#castor_binop as op), [ e; e' ] ->
            binop (conv_binop op, conv_expr e, conv_expr e')
        | #castor_binop, _ -> failwith "Expected two arguments"
        | `Neq, [ e; e' ] -> unop (Not, binop (Eq, conv_expr e, conv_expr e'))
        | (#castor_unop as op), [ e ] -> unop (conv_unop op, conv_expr e)
        | (`Neq | #castor_unop), _ -> failwith "Expected one argument"
        | `Count, _ -> Count
        | `Min, [ e ] -> Min (conv_expr e)
        | `Max, [ e ] -> Max (conv_expr e)
        | `Avg, [ e ] -> Avg (conv_expr e)
        | `Sum, [ e ] -> Sum (conv_expr e)
        | (`Min | `Max | `Avg | `Sum), _ ->
            failwith "Unexpected aggregate arguments"
        | `Between, [ e1; e2; e3 ] ->
            let e2 = conv_expr e2 in
            binop
              (And, binop (Le, conv_expr e1, e2), binop (Le, e2, conv_expr e3))
        | #Sql.bit_op, _ -> failwith "Bit ops not supported"
        | op, _ ->
            Error.create "Unsupported op" op [%sexp_of: Sql.op] |> Error.raise
        )
    | Select (s, `Exists) -> Exists (conv_stmt s)
    | Select (s, `AsValue) -> First (conv_stmt s)
    | Column { cname; _ } -> Name (Name.create cname)
  and conv_select (s, ss) =
    if ss <> [] then Log.warn (fun m -> m "Unexpected nonempty select list.");
    let select_list =
      List.filter_map s.Sql.columns ~f:(function
        | All | AllOf _ ->
            Log.warn (fun m -> m "All and AllOf unsupported.");
            None
        | Expr (e, None) -> Some (conv_expr e)
        | Expr (e, Some a) -> Some (As_pred (conv_expr e, a)))
    in
    let from =
      match s.from with
      | Some from -> conv_filter s.where (conv_nested from)
      | None -> scalar (Pred.int 0)
    in
    if s.group = [] then select select_list from
    else
      let group_key =
        List.map s.group ~f:(function
          | Sql.Column { cname; _ } -> Name.create cname
          | _ -> failwith "Unexpected grouping key.")
      in
      conv_filter s.where (group_by select_list group_key from)
  in
  conv_order s.Sql.order (conv_select s.select)

let () =
  let fn = Sys.argv.(1) in
  try
    In_channel.with_file fn ~f:(fun ch ->
        Sqlgg.Parser.get_statements ch
        |> Sequence.iter ~f:(fun stmt ->
               match stmt with
               | Sqlgg.Sql.Select s -> (
                   try
                     conv_stmt s |> ignore;
                     printf "Loading %s succeeded.\n" fn
                   with exn ->
                     printf "Converting %s failed: %s\n" fn (Exn.to_string exn)
                   )
               | _ -> failwith ""))
  with Sqlgg.Parser.Error (_, (x, y, tok, _)) ->
    printf "Parsing %s failed. Unexpected token %s (%d, %d).\n" fn tok x y
