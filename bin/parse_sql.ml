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

type castor_unop = [ `Not | `Day | `Year ]

let load_params fn query =
  let open Yojson.Basic in
  let member_exn ks k' =
    let out =
      List.find_map
        ~f:(fun (k, v) -> if String.(k = k') then Some v else None)
        ks
    in
    Option.value_exn out
  in
  let params =
    from_file fn |> Util.to_list
    |> List.find_map ~f:(fun q ->
           let q = Util.to_assoc q in
           let queries =
             member_exn q "query" |> Util.(convert_each to_string)
           in
           if List.mem queries query ~equal:String.( = ) then
             let params = member_exn q "params" |> Util.to_assoc in
             List.map params ~f:(function
               | p, `String v -> (p, v)
               | _ -> failwith "Unexpected parameter value")
             |> Option.some
           else None)
  in
  params

let unsub ps r =
  let params =
    List.map ps ~f:(fun (k, v) ->
        let name, type_, _ = Util.param_of_string k in
        let v =
          let open Pred in
          match type_ with
          | DateT _ -> Date (Date.of_string v)
          | StringT _ -> String v
          | _ -> of_string_exn v
        in
        (v, Name (Name.create name), ref false))
  in
  let visitor =
    object
      inherit [_] map as super

      method! visit_pred () p =
        let p = super#visit_pred () p in
        match List.find params ~f:(fun (p', _, _) -> Pred.O.(p = p')) with
        | Some (_, p', replaced) ->
            replaced := true;
            p'
        | None -> p
    end
  in
  let r' = visitor#visit_t () r in
  List.iter params ~f:(fun (v, k, replaced) ->
      if not !replaced then
        Log.warn (fun m ->
            m "Param %a not replaced. Could not find %a." Pred.pp k Pred.pp v));
  r'

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

let conv_unop = function `Not -> Not | `Day -> Day | `Year -> Year

let rec conv_stmt s =
  let module Sql = Sqlgg.Sql in
  if Option.is_some s.Sql.limit then
    Log.warn (fun m -> m "Limit clauses not supported. Dropping.");
  let rec conv_order _ q =
    Log.warn (fun m -> m "Dropping orderby clause.");
    q
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
    | Case (branches, else_) ->
        let else_ =
          Option.map else_ ~f:conv_expr |> Option.value ~default:(Null None)
        in
        let rec to_pred = function
          | [] -> failwith "Empty case"
          | [ (p, x) ] -> If (conv_expr p, conv_expr x, else_)
          | (p, x) :: bs -> If (conv_expr p, conv_expr x, to_pred bs)
        in
        to_pred branches
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
        | `IsNull, [ x ] -> binop (Eq, conv_expr x, Null None)
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
        | `Substring, [ e1; e2; e3 ] ->
            Substring (conv_expr e1, conv_expr e2, conv_expr e3)
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

let main params fn =
  Log.info (fun m -> m "Converting %s." fn);
  let params =
    let _, query = String.rsplit2_exn ~on:'/' fn in
    match load_params params query with
    | Some ps -> ps
    | None ->
        Log.warn (fun m -> m "Could not load params for %s" fn);
        []
  in
  let stmts =
    try In_channel.with_file fn ~f:Sqlgg.Parser.get_statements
    with Sqlgg.Parser.Error (_, (x, y, tok, _)) ->
      eprintf "Parsing %s failed. Unexpected token %s (%d, %d).\n" fn tok x y;
      exit 1
  in
  Sequence.iter stmts ~f:(fun stmt ->
      match stmt with
      | Sqlgg.Sql.Select s ->
          let r =
            try conv_stmt s
            with exn ->
              eprintf "Converting %s failed: %s\n" fn (Exn.to_string exn);
              exit 1
          in
          unsub params r |> Format.printf "%a\n@." Abslayout.pp
      | _ -> failwith "")

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ());
  Logs.set_level (Some Logs.Info);
  let open Command in
  let open Let_syntax in
  basic ~summary:"Convert a SQL query to a Castor spec."
    [%map_open
      let () = Log.param
      and params = flag ~doc:" parameter file" "p" (required string)
      and file = anon ("file" %: string) in
      fun () -> main params file]
  |> run
