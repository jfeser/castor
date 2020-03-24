open Core
open Castor
open Ast
open Abslayout
module P = Pred.Infix

type castor_binop =
  [ `Add | `And | `Div | `Eq | `Ge | `Gt | `Le | `Lt | `Mod | `Mul | `Or | `Sub ]

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
  let open Pred in
  let params =
    List.map ps ~f:(fun (k, v) ->
        let name, type_, _ = Util.param_of_string k in
        let v =
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

module Sql = Sqlgg.Sql

class conv_sql db =
  object (self : 'a)
    val mutable aliases = Set.empty (module String)

    val mutable alias_of_name = Map.empty (module String)

    method alias = sprintf "%s_%s"

    method unop =
      let open Pred.Unop in
      function `Not -> Not | `Day -> Day | `Year -> Year

    method binop =
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

    method order _ q =
      Log.warn (fun m -> m "Dropping orderby clause.");
      q

    method limit l q =
      if Option.is_some l then Log.warn (fun m -> m "Dropping limit clause.");
      q

    method distinct d q = if d then dedup q else q

    method filter f q =
      match f with Some e -> filter (self#expr e) q | None -> q

    method source (s, alias) =
      let q =
        match s with
        | `Subquery s -> self#query s
        | `Table t -> relation (Db.relation db t)
        | `Nested n -> self#nested n
      in
      match alias with
      | Some a ->
          aliases <- Set.add aliases a;
          alias_of_name <-
            List.fold_left (Schema.schema q) ~init:alias_of_name ~f:(fun m n ->
                Map.set m ~key:(Name.name n) ~data:a);
          let select_list =
            Schema.schema q
            |> List.map ~f:(fun n ->
                   P.(as_ (name n) (sprintf "%s_%s" a (Name.name n))))
          in
          select select_list q
      | None -> q

    method nested (q, qs) =
      match qs with
      | [] -> self#source q
      | (q', j) :: qs' -> (
          match j with
          | `Cross | `Default ->
              join (bool true) (self#source q) (self#nested (q', qs'))
          | `Search e ->
              join (self#expr e) (self#source q) (self#nested (q', qs'))
          | `Using _ | `Natural -> failwith "Join type not supported" )

    method subquery = self#query

    method column c =
      let n =
        match c.Sql.tname with
        | Some a -> if Set.mem aliases a then self#alias a c.cname else c.cname
        | None -> (
            match Map.find alias_of_name c.cname with
            | Some a -> self#alias a c.cname
            | None -> c.cname )
      in
      Name.create n

    method expr e =
      let open Pred in
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
            Option.map else_ ~f:self#expr |> Option.value ~default:(Null None)
          in
          let rec to_pred = function
            | [] -> failwith "Empty case"
            | [ (p, x) ] -> If (self#expr p, self#expr x, else_)
            | (p, x) :: bs -> If (self#expr p, self#expr x, to_pred bs)
          in
          to_pred branches
      | Fun (op, args) -> (
          match (op, args) with
          | `In, [ x; Sequence vs ] ->
              let x = self#expr x in
              let rec to_pred = function
                | [] -> Bool false
                | [ v ] -> Infix.(x = self#expr v)
                | v :: vs -> Infix.(x = self#expr v || to_pred vs)
              in
              to_pred vs
          | `IsNull, [ x ] -> Infix.(self#expr x = null None)
          | `In, [ x; Subquery (s, _) ] ->
              let q = self#subquery s in
              let f = Schema.schema q |> List.hd_exn in
              exists @@ filter Infix.(self#expr x = name f) q
          | (#castor_binop as op), [ e; e' ] ->
              binop (self#binop op) (self#expr e) (self#expr e')
          | #castor_binop, _ -> failwith "Expected two arguments"
          | `Neq, [ e; e' ] -> Infix.(not (self#expr e = self#expr e'))
          | (#castor_unop as op), [ e ] -> unop (self#unop op) (self#expr e)
          | (`Neq | #castor_unop), _ -> failwith "Expected one argument"
          | `Count, _ -> Count
          | `Min, [ e ] -> Min (self#expr e)
          | `Max, [ e ] -> Max (self#expr e)
          | `Avg, [ e ] -> Avg (self#expr e)
          | `Sum, [ e ] -> Sum (self#expr e)
          | `Substring, [ e1; e2; e3 ] ->
              Substring (self#expr e1, self#expr e2, self#expr e3)
          | (`Min | `Max | `Avg | `Sum), _ ->
              failwith "Unexpected aggregate arguments"
          | `Between, [ e1; e2; e3 ] ->
              let e2 = self#expr e2 in
              Infix.(self#expr e1 <= e2 && e2 <= self#expr e3)
          | #Sql.bit_op, _ -> failwith "Bit ops not supported"
          | op, _ ->
              Error.create "Unsupported op" op [%sexp_of: Sql.op] |> Error.raise
          )
      | Subquery (s, `Exists) -> Exists (self#subquery s)
      | Subquery (s, `AsValue) -> First (self#subquery s)
      | Column c -> Name (self#column c)

    method select s =
      (* Build query in order. First, FROM *)
      let query =
        match s.Sql.from with
        | Some from -> self#nested from
        | None -> scalar (P.int 0)
      in
      (* WHERE *)
      let query = self#filter s.where query in
      (* GROUP_BY & SELECT *)
      let select_list =
        List.filter_map s.Sql.columns ~f:(function
          | All | AllOf _ ->
              Log.warn (fun m -> m "All and AllOf unsupported.");
              None
          | Expr (e, None) -> Some (self#expr e)
          | Expr (e, Some a) -> Some (As_pred (self#expr e, a)))
      in
      let query =
        if List.is_empty s.group then select select_list query
        else
          let group_key =
            List.map s.group ~f:(function
              | Sql.Column c -> self#column c
              | _ -> failwith "Unexpected grouping key.")
          in
          group_by select_list group_key query
      in
      (* HAVING *)
      let query = self#filter s.having query in
      (* DISTINCT *)
      let query = self#distinct s.distinct query in
      query

    method clauses (Sql.Clause (s, ss)) =
      let q = self#select s in
      match ss with
      | None -> q
      | Some (op, c) -> (
          match op with
          | `UnionAll -> tuple [ q; self#clauses c ] Concat
          | `Union -> dedup (tuple [ q; self#clauses c ] Concat)
          | `Intersect | `Except -> failwith "Unsupported compound op." )

    method query q =
      self#clauses q.Sql.clauses |> self#order q.Sql.order
      |> self#limit q.Sql.limit
  end

let main params db fn =
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
  Sequence.iter stmts ~f:(fun q ->
      let r = (new conv_sql db)#query q in
      unsub params r |> Format.printf "%a\n@." Abslayout.pp)

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
      and db = Db.param
      and file = anon ("file" %: string) in
      fun () -> main params db file]
  |> run
