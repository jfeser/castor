open! Core
open Collections
open Abslayout

type binop = [ `Le | `Lt | `Ge | `Gt | `Add | `Sub | `Mul | `Div | `Eq | `And ]
[@@deriving sexp]

type expr =
  [ `Binop of binop * expr * expr
  | `Call of string * expr list
  | `Cond of expr * expr * expr
  | `Int of int
  | `Len of query
  | `Sum of query
  | `The of query
  | `Tuple of expr list
  | `TupleGet of expr * int
  | `Var of string
  | `Let of string * expr * expr
  | `String of string ]
[@@deriving sexp]

and 'a lambda = [ `Lambda of string * 'a ] [@@deriving sexp]

and query =
  [ `Distinct of query
  | `Filter of expr lambda * query
  | `Flatmap of query lambda * query
  | `Map of expr lambda * query
  | `Table of string
  | `Empty ]
[@@deriving sexp]

let cost_lambda c (`Lambda (_, e)) = c e

let rec cost = function
  | `Distinct q -> cost q
  | `Flatmap (l, q) -> Big_o.(cost_lambda cost l * cost q)
  | `Filter (l, q) | `Map (l, q) -> Big_o.(cost_lambda cost_expr l * cost q)
  | `Table n -> Big_o.Var n
  | `Empty -> Big_o.Const

and cost_expr = function
  | `Binop (_, e, e') -> Big_o.(cost_expr e + cost_expr e')
  | `Call (_, args) | `Tuple args -> List.map args ~f:cost_expr |> Big_o.sum
  | `Cond (e1, e2, e3) -> Big_o.(cost_expr e1 + cost_expr e2 + cost_expr e3)
  | `Len q | `Sum q | `The q -> cost q
  | `Int _ | `Var _ | `String _ -> Big_o.Const
  | `TupleGet (e, _) -> cost_expr e
  | `Let (_, e, e') -> Big_o.(cost_expr e + cost_expr e')

type t = {
  state : (string * string) list;
  queries : (string * string) list;
  top_query : [ `Query of string | `Expr of string ];
}

let empty = { state = []; queries = []; top_query = `Query "" }

let extend c1 c2 =
  {
    state = c1.state @ c2.state;
    queries = c1.queries @ c2.queries;
    top_query = c2.top_query;
  }

let subst_of_schema bind schema =
  match schema with
  | [ n ] -> Map.singleton (module Name) n bind
  | ns ->
      List.mapi ns ~f:(fun i n -> (n, sprintf "%s.%d" bind i))
      |> Map.of_alist_exn (module Name)

let type_to_cozy = function
  | Prim_type.IntT _ | DateT _ -> "Int"
  | BoolT _ -> "Bool"
  | StringT _ -> "String"
  | FixedT _ -> "Float"
  | _ -> failwith "Unexpected type."

let numeric float_fmt int_fmt t1 t2 n1 n2 =
  let p1, p2, t =
    match (t1, t2) with
    | Prim_type.FixedT _, Prim_type.FixedT _ -> (n1, n2, `Float)
    | IntT _, IntT _ | IntT _, DateT _ | DateT _, IntT _ | DateT _, DateT _ ->
        (n1, n2, `Int)
    | IntT _, FixedT _ -> (sprintf "i2f(%s)" n1, n2, `Float)
    | FixedT _, IntT _ -> (n1, sprintf "i2f(%s)" n2, `Float)
    | t, t' ->
        Error.create "Not a numeric type" (t, t')
          [%sexp_of: Prim_type.t * Prim_type.t]
        |> Error.raise
  in
  let fmt = match t with `Float -> float_fmt | `Int -> int_fmt in
  sprintf fmt p1 p2

let eq t1 t2 v1 v2 =
  match (t1, t2) with
  | Prim_type.StringT _, Prim_type.StringT _ -> sprintf "streq(%s, %s)" v1 v2
  | _, _ -> sprintf "%s == %s" v1 v2

let call' ~args n =
  match n with `Query n -> sprintf "%s%s" n args | `Expr x -> x

let call ~args n =
  match n with
  | `Query n ->
      let args_str = List.map args ~f:Name.name |> String.concat ~sep:", " in
      sprintf "%s(%s)" n args_str
  | `Expr x -> x

let mk_tuple_elems n s =
  assert (n > 0);
  if n = 1 then [ s ] else List.init n ~f:(sprintf "%s.%d" s)

let mk_tuple xs = String.concat ~sep:", " xs |> sprintf "(%s)"

class to_cozy ?fresh ?(subst = Map.empty (module Name)) args =
  let fresh = match fresh with Some f -> f | None -> Fresh.create () in
  object (self : 'a)
    val args_decl =
      List.map args ~f:(fun n ->
          sprintf "%s:%s" (Name.name n) (type_to_cozy (Name.type_exn n)))
      |> mk_tuple

    method make_query' ?name body =
      let name =
        match name with Some n -> n | None -> Fresh.name fresh "q%d"
      in
      let body =
        sprintf "query %s%s" name args_decl
        :: List.map ~f:(sprintf "        %s") body
        |> String.concat ~sep:"\n"
      in
      (name, body)

    method make_query ?name body = self#make_query' ?name [ body ]

    method agg_select sel q =
      let c = self#query q in
      let subst = subst_of_schema "t" (schema_exn q) in
      let out_exprs, queries =
        List.map sel ~f:(fun p ->
            match Pred.kind p with
            | `Window -> failwith "Windows not implemented"
            | `Scalar ->
                let c', s = self#pred subst p in
                let c'', dummy =
                  match schema_exn (select [ p ] q) with
                  | [ n ] ->
                      let lit =
                        match Name.type_exn n with
                        | IntT _ -> Pred.Int 0
                        | FixedT _ -> Fixed (Fixed_point.of_int 0)
                        | StringT _ -> String ""
                        | BoolT _ -> Bool false
                        | DateT _ -> Int 0
                        | _ -> assert false
                      in
                      self#pred (Map.empty (module Name)) lit
                  | _ -> assert false
                in
                ( sprintf "((len q == 1) ? let {t = the q} in %s : %s)" s dummy,
                  extend c' c'' )
            | `Agg ->
                let outer, inner = Pred.collect_aggs p in
                let binds, queries =
                  List.map inner ~f:(fun (name, agg) ->
                      let mk_simple_agg name p =
                        let q, s = self#pred subst p in
                        (q, sprintf "%s [%s | t <- q]" name s)
                      in
                      let queries, expr =
                        match agg with
                        | Sum p -> mk_simple_agg "sum" p
                        | Min p -> mk_simple_agg "min" p
                        | Max p -> mk_simple_agg "max" p
                        | Count -> (empty, "sum [1 | t <- q]")
                        | Avg p ->
                            let q, s = self#pred subst p in
                            let e =
                              match Pred.to_type p with
                              | IntT _ | DateT _ ->
                                  sprintf
                                    "fdiv(sum [i2f(%s) | t <- q], i2f(sum [1 | \
                                     t <- q]))"
                                    s
                              | _ ->
                                  sprintf
                                    "fdiv(sum [%s | t <- q], i2f(sum [1 | t <- \
                                     q]))"
                                    s
                            in
                            (q, e)
                        | _ -> failwith "Not an aggregate."
                      in
                      ((Name.create name, expr), queries))
                  |> List.unzip
                in
                let subst =
                  Map.merge_exn subst (Map.of_alist_exn (module Name) binds)
                in
                let q, s = self#pred subst outer in
                (s, List.fold_left ~init:q ~f:extend queries))
        |> List.unzip
      in
      let expr =
        sprintf "let {q = %s} in %s" (call ~args c.top_query)
          (mk_tuple out_exprs)
      in
      let ((name, _) as query) = self#make_query expr in
      extend (List.fold_left ~init:c ~f:extend queries)
      @@ { empty with top_query = `Query name; queries = [ query ] }

    method filter p q =
      let subst = subst_of_schema "t" (schema_exn q) in
      let cq = self#query q in
      let cqp, cp = self#pred subst p in
      let ((name, _) as query) =
        self#make_query
          (sprintf "[t | t <- %s, %s]" (call ~args cq.top_query) cp)
      in
      extend cq @@ extend cqp
      @@ { empty with top_query = `Query name; queries = [ query ] }

    method query q =
      match q.node with
      | Relation r ->
          let rel_type =
            Option.value_exn r.r_schema
            |> List.map ~f:(fun n -> type_to_cozy (Name.type_exn n))
            |> mk_tuple
          in
          {
            empty with
            state =
              [ (r.r_name, sprintf "state %s : Bag<%s>" r.r_name rel_type) ];
            top_query = `Expr r.r_name;
          }
      | Dedup q ->
          let cq = self#query q in
          extend cq
            {
              empty with
              top_query =
                `Expr (sprintf "distinct %s" (call ~args cq.top_query));
            }
      | Filter (p, q) -> self#filter p q
      | Join { pred; r1; r2 } ->
          let r1_schema = schema_exn r1 in
          let r2_schema = schema_exn r2 in
          let subst =
            Map.merge_exn
              (subst_of_schema "t1" r1_schema)
              (subst_of_schema "t2" r2_schema)
          in
          let c1 = self#query r1 in
          let c2 = self#query r2 in
          let out_tuple =
            mk_tuple_elems (List.length r1_schema) "t1"
            @ mk_tuple_elems (List.length r2_schema) "t2"
            |> mk_tuple
          in
          let cqp, cp = self#pred subst pred in
          let ((name, _) as query) =
            self#make_query
              (sprintf "[%s | t1 <- [t | t <- %s], t2 <- [t | t <- %s], %s]"
                 out_tuple (call ~args c1.top_query) (call ~args c2.top_query)
                 cp)
          in
          extend c1 @@ extend c2 @@ extend cqp
          @@ { empty with top_query = `Query name; queries = [ query ] }
      | OrderBy { rel; _ } ->
          Logs.warn (fun m ->
              m "Cozy does not support ordering. Ignoring order.");
          self#query rel
      | Select (sel, q) -> (
          match select_kind sel with
          | `Scalar ->
              let subst = subst_of_schema "t" (schema_exn q) in
              let c = self#query q in
              let queries, preds =
                List.map sel ~f:(self#pred subst) |> List.unzip
              in
              let out_tuple = mk_tuple preds in
              let ((name, _) as query) =
                self#make_query
                  (sprintf "[%s | t <- %s]" out_tuple (call ~args c.top_query))
              in
              extend
                (List.fold_left ~init:c ~f:extend queries)
                { empty with top_query = `Query name; queries = [ query ] }
          | `Agg -> self#agg_select sel q )
      | GroupBy (sel, key, q) ->
          let kc = self#query (dedup (select (List.map ~f:Pred.name key) q)) in
          let filter_preds, pred_args =
            List.map key ~f:(fun k ->
                let arg =
                  Name.create ~type_:(Name.type_exn k) (Fresh.name fresh "x%d")
                in
                (Pred.Infix.(name k = name arg), arg))
            |> List.unzip
          in
          let vc =
            (new to_cozy ~fresh (args @ pred_args))#query
              (select sel (filter (Pred.conjoin filter_preds) q))
          in
          let args_str =
            List.map ~f:Name.name args @ mk_tuple_elems (List.length key) "k"
            |> mk_tuple
          in
          let ((name, _) as query) =
            self#make_query
              (sprintf "[%s | k <- %s]"
                 (call' vc.top_query ~args:args_str)
                 (call ~args kc.top_query))
          in
          extend kc @@ extend vc
          @@ { empty with top_query = `Query name; queries = [ query ] }
      | _ -> failwith "Unimplemented"

    method subquery subst fmt q =
      let binds = Map.to_alist subst in
      let call_args = args @ List.map ~f:(fun (_, v) -> Name.create v) binds in
      let subst, decl_args =
        List.map binds ~f:(fun (k, _) ->
            let v' = Fresh.name fresh "x%d" in
            ((k, v'), Name.copy k ~name:v'))
        |> List.unzip
      in
      let subst = Map.of_alist_exn (module Name) subst in
      let args = args @ decl_args in
      let cq = (new to_cozy ~fresh ~subst args)#query q in
      (cq, sprintf fmt (call ~args:call_args cq.top_query))

    method pred s (p : Pred.t) : t * _ =
      let subst = Map.merge_exn subst s in
      match p with
      | As_pred (p, _) -> self#pred s p
      | Name n ->
          (empty, Map.find subst n |> Option.value ~default:(Name.name n))
      | Int x -> (empty, Int.to_string x)
      | Fixed x -> (empty, sprintf "%sf" (Fixed_point.to_string x))
      | Date x -> (empty, Date.to_int x |> sprintf "%d")
      | Bool true -> (empty, "true")
      | Bool false -> (empty, "false")
      | String s -> (empty, sprintf "\"%s\"" s)
      | Null _ -> failwith "Null literal not supported"
      | Unop (op, p) ->
          let queries, pred = self#pred s p in
          let pred =
            match op with
            | Not -> sprintf "not (%s)" pred
            | Day -> sprintf "day(%s)" pred
            | Year -> sprintf "year(%s)" pred
            | Strlen -> sprintf "strlen(%s)" pred
            | Month -> sprintf "month(%s)" pred
            | ExtractY -> sprintf "extracty(%s)" pred
            | _ ->
                Error.create "Unimplemented unop" op [%sexp_of: Unop.t]
                |> Error.raise
          in
          (queries, pred)
      | Binop (op, p1, p2) ->
          let q1, s1 = self#pred s p1 in
          let s1 = sprintf "(%s)" s1 in
          let q2, s2 = self#pred s p2 in
          let s2 = sprintf "(%s)" s2 in
          let t1 = Pred.to_type p1 in
          let t2 = Pred.to_type p2 in
          let s =
            match op with
            | Eq -> eq t1 t2 s1 s2
            | Lt -> numeric "%s < %s" "%s < %s" t1 t2 s1 s2
            | Le -> numeric "%s <= %s" "%s <= %s" t1 t2 s1 s2
            | Gt -> numeric "%s > %s" "%s > %s" t1 t2 s1 s2
            | Ge -> numeric "%s >= %s" "%s >= %s" t1 t2 s1 s2
            | And -> sprintf "%s and %s" s1 s2
            | Or -> sprintf "%s or %s" s1 s2
            | Add -> numeric "%s + %s" "%s + %s" t1 t2 s1 s2
            | Sub -> numeric "%s - %s" "%s - %s" t1 t2 s1 s2
            | Mul -> numeric "fmul(%s, %s)" "imul(%s, %s)" t1 t2 s1 s2
            | Div -> numeric "fdiv(%s, %s)" "idiv(%s, %s)" t1 t2 s1 s2
            | Mod -> sprintf "mod(%s, %s)" s1 s2
            | Strpos -> sprintf "strpos(%s, %s)" s1 s2
          in
          (extend q1 q2, s)
      | If (p1, p2, p3) ->
          let q1, s1 = self#pred s p1 in
          let q2, s2 = self#pred s p2 in
          let q3, s3 = self#pred s p3 in
          (extend q1 @@ extend q2 q3, sprintf "(%s) ? (%s) : (%s)" s1 s2 s3)
      | Exists q -> self#subquery subst "exists %s" q
      | First q -> self#subquery subst "%s" q
      | Substring _ -> failwith "Substring unsupported"
      | Count | Sum _ | Avg _ | Min _ | Max _ ->
          failwith "Unexpected aggregates"
      | Row_number -> failwith "Row_number not supported"
  end

let to_string bench_name params q =
  let x = (new to_cozy params)#query q in
  let state =
    List.dedup_and_sort x.state ~compare:(fun (n, _) (n', _) ->
        [%compare: string] n n')
    |> List.map ~f:(fun (_, x) -> "    " ^ x)
    |> String.concat ~sep:"\n"
  in
  let queries =
    List.map x.queries ~f:(fun (n, q) ->
        let s =
          if Poly.(`Query n = x.top_query) then q else sprintf "private %s" q
        in
        "    " ^ s)
    |> String.concat ~sep:"\n"
  in
  let lib =
    {|

    extern imul(x : Int, y : Int) : Int = "{x} * {y}"
    extern idiv(x : Int, y : Int) : Int = "{x} / {y}"
    extern imod(x : Int, y : Int) : Int = "{x} % {y}"
    extern fmul(x : Float, y : Float) : Float = "{x} * {y}"
    extern fdiv(x : Float, y : Float) : Float = "{x} / {y}"
    extern strpos(x : String, y : String) : Int = "strpos({x}, {y})"
    extern day(x : Int) : Int = "of_day({x})"
    extern year(x : Int) : Int = "of_year({x})"
    extern month(x : Int) : Int = "to_month({x})"
    extern extracty(x : Int) : Int = "to_year({x})"
    extern i2f(x : Int) : Float = "int_to_float({x})"
    extern strlen(x : String) : Int = "strlen({x})"
    extern streq(x : String, y : String) : Bool = "streq({x}, {y})"
|}
    |> String.strip
  in
  let name = String.filter bench_name ~f:(fun ch -> Char.(ch <> '-')) in
  sprintf "query%s:\n%s\n%s\n%s" name lib state queries
