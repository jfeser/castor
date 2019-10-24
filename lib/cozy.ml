open! Core
open Collections
open Abslayout

type t = {
  state : (string * string) list;
  queries : (string * string) list;
  top_query : string;
}

let empty = { state = []; queries = []; top_query = "" }

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
  | Type.PrimType.IntT _ | DateT _ -> "Int"
  | BoolT _ -> "Bool"
  | StringT _ -> "String"
  | FixedT _ -> "Float"
  | _ -> failwith "Unexpected type."

let numeric float_fmt int_fmt t1 t2 n1 n2 =
  let p1, p2, t =
    match (t1, t2) with
    | Type.PrimType.FixedT _, Type.PrimType.FixedT _ -> (n1, n2, `Float)
    | IntT _, IntT _ | IntT _, DateT _ | DateT _, IntT _ | DateT _, DateT _ ->
        (n1, n2, `Int)
    | IntT _, FixedT _ -> (sprintf "i2f(%s)" n1, n2, `Float)
    | FixedT _, IntT _ -> (n1, sprintf "i2f(%s)" n2, `Float)
    | t, t' ->
        Error.create "Not a numeric type" (t, t')
          [%sexp_of: Type.PrimType.t * Type.PrimType.t]
        |> Error.raise
  in
  let fmt = match t with `Float -> float_fmt | `Int -> int_fmt in
  sprintf fmt p1 p2

let call ~args n =
  let args_str = List.map args ~f:Name.name |> String.concat ~sep:", " in
  sprintf "%s(%s)" n args_str

let mk_tuple_elems n s =
  assert (n > 0);
  if n = 1 then [ s ] else List.init n ~f:(sprintf "%s.%d" s)

let mk_tuple xs = String.concat ~sep:", " xs |> sprintf "(%s)"

let to_cozy args q =
  let fresh = Fresh.create () in
  let rec pred_to_cozy args subst p =
    let p2s = pred_to_cozy args subst in
    match p with
    | As_pred (p, _) -> p2s p
    | Name n -> (empty, Map.find subst n |> Option.value ~default:(Name.name n))
    | Int x -> (empty, Int.to_string x)
    | Fixed x -> (empty, sprintf "%sf" (Fixed_point.to_string x))
    | Date x -> (empty, Date.to_int x |> sprintf "%d")
    | Bool true -> (empty, "true")
    | Bool false -> (empty, "false")
    | String s -> (empty, sprintf "\"%s\"" s)
    | Null _ -> failwith "Null literal not supported"
    | Unop (op, p) ->
        let queries, pred = p2s p in
        let pred =
          match op with
          | Not -> sprintf "not (%s)" pred
          | Day -> sprintf "day(%s)" pred
          | Year -> sprintf "year(%s)" pred
          | Strlen -> sprintf "strlen(%s)" pred
          | Month -> sprintf "month(%s)" pred
          | ExtractY -> sprintf "extracty(%s)" pred
          | _ ->
              Error.create "Unimplemented unop" op [%sexp_of: unop]
              |> Error.raise
        in
        (queries, pred)
    | Binop (op, p1, p2) ->
        let q1, s1 = p2s p1 in
        let s1 = sprintf "(%s)" s1 in
        let q2, s2 = p2s p2 in
        let s2 = sprintf "(%s)" s2 in
        let t1 = Pred.to_type p1 in
        let t2 = Pred.to_type p2 in
        let s =
          match op with
          | Eq -> sprintf "%s == %s" s1 s2
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
        let q1, s1 = p2s p1 in
        let q2, s2 = p2s p2 in
        let q3, s3 = p2s p3 in
        (extend q1 @@ extend q2 q3, sprintf "(%s) ? (%s) : (%s)" s1 s2 s3)
    | Exists q ->
        let cq = to_cozy args q in
        (cq, sprintf "exists %s" (call ~args cq.top_query))
    | First q ->
        let cq = to_cozy args q in
        (cq, call ~args cq.top_query)
    | Substring _ -> failwith "Substring unsupported"
    | Count | Sum _ | Avg _ | Min _ | Max _ -> failwith "Unexpected aggregates"
    | Row_number -> failwith "Row_number not supported"
  and to_cozy args q =
    let query_name = Fresh.name fresh "q%d" in
    let args_decl =
      List.map args ~f:(fun n ->
          sprintf "%s:%s" (Name.name n) (type_to_cozy (Name.type_exn n)))
      |> mk_tuple
    in
    let make_query ?(name = query_name) =
      sprintf "query %s%s\n        %s" name args_decl
    in
    match q.node with
    | Relation r ->
        let rel_type =
          Option.value_exn r.r_schema
          |> List.map ~f:(fun n -> type_to_cozy (Name.type_exn n))
          |> mk_tuple
        in
        {
          state =
            [ (r.r_name, sprintf "state %s : Bag<%s>" r.r_name rel_type) ];
          queries =
            [ (query_name, make_query (sprintf "[t | t <- %s]" r.r_name)) ];
          top_query = query_name;
        }
    | Dedup q ->
        let cq = to_cozy args q in
        extend cq
          {
            empty with
            top_query = query_name;
            queries =
              [
                ( query_name,
                  make_query (sprintf "distinct %s" (call ~args cq.top_query))
                );
              ];
          }
    | Filter (p, q) ->
        let subst = subst_of_schema "t" (schema_exn q) in
        let cq = to_cozy args q in
        let cqp, cp = pred_to_cozy args subst p in
        extend cq @@ extend cqp
        @@ {
             empty with
             top_query = query_name;
             queries =
               [
                 ( query_name,
                   make_query
                     (sprintf "[t | t <- %s, %s]" (call ~args cq.top_query) cp)
                 );
               ];
           }
    | Join { pred; r1; r2 } ->
        let r1_schema = schema_exn r1 in
        let r2_schema = schema_exn r2 in
        let subst =
          Map.merge_exn
            (subst_of_schema "t1" r1_schema)
            (subst_of_schema "t2" r2_schema)
        in
        let c1 = to_cozy args r1 in
        let c2 = to_cozy args r2 in
        let out_tuple =
          mk_tuple_elems (List.length r1_schema) "t1"
          @ mk_tuple_elems (List.length r2_schema) "t2"
          |> mk_tuple
        in
        let cqp, cp = pred_to_cozy args subst pred in
        extend c1 @@ extend c2 @@ extend cqp
        @@ {
             empty with
             top_query = query_name;
             queries =
               [
                 ( query_name,
                   make_query
                     (sprintf "[%s | t1 <- %s, t2 <- %s, %s]" out_tuple
                        (call ~args c1.top_query) (call ~args c2.top_query) cp)
                 );
               ];
           }
    | OrderBy { rel; _ } ->
        Logs.warn (fun m ->
            m "Cozy does not support ordering. Ignoring order.");
        to_cozy args rel
    | Select (sel, q) -> (
        let c = to_cozy args q in
        let subst = subst_of_schema "t" (schema_exn q) in
        match select_kind sel with
        | `Scalar ->
            let queries, preds =
              List.map sel ~f:(pred_to_cozy args subst) |> List.unzip
            in
            let out_tuple = mk_tuple preds in
            extend
              (List.fold_left ~init:c ~f:extend queries)
              {
                top_query = query_name;
                queries =
                  [
                    ( query_name,
                      make_query
                        (sprintf "[%s | t <- %s]" out_tuple
                           (call ~args c.top_query)) );
                  ];
                state = [];
              }
        | `Agg ->
            let out_exprs, queries, fragments =
              List.map sel ~f:(fun p ->
                  let agg_name = Fresh.name fresh "q%d" in
                  let inner_query =
                    sprintf "t <- %s" (call ~args c.top_query)
                  in
                  match Pred.kind p with
                  | `Window -> failwith "Windows not implemented"
                  | `Scalar ->
                      let q, s = pred_to_cozy args subst p in
                      ( call ~args agg_name,
                        q,
                        [
                          ( agg_name,
                            make_query ~name:agg_name
                              (sprintf "min [%s | %s]" s inner_query) );
                        ] )
                  | `Agg ->
                      let outer, inner = Pred.collect_aggs p in
                      let binds, queries, fragments =
                        List.map inner ~f:(fun (name, agg) ->
                            let mk_simple_agg name p =
                              let q, s = pred_to_cozy args subst p in
                              ( q,
                                [
                                  ( agg_name,
                                    make_query ~name:agg_name
                                      (sprintf "%s [%s | %s]" name s
                                         inner_query) );
                                ] )
                            in
                            let queries, fragments =
                              match agg with
                              | Sum p -> mk_simple_agg "sum" p
                              | Min p -> mk_simple_agg "min" p
                              | Max p -> mk_simple_agg "max" p
                              | Count ->
                                  ( empty,
                                    [
                                      ( agg_name,
                                        make_query ~name:agg_name
                                          (sprintf "sum [1 | %s]" inner_query)
                                      );
                                    ] )
                              | Avg p ->
                                  let qn1 = Fresh.name fresh "q%d" in
                                  let qn2 = Fresh.name fresh "q%d" in
                                  let q, s = pred_to_cozy args subst p in
                                  let q1 =
                                    make_query ~name:qn1
                                      (sprintf "sum [%s | %s]" s inner_query)
                                  in
                                  let q2 =
                                    make_query ~name:qn2
                                      (sprintf "sum [1 | %s]" inner_query)
                                  in
                                  let call_qn1 =
                                    match Pred.to_type p with
                                    | IntT _ | DateT _ ->
                                        sprintf "i2f(%s)" (call ~args qn1)
                                    | _ -> call ~args qn1
                                  in
                                  ( q,
                                    [
                                      (qn1, q1);
                                      (qn2, q2);
                                      ( agg_name,
                                        make_query ~name:agg_name
                                          (sprintf "fdiv(%s, i2f(%s))" call_qn1
                                             (call ~args qn2)) );
                                    ] )
                              | _ -> failwith "Not an aggregate."
                            in
                            ( (Name.create name, call ~args agg_name),
                              queries,
                              fragments ))
                        |> List.unzip3
                      in
                      let subst =
                        Map.merge_exn subst
                          (Map.of_alist_exn (module Name) binds)
                      in
                      let q, s = pred_to_cozy args subst outer in
                      ( s,
                        List.fold_left ~init:q ~f:extend queries,
                        List.concat fragments ))
              |> List.unzip3
            in
            extend (List.fold_left ~init:c ~f:extend queries)
            @@ {
                 empty with
                 top_query = query_name;
                 queries =
                   List.concat fragments
                   @ [ (query_name, make_query (mk_tuple out_exprs)) ];
               } )
    | GroupBy (sel, key, q) ->
        let kc = to_cozy args (dedup (select (List.map ~f:Pred.name key) q)) in
        let filter_preds, pred_args =
          List.map key ~f:(fun k ->
              let arg =
                Name.create ~type_:(Name.type_exn k) (Fresh.name fresh "x%d")
              in
              (Pred.(binop (Eq, name k, name arg)), arg))
          |> List.unzip
        in
        let vc =
          to_cozy (args @ pred_args)
            (select sel (filter (Pred.conjoin filter_preds) q))
        in
        let args_str =
          List.map ~f:Name.name args @ mk_tuple_elems (List.length key) "k"
          |> mk_tuple
        in
        extend kc @@ extend vc
        @@ {
             empty with
             top_query = query_name;
             queries =
               [
                 ( query_name,
                   make_query
                     (sprintf "[%s%s | k <- %s]" vc.top_query args_str
                        (call ~args kc.top_query)) );
               ];
           }
    | _ -> failwith "Unimplemented"
  in
  to_cozy args q

let to_string bench_name params q =
  let x = to_cozy params q in
  let state =
    List.dedup_and_sort x.state ~compare:(fun (n, _) (n', _) ->
        [%compare: string] n n')
    |> List.map ~f:(fun (_, x) -> "    " ^ x)
    |> String.concat ~sep:"\n"
  in
  let queries =
    List.map x.queries ~f:(fun (n, q) ->
        let s =
          if String.(n = x.top_query) then q else sprintf "private %s" q
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
    extern fmod(x : Float, y : Float) : Float = "{x} % {y}"
    extern strpos(x : String, y : String) : Int = "strpos({x}, {y})"
    extern day(x : Int) : Int = "{x}"
    extern year(x : Int) : Int = "{x}"
    extern month(x : Int) : Int = "{x}"
    extern extracty(x : Int) : Int = "{x}"
    extern i2f(x : Int) : Float = "{x}"
    extern strlen(x : String) : Int = "{x}"
|}
    |> String.strip
  in
  let name = String.filter bench_name ~f:(fun ch -> ch <> '-') in
  sprintf "query%s:\n%s\n%s\n%s" name lib state queries
