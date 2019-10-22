open! Core
open Collections
open Abslayout

type t = {
  state : (string * string) list;
  queries : (string * string) list;
  top_query : string;
}

let subst_of_schema bind schema =
  List.mapi schema ~f:(fun i n -> (n, sprintf "%s.%d" bind i))
  |> Map.of_alist_exn (module Name)

let type_to_cozy = function
  | Type.PrimType.IntT _ | DateT _ -> "Int"
  | BoolT _ -> "Bool"
  | StringT _ -> "String"
  | FixedT _ -> "Float"
  | _ -> failwith "Unexpected type."

let extend c1 c2 =
  {
    state = c1.state @ c2.state;
    queries = c1.queries @ c2.queries;
    top_query = c2.top_query;
  }

let numeric p2c float_fmt int_fmt n1 n2 =
  let p1, p2, t =
    match (Pred.to_type n1, Pred.to_type n2) with
    | FixedT _, FixedT _ -> (p2c n1, p2c n2, `Float)
    | IntT _, IntT _ | IntT _, DateT _ | DateT _, IntT _ | DateT _, DateT _ ->
        (p2c n1, p2c n2, `Int)
    | IntT _, FixedT _ -> (sprintf "i2f(%s)" (p2c n1), p2c n2, `Float)
    | FixedT _, IntT _ -> (p2c n1, sprintf "i2f(%s)" (p2c n2), `Float)
    | t, t' ->
        Error.create "Not a numeric type" (t, t')
          [%sexp_of: Type.PrimType.t * Type.PrimType.t]
        |> Error.raise
  in
  let fmt = match t with `Float -> float_fmt | `Int -> int_fmt in
  sprintf fmt p1 p2

let rec pred_to_cozy queries subst p =
  let p2s = pred_to_cozy queries subst in
  match p with
  | As_pred (p, _) -> p2s p
  | Name n -> Map.find subst n |> Option.value ~default:(Name.name n)
  | Int x -> Int.to_string x
  | Fixed x -> Fixed_point.to_string x
  | Date x -> Date.to_int x |> sprintf "%d"
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> sprintf "\"%s\"" s
  | Null _ -> failwith "Null literal not supported"
  | Unop (op, p) -> (
      let s = sprintf "(%s)" (p2s p) in
      match op with
      | Not -> sprintf "not (%s)" s
      | Day -> sprintf "day(%s)" s
      | Year -> sprintf "year(%s)" s
      | Strlen -> sprintf "strlen(%s)" s
      | Month -> sprintf "month(%s)" s
      | ExtractY -> sprintf "extracty(%s)" s
      | _ ->
          Error.create "Unimplemented unop" op [%sexp_of: unop] |> Error.raise
      )
  | Binop (op, p1, p2) -> (
      let s1 = sprintf "(%s)" (p2s p1) in
      let s2 = sprintf "(%s)" (p2s p2) in
      match op with
      | Eq -> sprintf "%s == %s" s1 s2
      | Lt -> sprintf "%s < %s" s1 s2
      | Le -> sprintf "%s <= %s" s1 s2
      | Gt -> sprintf "%s > %s" s1 s2
      | Ge -> sprintf "%s >= %s" s1 s2
      | And -> sprintf "%s and %s" s1 s2
      | Or -> sprintf "%s or %s" s1 s2
      | Add -> numeric p2s "%s + %s" "%s + %s" p1 p2
      | Sub -> numeric p2s "%s - %s" "%s - %s" p1 p2
      | Mul -> numeric p2s "fmul(%s, %s)" "imul(%s, %s)" p1 p2
      | Div -> numeric p2s "fdiv(%s, %s)" "idiv(%s, %s)" p1 p2
      | Mod -> sprintf "mod(%s, %s)" s1 s2
      | Strpos -> sprintf "strpos(%s, %s)" s1 s2 )
  | If (p1, p2, p3) -> sprintf "(%s) ? (%s) : (%s)" (p2s p1) (p2s p2) (p2s p3)
  | Exists _ -> failwith "Exists unimplemented"
  | First _ -> failwith "First unimplemented"
  | Substring _ -> failwith "Substring unsupported"
  | Count -> "count(*)"
  | Sum n -> sprintf "sum(%s)" (p2s n)
  | Avg n -> sprintf "avg(%s)" (p2s n)
  | Min n -> sprintf "min(%s)" (p2s n)
  | Max n -> sprintf "max(%s)" (p2s n)
  | Row_number -> failwith "Row_number not supported"

and to_cozy args q =
  let fresh = Fresh.create () in
  let rec to_cozy args q =
    let query_name = Fresh.name fresh "q%d" in
    let args_decl =
      List.map args ~f:(fun n ->
          sprintf "%s:%s" (Name.name n) (type_to_cozy (Name.type_exn n)))
      |> String.concat ~sep:", "
    in
    let make_query ?(name = query_name) =
      sprintf "query %s(%s)\n        %s" name args_decl
    in
    let call ?(args = args) n =
      let args_str = List.map args ~f:Name.name |> String.concat ~sep:", " in
      sprintf "%s(%s)" n args_str
    in
    match q.node with
    | Relation r ->
        let rel_type =
          Option.value_exn r.r_schema
          |> List.map ~f:(fun n -> type_to_cozy (Name.type_exn n))
          |> String.concat ~sep:"," |> sprintf "(%s)"
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
            top_query = query_name;
            state = [];
            queries =
              [
                ( query_name,
                  make_query (sprintf "distinct %s" (call cq.top_query)) );
              ];
          }
    | Filter (p, q) ->
        let subst = subst_of_schema "t" (schema_exn q) in
        let cq = to_cozy args q in
        extend cq
          {
            top_query = query_name;
            state = [];
            queries =
              [
                ( query_name,
                  make_query
                    (sprintf "[t | t <- %s, %s]" (call cq.top_query)
                       (pred_to_cozy subst p)) );
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
          List.init (List.length r1_schema) ~f:(sprintf "t1.%d")
          @ List.init (List.length r2_schema) ~f:(sprintf "t2.%d")
          |> String.concat ~sep:", " |> sprintf "(%s)"
        in
        extend c1 @@ extend c2
        @@ {
             top_query = query_name;
             queries =
               [
                 ( query_name,
                   make_query
                     (sprintf "[%s | t1 <- %s, t2 <- %s, %s]" out_tuple
                        (call c1.top_query) (call c2.top_query)
                        (pred_to_cozy subst pred)) );
               ];
             state = [];
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
            let out_tuple =
              List.map sel ~f:(pred_to_cozy subst)
              |> String.concat ~sep:", " |> sprintf "(%s)"
            in
            extend c
              {
                top_query = query_name;
                queries =
                  [
                    ( query_name,
                      make_query
                        (sprintf "[%s | t <- %s]" out_tuple (call c.top_query))
                    );
                  ];
                state = [];
              }
        | `Agg ->
            let out_exprs, queries =
              List.map sel ~f:(fun p ->
                  let agg_name = Fresh.name fresh "q%d" in
                  let inner_query = sprintf "t <- %s" (call c.top_query) in
                  match Pred.kind p with
                  | `Window -> failwith "Windows not implemented"
                  | `Scalar ->
                      ( call agg_name,
                        [
                          ( agg_name,
                            make_query ~name:agg_name
                              (sprintf "min [%s | %s]" (pred_to_cozy subst p)
                                 inner_query) );
                        ] )
                  | `Agg ->
                      let outer, inner = Pred.collect_aggs p in
                      let binds, queries =
                        List.map inner ~f:(fun (name, agg) ->
                            let queries =
                              match agg with
                              | Sum p ->
                                  [
                                    ( agg_name,
                                      make_query ~name:agg_name
                                        (sprintf "sum [%s | %s]"
                                           (pred_to_cozy subst p) inner_query)
                                    );
                                  ]
                              | Min p ->
                                  [
                                    ( agg_name,
                                      make_query ~name:agg_name
                                        (sprintf "min [%s | %s]"
                                           (pred_to_cozy subst p) inner_query)
                                    );
                                  ]
                              | Max p ->
                                  [
                                    ( agg_name,
                                      make_query ~name:agg_name
                                        (sprintf "max [%s | %s]"
                                           (pred_to_cozy subst p) inner_query)
                                    );
                                  ]
                              | Count ->
                                  [
                                    ( agg_name,
                                      make_query ~name:agg_name
                                        (sprintf "sum [1 | %s]" inner_query) );
                                  ]
                              | Avg p ->
                                  let qn1 = Fresh.name fresh "q%d" in
                                  let qn2 = Fresh.name fresh "q%d" in
                                  let q1 =
                                    make_query ~name:qn1
                                      (sprintf "sum [%s | %s]"
                                         (pred_to_cozy subst p) inner_query)
                                  in
                                  let q2 =
                                    make_query ~name:qn2
                                      (sprintf "sum [1 | %s]" inner_query)
                                  in
                                  let call_qn1 =
                                    match Pred.to_type p with
                                    | IntT _ | DateT _ ->
                                        sprintf "i2f(%s)" (call qn1)
                                    | _ -> call qn1
                                  in
                                  [
                                    (qn1, q1);
                                    (qn2, q2);
                                    ( agg_name,
                                      make_query ~name:agg_name
                                        (sprintf "fdiv(%s, i2f(%s))" call_qn1
                                           (call qn2)) );
                                  ]
                              | _ -> failwith "Not an aggregate."
                            in
                            ((Name.create name, call agg_name), queries))
                        |> List.unzip
                      in
                      let subst =
                        Map.merge_exn subst
                          (Map.of_alist_exn (module Name) binds)
                      in
                      (pred_to_cozy subst outer, List.concat queries))
              |> List.unzip
            in
            extend c
              {
                top_query = query_name;
                state = [];
                queries =
                  List.concat queries
                  @ [
                      ( query_name,
                        make_query
                          (sprintf "(%s)" (out_exprs |> String.concat ~sep:", "))
                      );
                    ];
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
          List.map ~f:Name.name args
          @ List.init (List.length key) ~f:(sprintf "k.%d")
          |> String.concat ~sep:", "
        in
        extend kc @@ extend vc
        @@ {
             top_query = query_name;
             queries =
               [
                 ( query_name,
                   make_query
                     (sprintf "[%s(%s) | k <- %s]" vc.top_query args_str
                        (call kc.top_query)) );
               ];
             state = [];
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
