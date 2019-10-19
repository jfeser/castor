open! Core
open Collections
open Abslayout
open Sql

type t = {
  state : (string * string) list;
  queries : (string * string) list;
  top_query : string;
}

let rec pred_to_cozy subst p =
  let p2s = pred_to_cozy subst in
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
      | _ -> failwith "Unsupported unary op" )
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
      | Add -> sprintf "%s + %s" s1 s2
      | Sub -> sprintf "%s - %s" s1 s2
      | Mul -> sprintf "mul(%s, %s)" s1 s2
      | Div -> sprintf "div(%s, %s)" s1 s2
      | Mod -> sprintf "mod(%s, %s)" s1 s2
      | Strpos -> failwith "Strpos not supported" )
  | If (p1, p2, p3) -> sprintf "(%s) ? (%s) : (%s)" (p2s p1) (p2s p2) (p2s p3)
  | Exists r ->
      let sql = of_ralgebra r |> to_string in
      sprintf "exists (%s)" sql
  | First r ->
      let sql = of_ralgebra r |> to_string in
      sprintf "(%s)" sql
  | Substring (p1, p2, p3) -> failwith "Substring unsupported"
  | Count -> "count(*)"
  | Sum n -> sprintf "sum(%s)" (p2s n)
  | Avg n -> sprintf "avg(%s)" (p2s n)
  | Min n -> sprintf "min(%s)" (p2s n)
  | Max n -> sprintf "max(%s)" (p2s n)
  | Row_number -> failwith "Row_number not supported"

let subst_of_schema bind schema =
  List.mapi schema ~f:(fun i n -> (n, sprintf "%s.%d" bind i))
  |> Map.of_alist_exn (module Name)

let to_cozy args q =
  let fresh = Fresh.create () in
  let rec to_cozy args q =
    let query_name = Fresh.name fresh "q%d" in
    match q.node with
    | Relation r ->
        let rel_type =
          Option.value_exn r.r_schema
          |> List.map ~f:(fun n ->
                 match Name.type_exn n with
                 | IntT _ | DateT _ -> "Int"
                 | BoolT _ -> "Bool"
                 | StringT _ -> "String"
                 | FixedT _ -> "Float"
                 | _ -> failwith "Unexpected type.")
          |> String.concat ~sep:"," |> sprintf "(%s)"
        in
        {
          state =
            [ (r.r_name, sprintf "state %s : Bag<%s>" r.r_name rel_type) ];
          queries =
            [
              ( query_name,
                sprintf "query %s()\n\t[t | t <- %s]" query_name r.r_name );
            ];
          top_query = query_name;
        }
    | Dedup q ->
        let cq = to_cozy args q in
        {
          cq with
          top_query = query_name;
          queries =
            ( query_name,
              sprintf "query %s()\n\tdistinct %s()" query_name cq.top_query )
            :: cq.queries;
        }
    | Filter (p, q) ->
        let subst = subst_of_schema "t" (schema_exn q) |> Map.merge_exn args in
        let cq = to_cozy args q in
        {
          cq with
          top_query = query_name;
          queries =
            ( query_name,
              sprintf "query %s()\n\t[t | t <- %s(), %s]" query_name
                cq.top_query (pred_to_cozy subst p) )
            :: cq.queries;
        }
    | Join { pred; r1; r2 } ->
        let r1_schema = schema_exn r1 in
        let r2_schema = schema_exn r2 in
        let subst =
          Map.merge_exn
            (subst_of_schema "t1" r1_schema)
            (subst_of_schema "t2" r2_schema)
          |> Map.merge_exn args
        in
        let c1 = to_cozy args r1 in
        let c2 = to_cozy args r2 in
        let out_tuple =
          List.init (List.length r1_schema) ~f:(sprintf "t1.%d")
          @ List.init (List.length r2_schema) ~f:(sprintf "t2.%d")
          |> String.concat ~sep:", " |> sprintf "(%s)"
        in
        {
          top_query = query_name;
          queries =
            ( query_name,
              sprintf "query %s()\n\t[%s | t1 <- %s(), t2 <- %s(), %s]"
                query_name out_tuple c1.top_query c2.top_query
                (pred_to_cozy subst pred) )
            :: (c1.queries @ c2.queries);
          state = c1.state @ c2.state;
        }
    | OrderBy { rel; _ } ->
        Logs.warn (fun m ->
            m "Cozy does not support ordering. Ignoring order.");
        to_cozy args rel
    | Select (sel, q) -> (
        let c = to_cozy args q in
        let subst = subst_of_schema "t" (schema_exn q) |> Map.merge_exn args in
        match select_kind sel with
        | `Scalar ->
            let out_tuple =
              List.map sel ~f:(pred_to_cozy subst)
              |> String.concat ~sep:", " |> sprintf "(%s)"
            in
            {
              c with
              top_query = query_name;
              queries =
                ( query_name,
                  sprintf "query %s()\n\t [%s | t <- %s()]" query_name
                    out_tuple c.top_query )
                :: c.queries;
            }
        | `Agg ->
            let out_exprs, in_exprs, queries =
              List.map sel ~f:(fun p ->
                  let query_name = Fresh.name fresh "q%d" in
                  let inner_query = sprintf "t <- %s()" c.top_query in
                  match Pred.kind p with
                  | `Window -> failwith "Windows not implemented"
                  | `Scalar ->
                      let value_name = Fresh.name fresh "x%d" in
                      ( value_name,
                        [ sprintf "%s <- %s()" value_name query_name ],
                        [
                          ( query_name,
                            sprintf "query %s()\n\t min [%s | %s]" query_name
                              (pred_to_cozy subst p) inner_query );
                        ] )
                  | `Agg ->
                      let outer, inner = Pred.collect_aggs p in
                      let binds, queries =
                        List.map inner ~f:(fun (name, agg) ->
                            let query_name = Fresh.name fresh "q%d" in
                            let queries =
                              match agg with
                              | Sum p ->
                                  [
                                    ( query_name,
                                      sprintf "query %s()\n\t sum [%s | %s]"
                                        query_name (pred_to_cozy subst p)
                                        inner_query );
                                  ]
                              | Min p ->
                                  [
                                    ( query_name,
                                      sprintf "query %s()\n\t min [%s | %s]"
                                        query_name (pred_to_cozy subst p)
                                        inner_query );
                                  ]
                              | Max p ->
                                  [
                                    ( query_name,
                                      sprintf "query %s()\n\t max [%s | %s]"
                                        query_name (pred_to_cozy subst p)
                                        inner_query );
                                  ]
                              | Count ->
                                  [
                                    ( query_name,
                                      sprintf "query %s()\n\t sum [1 | %s]"
                                        query_name inner_query );
                                  ]
                              | Avg p ->
                                  let qn1 = Fresh.name fresh "q%d" in
                                  let qn2 = Fresh.name fresh "q%d" in
                                  let q1 =
                                    sprintf "query %s()\n\t sum [%s | %s]\n"
                                      qn1 (pred_to_cozy subst p) inner_query
                                  in
                                  let q2 =
                                    sprintf "query %s()\n\t sum [1 | %s]\n" qn2
                                      inner_query
                                  in
                                  [
                                    (qn1, q1);
                                    (qn2, q2);
                                    ( query_name,
                                      sprintf
                                        "query %s()\n\
                                         \t [e1 / e2 | e1 <- %s(), e2 <- %s()]"
                                        query_name qn1 qn2 );
                                  ]
                              | _ -> failwith "Not an aggregate."
                            in
                            (sprintf "%s <- %s()" name query_name, queries))
                        |> List.unzip
                      in
                      (pred_to_cozy subst outer, binds, List.concat queries))
              |> List.unzip3
            in
            {
              c with
              top_query = query_name;
              queries =
                ( query_name,
                  sprintf "query %s()\n\t [(%s) | %s]" query_name
                    (out_exprs |> String.concat ~sep:", ")
                    (in_exprs |> List.concat |> String.concat ~sep:", ") )
                :: (List.concat queries @ c.queries);
            } )
    | GroupBy (_, key, q) -> failwith ""
    | _ -> failwith "Unimplemented"
  in
  to_cozy args q

let to_string bench_name q =
  let x = to_cozy (Map.empty (module Name)) q in
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
    extern mul(x : Float, y : Float) : Float = "{x} * {y}"
    extern div(x : Float, y : Float) : Float = "{x} / {y}"
    extern mod(x : Float, y : Float) : Float = "{x} % {y}"
    extern day(x : Int) : Int = "{x}"
|}
    |> String.strip
  in
  sprintf "query%s:\n%s\n%s\n%s" bench_name lib state queries
