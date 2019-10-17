open! Core
open Collections
open Abslayout
open Sql

type t = {
  state : (string * string) list;
  queries : (string * string) list;
  top_query : string;
}

let rec pred_to_cozy p =
  let p2s = pred_to_cozy in
  match p with
  | As_pred (p, _) -> p2s p
  | Name n -> (
      match Name.rel n with
      | Some r -> sprintf "%s.%s" r (Name.name n)
      | None -> Name.name n )
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

let to_cozy q =
  let fresh = Fresh.create () in
  let sql = Sql.of_ralgebra q in
  let rec sql_to_cozy = function
    | Query spj ->
        if List.length spj.order > 0 then
          Logs.warn (fun m ->
              m "Cozy does not support ordering. Ignoring order.");
        if List.length spj.group > 0 then
          Logs.err (fun m -> m "Grouping not implemented.");
        let query_name = Fresh.name fresh "q%d" in
        let select =
          List.map spj.select ~f:(fun x -> pred_to_cozy x.pred)
          |> String.concat ~sep:", " |> sprintf "(%s)"
        in
        let conds =
          if List.length spj.conds = 0 then ""
          else
            ", "
            ^ (List.map spj.conds ~f:pred_to_cozy |> String.concat ~sep:", ")
        in
        let distinct = if spj.distinct then "distinct" else "" in
        let state, sub_specs, relations =
          List.map spj.relations ~f:(function
            | `Table (rel, alias), `Left ->
                let schema = Option.value_exn rel.r_schema in
                let rel_record =
                  List.map schema ~f:(fun n ->
                      let type_str =
                        match Name.type_exn n with
                        | IntT _ | DateT _ -> "Int"
                        | BoolT _ -> "Bool"
                        | StringT _ -> "String"
                        | FixedT _ -> "Float"
                        | _ -> failwith "Unexpected type."
                      in
                      sprintf "%s:%s" (Name.name n) type_str)
                  |> String.concat ~sep:"," |> sprintf "{%s}"
                in
                ( Some
                    ( rel.r_name,
                      sprintf "state Bag<%s> %s" rel_record rel.r_name ),
                  None,
                  sprintf "%s <- %s" alias rel.r_name )
            | `Subquery (q, alias), `Left ->
                let spec = sql_to_cozy q in
                (None, Some spec, sprintf "%s <- %s()" alias spec.top_query)
            | _, `Lateral -> failwith "Lateral joins not supported"
            | `Series _, _ -> failwith "Series not supported")
          |> List.unzip3
        in
        let state = List.filter_map state ~f:Fun.id in
        let sub_specs = List.filter_map sub_specs ~f:Fun.id in
        let relations = String.concat relations ~sep:", " in
        let query_str =
          sprintf "\tquery %s()\n\t%s [%s | %s %s]" query_name distinct select
            relations conds
        in
        {
          top_query = query_name;
          state = state @ List.concat_map sub_specs ~f:(fun x -> x.state);
          queries =
            (query_name, query_str)
            :: List.concat_map sub_specs ~f:(fun x -> x.queries);
        }
    | Union_all _ -> failwith "Union all not supported"
  in
  sql_to_cozy sql

let to_string bench_name q =
  let x = to_cozy q in
  let state =
    List.dedup_and_sort x.state ~compare:(fun (n, _) (n', _) ->
        [%compare: string] n n')
    |> List.map ~f:(fun (_, x) -> "\t" ^ x)
    |> String.concat ~sep:"\n"
  in
  let queries =
    List.map x.queries ~f:(fun (n, q) ->
        if String.(n = x.top_query) then q else sprintf "private %s" q)
    |> String.concat ~sep:"\n"
  in
  let lib =
    {|
    extern mul(x : Float, y : Float) : Float = "{x} * {y}"
    extern div(x : Float, y : Float) : Float = "{x} / {y}"
    extern mod(x : Float, y : Float) : Float = "{x} % {y}"
    extern day(x : Int) : Int = "{x}"
|}
  in
  sprintf "%s:\n%s\n%s\n%s" bench_name lib state queries
