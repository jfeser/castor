open Base
open Printf

open Db
open Layout

type op =
  | Eq
  | Lt
  | Le
  | Gt
  | Ge
  | And
  | Or
  | Add
  | Sub
  | Mul
  | Div
  | Mod

type 'f agg =
  | Count
  | Key of 'f
  | Sum of 'f
  | Avg of 'f
  | Min of 'f
  | Max of 'f

type ('f, 'l) ralgebra =
  | Project of 'f list * ('f, 'l) ralgebra
  | Filter of 'f Ralgebra0.pred * ('f, 'l) ralgebra
  | EqJoin of 'f * 'f * ('f, 'l) ralgebra * ('f, 'l) ralgebra
  | Scan of 'l
  | Concat of ('f, 'l) ralgebra list
  | Count of ('f, 'l) ralgebra
  | Agg of 'f agg list * 'f list * ('f, 'l) ralgebra

type 'a lambda = string * 'a

type hash_idx = {
  lookup : Field.t Ralgebra0.pred;
}

type ordered_idx = {
  lookup_low : Field.t Ralgebra0.pred;
  lookup_high : Field.t Ralgebra0.pred;
  order : Field.t Ralgebra0.pred;
}

type t =
  | AEmpty
  | AScalar of Field.t Ralgebra0.pred
  | AList of (Relation.t, Field.t) ralgebra * t lambda
  | ATuple of [`Cross | `Zip] * t list lambda
  | AHashIdx of (Relation.t, Field.t) ralgebra * t list lambda * hash_idx
  | AOrderedIdx of (Relation.t, Field.t) ralgebra * t list lambda * ordered_idx

let rec pred_to_sql : string Ralgebra0.pred -> string = function
  | Var _ as x -> Error.create "Unsupported." x [%sexp_of:string Ralgebra0.pred] |> Error.raise
  | Field f -> sprintf "\"%s\"" f
  | Int x -> Int.to_string x
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> sprintf "'%S'" s
  | Binop (op, p1, p2) ->
    let s1 = sprintf "(%s)" (pred_to_sql p1) in
    let s2 = sprintf "(%s)" (pred_to_sql p2) in
    begin match op with
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
      | Mod -> sprintf "%s %% %s" s1 s2
    end
  | Varop (op, ps) ->
    let ss = List.map ps ~f:(fun p -> sprintf "(%s)" (pred_to_sql p)) in
    begin match op with
      | And -> String.concat ss ~sep:" and "
      | Or -> String.concat ss ~sep:" or "
      | _ -> failwith "Unsupported op."
    end

let ralgebra_to_sql : (Field.t, Relation.t) ralgebra -> string = fun r ->
  let fresh =
    let x = ref 0 in
    fun () -> incr x; !x
  in
  let rec f = function
    | Project ([], r) ->
      let table = fresh () in
      sprintf "select top 0 from (%s) as t%d" (f r) table
    | Project (fs, r) ->
      let table = fresh () in
      let fields =
        List.map fs ~f:(fun (f : Field.t) -> sprintf "t%d.%s" table f.name)
        |> String.concat ~sep:","
      in
      sprintf "select %s from (%s) as t%d" fields (f r) table
    | Scan r -> sprintf "select * from %s" r.name
    | Filter (pred, r) ->
      let table = fresh () in
      let pred =
        let f = object
          inherit ['f1, 'f2] Ralgebra0.pred_map as super
          method field (f : Field.t) = sprintf "t%d.%s" table f.name
        end in
        f#run pred
      in
      sprintf "select * from (%s) as t%d where %s" (f r) table (pred_to_sql pred)
    | EqJoin (f1, f2, r1, r2) ->
      let t1 = fresh () in
      let t2 = fresh () in
      sprintf "select * from (%s) as t%d, (%s) as t%d where t%d.%s = t%d.%s"
        (f r1) t1 (f r2) t2 t1 f1.name t2 f2.name
    | Count r -> sprintf "select count(*) from (%s)" (f r)
    | Agg (aggs, key, r) ->
      let table = fresh () in
      let aggs = List.map aggs ~f:(function
          | Count -> "count(*)"
          | Key f -> sprintf "t%d.%s" table f.name
          | Sum f -> sprintf "sum(t%d.%s)" table f.name
          | Avg f -> sprintf "avg(t%d.%s)" table f.name
          | Min f -> sprintf "min(t%d.%s)" table f.name
          | Max f -> sprintf "max(t%d.%s)" table f.name)
                 |> String.concat ~sep:", "
      in
      let key =
        List.map key ~f:(fun f -> sprintf "t%d.%s" table f.name)
        |> String.concat ~sep:", "
      in
      sprintf "select %s from (%s) as t%d group by (%s)" aggs (f r) table key
    | Concat _ -> failwith "Unsupported."
  in
  f r

let%expect_test "project-empty" =
  let r = Project ([], Scan (Relation.of_name "r")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select top 0 from (select * from r) as t1 |}]

let%expect_test "project" =
  let r = Project ([Field.of_name "f"], Scan (Relation.of_name "r")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select t1.f from (select * from r) as t1 |}]

let%expect_test "filter" =
  let r = Filter (Binop (Eq, Field (Field.of_name "f"), Field (Field.of_name "g")),
                  Scan (Relation.of_name "r")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select * from (select * from r) as t1 where ("t1.f") = ("t1.g") |}]

let%expect_test "eqjoin" =
  let r = EqJoin (Field.of_name "f", Field.of_name "g",
                  Scan (Relation.of_name "r"), Scan (Relation.of_name "s")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select * from (select * from r) as t1, (select * from s) as t2 where t1.f = t2.g |}]

let%expect_test "agg" =
  let f = Field.of_name "f" in
  let g = Field.of_name "g" in
  let r = Agg ([Sum g], [f; g], Scan (Relation.of_name "r")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select sum(t1.g) from (select * from r) as t1 group by (t1.f, t1.g) |}]

(* let eval_relation : Postgresql.connection -> Field.t list -> string -> Tuple.t list =
 *   fun conn fields query ->
 *     exec ~verbose:false conn query
 *     |> List.mapi ~f:(fun i vs ->
 *         let m_values = List.map2 vs fields ~f:(fun v f ->
 *             let pval = if String.(v = "") then `Null else
 *                 match f.dtype with
 *                 | DInt -> `Int (Int.of_string v)
 *                 | DString -> `String v
 *                 | DBool ->
 *                   begin match v with
 *                     | "t" -> `Bool true
 *                     | "f" -> `Bool false
 *                     | _ -> failwith "Unknown boolean value."
 *                   end
 *                 | _ -> `Unknown v
 *             in
 *             let value = Value.({ rel = Relation.dummy; field = f; value = pval }) in
 *             value)
 *         in
 *         match m_values with
 *         | Ok v -> v
 *         | Unequal_lengths ->
 *           Error.(create "Unexpected tuple width." (List.length fields, List.length vs)
 *                    [%sexp_of:int * int] |> raise))
 * 
 * let to_layout : Postgresql.connection -> t -> Layout.t = fun conn al ->
 *   let rec f tf = function
 *     | Tuple fs ->
 *       let query =
 *         List.map fs ~f:(fun f -> sprintf "%s.%s" f.relation.name f.name)
 *         |> String.concat ~sep:", "
 *         |> (fun fs_str -> sprintf "select %s from %s" fs_str
 *                (List.hd_exn fs).relation.name)
 *         |> tf
 *       in
 *       eval_relation conn fs query
 *       |> List.map ~f:(fun tup -> cross_tuple (List.map ~f:(fun v -> of_value v) tup))
 *       |> unordered_list
 *     | Table (value, ({ field = key; lookup } as t)) ->
 *       let key_query = sprintf "select %s from %s" key.name key.relation.name |> tf in
 *       let keys = eval_relation conn [key] key_query |> List.map ~f:(fun [x] -> x) in
 *       let map = List.map keys ~f:(fun key_value ->
 *           let value_tf q =
 *             sprintf "select * from (%s) as t where %s = %s"
 *               q key.name (Value.to_sql key_value)
 *             |> tf
 *           in
 *           key_value, f value_tf value)
 *                 |> Map.of_alist_exn (module ValueMap.Elem)
 *       in
 *       table map t
 *     | Grouping (value, ({ key; } as t)) ->
 *       let key_query =
 *         List.map key ~f:(fun f -> sprintf "%s.%s" f.relation.name f.name)
 *         |> String.concat ~sep:", "
 *         |> (fun fs_str -> sprintf "select %s from %s" fs_str
 *                (List.hd_exn key).relation.name)
 *         |> tf
 * 
 *         sprintf "select %s from %s" key.name key.relation.name |> tf in
 *       let keys = eval_relation conn [key] key_query |> List.map ~f:(fun [x] -> x) in
 *       let map = List.map keys ~f:(fun key_value ->
 *           let value_tf q =
 *             sprintf "select * from (%s) as t where %s = %s"
 *               q key.name (Value.to_sql key_value)
 *             |> tf
 *           in
 *           key_value, f value_tf value)
 *                 |> Map.of_alist_exn (module ValueMap.Elem)
 *       in
 *       table map t
 * 
 *   in
 *   f (fun x -> x) al
 * 
 * let partition : PredCtx.Key.t -> Field.t -> t -> t = fun k f l ->
 *   Table (l, { field = f; lookup = k }) *)
