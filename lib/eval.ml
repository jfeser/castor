open Core
open Base
open Collections
open Abslayout
open Db

module type S = Eval_intf.S

module Config = struct
  module type S = sig
    val conn : Db.t
  end

  module type S_mock = sig
    val rels : (string * Value.t) list list Hashtbl.M(Relation).t
  end
end

let lookup ctx n =
  match Map.find ctx n with
  | Some v -> v
  | None ->
      Error.create "Unbound variable." (n, ctx) [%sexp_of: Name.t * Ctx.t]
      |> Error.raise

let eval_pred_shared eval ctx =
  let open Value in
  let rec eval_pred ctx = function
    | As_pred (p, _) -> eval_pred ctx p
    | Null -> Null
    | Int x -> Int x
    | Fixed x -> Fixed x
    | Date x -> Int (Date.to_int x)
    | Unop (op, p) -> (
        let x = eval_pred ctx p in
        let to_date d = Date.(add_days unix_epoch d) in
        match op with
        | Not -> Bool (not (to_bool x))
        | Year -> Int (365 * to_int x)
        | Month -> Int (30 * to_int x)
        | Day -> x
        | Strlen -> Int (String.length (to_string x))
        | ExtractY -> Int (Date.year (to_date (to_int x)))
        | ExtractM -> Int (Month.to_int (Date.month (to_date (to_int x))))
        | ExtractD -> Int (Date.day (to_date (to_int x))) )
    | String x -> String x
    | Bool x -> Bool x
    | Name n -> lookup ctx n
    | Binop (op, p1, p2) -> (
        let v1 = eval_pred ctx p1 in
        let v2 = eval_pred ctx p2 in
        match (op, v1, v2) with
        | Eq, Null, _ | Eq, _, Null -> Bool false
        | Eq, Bool x1, Bool x2 -> Bool Bool.(x1 = x2)
        | Eq, Int x1, Int x2 -> Bool Int.(x1 = x2)
        | Eq, String x1, String x2 -> Bool String.(x1 = x2)
        | Eq, Fixed x1, Fixed x2 -> Bool Fixed_point.(x1 = x2)
        | Lt, Int x1, Int x2 -> Bool (x1 < x2)
        | Le, Int x1, Int x2 -> Bool (x1 <= x2)
        | Gt, Int x1, Int x2 -> Bool (x1 > x2)
        | Ge, Int x1, Int x2 -> Bool (x1 >= x2)
        | Add, Int x1, Int x2 -> Int (x1 + x2)
        | Sub, Int x1, Int x2 -> Int (x1 - x2)
        | Mul, Int x1, Int x2 -> Int (x1 * x2)
        | Div, Int x1, Int x2 -> Int (x1 / x2)
        | Mod, Int x1, Int x2 -> Int (x1 % x2)
        | Lt, Fixed x1, Fixed x2 -> Bool Fixed_point.(x1 < x2)
        | Le, Fixed x1, Fixed x2 -> Bool Fixed_point.(x1 <= x2)
        | Gt, Fixed x1, Fixed x2 -> Bool Fixed_point.(x1 > x2)
        | Ge, Fixed x1, Fixed x2 -> Bool Fixed_point.(x1 >= x2)
        | Add, Fixed x1, Fixed x2 -> Fixed Fixed_point.(x1 + x2)
        | Sub, Fixed x1, Fixed x2 -> Fixed Fixed_point.(x1 - x2)
        | Mul, Fixed x1, Fixed x2 -> Fixed Fixed_point.(x1 * x2)
        | And, Bool x1, Bool x2 -> Bool (x1 && x2)
        | Or, Bool x1, Bool x2 -> Bool (x1 || x2)
        | Strpos, String x1, String x2 -> (
          match String.substr_index x1 ~pattern:x2 with
          | Some pos -> Int (pos + 1)
          | None -> Int 0 )
        | _ ->
            Error.create "Unexpected argument types." (op, v1, v2)
              [%sexp_of: binop * t * t]
            |> Error.raise )
    | If (p1, p2, p3) ->
        let v1 = eval_pred ctx p1 |> to_bool in
        if v1 then eval_pred ctx p2 else eval_pred ctx p3
    | Exists r -> Bool (eval ctx r |> Gen.is_empty |> not)
    | First r -> (
      match eval ctx r |> Gen.take 2 |> Gen.to_list with
      | [tup] -> (
          (* Remove fields that are also present in the context to compensate
             for a bug elsewhere. *)
          let tup_list =
            Map.to_alist tup |> List.filter ~f:(fun (n, _) -> not (Map.mem ctx n))
          in
          match tup_list with
          | [(_, v)] -> v
          | _ ->
              Error.create "Expected a single valued tuple." (tup, ctx)
                [%sexp_of:
                  Value.t Map.M(Name.Compare_no_type).t
                  * Value.t Map.M(Name.Compare_no_type).t]
              |> Error.raise )
      | [] -> failwith "Empty relation."
      | _ -> failwith "Expected a one-element relation." )
    | Substring (p1, p2, p3) ->
        let s = eval_pred ctx p1 in
        let p = eval_pred ctx p2 in
        let c = eval_pred ctx p3 in
        String (String.sub (to_string s) ~pos:(to_int p - 1) ~len:(to_int c))
    | Count | Avg _ | Max _ | Min _ | Sum _ -> failwith "Unexpected aggregate."
  in
  eval_pred ctx

module Make (Config : Config.S) = struct
  let load_relation = Relation.from_db Config.conn

  let eval_relation r =
    let query = "select * from $0" in
    exec Config.conn query ~params:[r.Relation.rname]
    |> result_to_strings |> Gen.of_list
    |> Gen.map ~f:(fun vs ->
           let m_values =
             List.map2 vs r.fields ~f:(fun v f ->
                 let name = Name.create ~relation:r.rname f.fname in
                 let value =
                   if String.(v = "") then Value.Null
                   else
                     match f.type_ with
                     | IntT _ -> Int (Int.of_string v)
                     | StringT _ -> String v
                     | BoolT _ -> (
                       match v with
                       | "t" -> Bool true
                       | "f" -> Bool false
                       | _ -> failwith "Unknown boolean value." )
                     | FixedT _ -> Fixed (Fixed_point.of_string v)
                     | NullT | VoidT | TupleT _ ->
                         failwith "Not possible column types."
                 in
                 (name, value) )
           in
           match m_values with
           | Ok v -> v
           | Unequal_lengths ->
               Error.create "Unexpected tuple width."
                 (r, List.length r.fields, List.length vs)
                 [%sexp_of: Relation.t * int * int]
               |> Error.raise )

  let eval_with_schema schema sql =
    Db.exec_cursor Config.conn sql
    |> Db.to_tuples schema
    |> Gen.map ~f:(fun t ->
           match (Map.to_alist t, schema) with
           (* TODO: Special case compensates for First(r) where r has a single
           unnamed field. *)
           | [(_, v)], [n] -> Map.singleton (module Name.Compare_no_type) n v
           | _ ->
               List.map schema ~f:(fun n ->
                   match Map.find t n with
                   | Some v -> (n, v)
                   | None ->
                       Error.create "Mismatched tuple." (t, schema)
                         [%sexp_of:
                           Value.t Map.M(Name.Compare_no_type).t * Name.t list]
                       |> Error.raise )
               |> Map.of_alist_exn (module Name.Compare_no_type) )

  let rec eval_pred ctx p = eval_pred_shared eval ctx p

  and eval ctx query =
    let sql = Sql.ralgebra_to_sql (subst (Map.map ctx ~f:pred_of_value) query) in
    let schema = Meta.(find_exn query schema) in
    eval_with_schema schema sql |> Gen.map ~f:(Map.merge_right ctx)

  (** Evaluates query2 for each tuple produced by query1. Should be faster than
     evaluating query2 in a loop over the results of query1. *)

  (* let eval_foreach_flat ctx query1 query2 =
   *   let ctx = Map.map ctx ~f:pred_of_value in
   *   Logs.debug (fun m ->
   *       m "%a" Sexp.pp_hum ([%sexp_of: pred Map.M(Name.Compare_no_type).t] ctx) ) ;
   *   let query1 = subst ctx query1 in
   *   let query2 = subst ctx query2 in
   *   let sql = Sql.ralgebra_foreach query1 query2 in
   *   let schema = Meta.(find_exn query1 schema) @ Meta.(find_exn query2 schema) in
   *   eval_with_schema schema sql
   * 
   * let eval_foreach = eval_foreach eval_foreach_flat *)
end

module Make_mock (Config : Config.S_mock) = struct
  open Config

  let load_relation n =
    List.find_exn (Hashtbl.keys rels) ~f:(fun r -> String.(r.rname = n))

  let eval_relation r =
    Hashtbl.find_exn rels r |> Gen.of_list
    |> Gen.map ~f:(fun t ->
           List.map t ~f:(fun (n, v) -> (Name.create ~relation:r.rname n, v)) )

  let eval_join eval_pred ctx p r1 r2 =
    Gen.flat_map r1 ~f:(fun t1 ->
        Gen.filter_map r2 ~f:(fun t2 ->
            let ctx = ctx |> Map.merge_right t1 |> Map.merge_right t2 in
            match eval_pred ctx p with
            | Value.Bool true -> Some (Map.merge_right t1 t2)
            | Bool false -> None
            | _ -> failwith "Expected a boolean." ) )

  let eval_filter eval_pred ctx p seq =
    Gen.filter seq ~f:(fun t ->
        let ctx = Map.merge_right ctx t in
        match eval_pred ctx p with
        | Value.Bool x -> x
        | _ -> failwith "Expected a boolean." )

  let eval_dedup seq =
    Gen.fold ~init:(Set.empty (module Ctx)) seq ~f:Set.add
    |> Set.to_sequence |> Gen.of_sequence

  let agg_init = function
    | Count | Sum _ -> `Int 0
    | Avg _ -> `Avg (0, 0)
    | Min _ -> `Int Int.max_value
    | Max _ -> `Int Int.min_value
    | _ -> failwith "Not an aggregate."

  let agg_step eval_pred ctx expr acc =
    let open Value in
    match (expr, acc) with
    | Count, `Int x -> `Int (x + 1)
    | Sum n, `Int x -> `Int (x + (eval_pred ctx n |> to_int))
    | Avg n, `Avg (num, den) -> `Avg (num + (eval_pred ctx n |> to_int), den + 1)
    | Min n, `Int x -> `Int (Int.min x (eval_pred ctx n |> to_int))
    | Max n, `Int x -> `Int (Int.max x (eval_pred ctx n |> to_int))
    | _ -> failwith "Not an aggregate."

  let agg_extract = function `Int x -> Value.Int x | `Avg (n, d) -> Int (n / d)

  let eval_select eval_pred ctx args seq =
    let result =
      match select_kind args with
      | `Scalar ->
          Gen.map seq ~f:(fun t ->
              let ctx = Map.merge_right ctx t in
              List.map args ~f:(eval_pred ctx) )
      | `Agg ->
          Gen.fold seq ~init:(List.map args ~f:agg_init) ~f:(fun aggs t ->
              let ctx = Map.merge_right ctx t in
              List.map2_exn args aggs ~f:(agg_step eval_pred ctx) )
          |> List.map ~f:agg_extract |> Gen.singleton
    in
    Gen.map result ~f:(fun rs ->
        List.zip_exn args rs
        |> List.filter_map ~f:(fun (e, v) ->
               Option.map (pred_to_name e) ~f:(fun n -> (n, v)) )
        |> Map.of_alist_exn (module Name.Compare_no_type) )

  let eval_as rel_n seq =
    Gen.map seq ~f:(fun t ->
        Map.to_alist t
        |> List.map ~f:(fun (n, v) -> (Name.{n with relation= Some rel_n}, v))
        |> Map.of_alist_exn (module Name.Compare_no_type) )

  module Key = struct
    module T = struct
      type t = Value.t list [@@deriving compare, sexp]
    end

    include T
    include Comparable.Make (T)
  end

  let eval_orderby eval_pred ctx key order rel =
    let map =
      Gen.fold
        ~init:(Map.empty (module Key))
        ~f:(fun map t ->
          let ctx = Map.merge_right ctx t in
          let key_val = List.map key ~f:(eval_pred ctx) in
          Map.add_multi map ~key:key_val ~data:t )
        rel
    in
    let kv =
      match order with
      | `Asc -> Map.to_sequence ~order:`Increasing_key map |> Gen.of_sequence
      | `Desc -> Map.to_sequence ~order:`Decreasing_key map |> Gen.of_sequence
    in
    Gen.flat_map kv ~f:(fun (_, v) -> Gen.of_list v)

  let rec eval_pred ctx p = eval_pred_shared eval ctx p

  and eval ctx r =
    let rec eval {node; _} =
      match node with
      | Scan r ->
          eval_relation (load_relation r)
          |> Gen.map ~f:(Map.of_alist_exn (module Name.Compare_no_type))
      | Filter (p, r) -> eval_filter eval_pred ctx p (eval r)
      | Join {pred= p; r1; r2} -> eval_join eval_pred ctx p (eval r1) (eval r2)
      | Dedup r -> eval_dedup (eval r)
      | Select (out, r) -> eval_select eval_pred ctx out (eval r)
      | As (n, r) -> eval_as n (eval r)
      | OrderBy {key; order; rel} -> eval_orderby eval_pred ctx key order (eval rel)
      | r -> Error.create "Unsupported." r [%sexp_of: node] |> Error.raise
    in
    eval r |> Gen.map ~f:(Map.merge_right ctx)

  (* let eval_foreach_flat ctx q1 q2 =
   *   eval ctx q1
   *   |> Gen.concat_map ~f:(fun ctx -> Gen.map (eval ctx q2) ~f:(Map.merge_right ctx))
   * 
   * let eval_foreach = eval_foreach eval_foreach_flat *)
end
