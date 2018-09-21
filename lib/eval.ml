open Base
open Collections
open Abslayout
open Db

module type S = Eval_intf.S

module Config = struct
  module type S = sig
    val conn : Postgresql.connection
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

let to_int = function Value.Int x -> x | _ -> failwith "Not an int."

let to_bool = function Value.Bool x -> x | _ -> failwith "Not a bool."

let rec eval_pred ctx = function
  | As_pred (p, _) -> eval_pred ctx p
  | Null -> Value.Null
  | Int x -> Int x
  | Fixed x -> Fixed x
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
      | _ ->
          Error.create "Unexpected argument types." (op, v1, v2)
            [%sexp_of: op * Value.t * Value.t]
          |> Error.raise )
  | If (p1, p2, p3) ->
      let v1 = eval_pred ctx p1 |> to_bool in
      if v1 then eval_pred ctx p2 else eval_pred ctx p3
  | Count | Avg _ | Max _ | Min _ | Sum _ -> failwith "Unexpected aggregate."

module Make (Config : Config.S) : S = struct
  let load_relation = Relation.from_db Config.conn

  let eval_pred = eval_pred

  let eval_relation r =
    let query = "select * from $0" in
    exec ~verbose:false Config.conn query ~params:[r.rname]
    |> Seq.of_list
    |> Seq.map ~f:(fun vs ->
           let m_values =
             List.map2 vs r.fields ~f:(fun v f ->
                 let name = Name.create ~relation:r.rname f.fname in
                 let value =
                   if String.(v = "") then Value.Null
                   else
                     match f.dtype with
                     | DInt -> Int (Int.of_string v)
                     | DString -> String v
                     | DBool -> (
                       match v with
                       | "t" -> Bool true
                       | "f" -> Bool false
                       | _ -> failwith "Unknown boolean value." )
                     | _ -> String v
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
    |> Seq.map ~f:(fun t ->
           List.map schema ~f:(fun n ->
               match Map.find t n.Name.name with
               | Some v -> (n, v)
               | None ->
                   Error.create "Mismatched tuple." (t, schema)
                     [%sexp_of: Value.t Map.M(String).t * Name.t list]
                   |> Error.raise )
           |> Map.of_alist_exn (module Name.Compare_no_type) )

  let eval ctx query =
    let sql = Sql.ralgebra_to_sql (subst (Map.map ctx ~f:pred_of_value) query) in
    let schema = Meta.(find_exn query schema) in
    eval_with_schema schema sql |> Seq.map ~f:(Map.merge_right ctx)

  (** Evaluates query2 for each tuple produced by query1. Should be faster than
     evaluating query2 in a loop over the results of query1. *)
  let eval_foreach_flat ctx query1 query2 =
    let ctx = Map.map ctx ~f:pred_of_value in
    let query1 = subst ctx query1 in
    let query2 = subst ctx query2 in
    let sql = Sql.ralgebra_foreach query1 query2 in
    let schema = Meta.(find_exn query1 schema) @ Meta.(find_exn query2 schema) in
    eval_with_schema schema sql

  let eval_foreach ctx query1 query2 =
    let extract_tup s t =
      List.map s ~f:(fun n -> (n, Map.find_exn t n))
      |> Map.of_alist_exn (module Name.Compare_no_type)
    in
    let outer_schema = Meta.(find_exn query1 schema) in
    let inner_schema = Meta.(find_exn query2 schema) in
    let inner_tuples outer_t tuples =
      Seq.take_while tuples ~f:(fun t ->
          [%compare.equal: Ctx.t] (extract_tup outer_schema t) outer_t )
      |> Seq.map ~f:(extract_tup inner_schema)
    in
    let tuples = eval_foreach_flat ctx query1 query2 |> Seq.memoize in
    match Seq.hd tuples with
    | None -> Seq.empty
    | Some t ->
        Seq.unfold_step ~init:(tuples, t) ~f:(function tuples, outer_t ->
            ( match Seq.next tuples with
            | Some (t, ts) ->
                let outer_t' = extract_tup outer_schema t in
                if [%compare.equal: Ctx.t] outer_t outer_t' then Skip (ts, outer_t)
                else Yield ((outer_t', inner_tuples outer_t' tuples), (ts, outer_t'))
            | None -> Done ) )
end

module Make_mock (Config : Config.S_mock) : S = struct
  open Config

  let eval_pred = eval_pred

  let load_relation n =
    List.find_exn (Hashtbl.keys rels) ~f:(fun r -> String.(r.rname = n))

  let eval_relation r =
    Hashtbl.find_exn rels r |> Seq.of_list
    |> Seq.map ~f:(fun t ->
           List.map t ~f:(fun (n, v) -> (Name.create ~relation:r.rname n, v)) )

  let eval_join ctx p r1 r2 =
    Seq.concat_map r1 ~f:(fun t1 ->
        Seq.filter_map r2 ~f:(fun t2 ->
            let ctx = ctx |> Map.merge_right t1 |> Map.merge_right t2 in
            match eval_pred ctx p with
            | Bool true -> Some (Map.merge_right t1 t2)
            | Bool false -> None
            | _ -> failwith "Expected a boolean." ) )

  let eval_filter ctx p seq =
    Seq.filter seq ~f:(fun t ->
        let ctx = Map.merge_right ctx t in
        match eval_pred ctx p with
        | Bool x -> x
        | _ -> failwith "Expected a boolean." )

  let eval_dedup seq =
    Seq.fold ~init:(Set.empty (module Ctx)) seq ~f:Set.add |> Set.to_sequence

  let agg_init = function
    | Count | Sum _ -> `Int 0
    | Avg _ -> `Avg (0, 0)
    | Min _ -> `Int Int.max_value
    | Max _ -> `Int Int.min_value
    | _ -> failwith "Not an aggregate."

  let agg_step ctx expr acc =
    match (expr, acc) with
    | Count, `Int x -> `Int (x + 1)
    | Sum n, `Int x -> `Int (x + (eval_pred ctx n |> to_int))
    | Avg n, `Avg (num, den) -> `Avg (num + (eval_pred ctx n |> to_int), den + 1)
    | Min n, `Int x -> `Int (Int.min x (eval_pred ctx n |> to_int))
    | Max n, `Int x -> `Int (Int.max x (eval_pred ctx n |> to_int))
    | _ -> failwith "Not an aggregate."

  let agg_extract = function `Int x -> Value.Int x | `Avg (n, d) -> Int (n / d)

  let eval_select ctx args seq =
    let result =
      match select_kind args |> Or_error.ok_exn with
      | `Scalar ->
          Seq.map seq ~f:(fun t ->
              let ctx = Map.merge_right ctx t in
              List.map args ~f:(eval_pred ctx) )
      | `Agg ->
          Seq.fold seq ~init:(List.map args ~f:agg_init) ~f:(fun aggs t ->
              let ctx = Map.merge_right ctx t in
              List.map2_exn args aggs ~f:(agg_step ctx) )
          |> List.map ~f:agg_extract |> Seq.singleton
    in
    Seq.map result ~f:(fun rs ->
        List.zip_exn args rs
        |> List.filter_map ~f:(fun (e, v) ->
               Option.map (pred_to_name e) ~f:(fun n -> (n, v)) )
        |> Map.of_alist_exn (module Name.Compare_no_type) )

  let eval_as rel_n seq =
    Seq.map seq ~f:(fun t ->
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

  let eval_orderby ctx key order rel =
    let map =
      Seq.fold
        ~init:(Map.empty (module Key))
        ~f:(fun map t ->
          let ctx = Map.merge_right ctx t in
          let key_val = List.map key ~f:(eval_pred ctx) in
          Map.add_multi map ~key:key_val ~data:t )
        rel
    in
    let kv =
      match order with
      | `Asc -> Map.to_sequence ~order:`Increasing_key map
      | `Desc -> Map.to_sequence ~order:`Decreasing_key map
    in
    Seq.concat_map kv ~f:(fun (_, v) -> Seq.of_list v)

  let eval ctx r =
    let rec eval {node; _} =
      match node with
      | Scan r ->
          eval_relation (load_relation r)
          |> Seq.map ~f:(Map.of_alist_exn (module Name.Compare_no_type))
      | Filter (p, r) -> eval_filter ctx p (eval r)
      | Join {pred= p; r1; r2} -> eval_join ctx p (eval r1) (eval r2)
      | Dedup r -> eval_dedup (eval r)
      | Select (out, r) -> eval_select ctx out (eval r)
      | As (n, r) -> eval_as n (eval r)
      | OrderBy {key; order; rel} -> eval_orderby ctx key order (eval rel)
      | r -> Error.create "Unsupported." r [%sexp_of: node] |> Error.raise
    in
    eval r |> Seq.map ~f:(Map.merge_right ctx)

  let eval_foreach ctx r1 r2 =
    eval ctx r1 |> Seq.map ~f:(fun ctx -> (ctx, eval ctx r2))
end
