open Base
open Collections
open Abslayout
open Db

module type S = sig
  val load_relation : string -> Relation.t

  val eval_relation : Relation.t -> Tuple.t Seq.t

  val eval_pred : Ctx.t -> pred -> primvalue

  val eval : Ctx.t -> t -> Ctx.t Seq.t
end

module Config = struct
  module type S = sig
    val conn : Postgresql.connection
  end

  module type S_mock = sig
    val rels : (string * primvalue) list list Hashtbl.M(Relation).t
  end
end

let rec eval_pred ctx = function
  | Null -> `Null
  | Int x -> `Int x
  | String x -> `String x
  | Bool x -> `Bool x
  | Name n -> (
    match Map.find ctx n with
    | Some v -> v
    | None ->
        Error.create "Unbound variable." (n, ctx) [%sexp_of : Name.t * Ctx.t]
        |> Error.raise )
  | Binop (op, p1, p2) -> (
      let v1 = eval_pred ctx p1 in
      let v2 = eval_pred ctx p2 in
      match (op, v1, v2) with
      | Eq, `Null, _ | Eq, _, `Null -> `Bool false
      | Eq, `Bool x1, `Bool x2 -> `Bool Bool.(x1 = x2)
      | Eq, `Int x1, `Int x2 -> `Bool Int.(x1 = x2)
      | Eq, `String x1, `String x2 -> `Bool String.(x1 = x2)
      | Lt, `Int x1, `Int x2 -> `Bool (x1 < x2)
      | Le, `Int x1, `Int x2 -> `Bool (x1 <= x2)
      | Gt, `Int x1, `Int x2 -> `Bool (x1 > x2)
      | Ge, `Int x1, `Int x2 -> `Bool (x1 >= x2)
      | Add, `Int x1, `Int x2 -> `Int (x1 + x2)
      | Sub, `Int x1, `Int x2 -> `Int (x1 - x2)
      | Mul, `Int x1, `Int x2 -> `Int (x1 * x2)
      | Div, `Int x1, `Int x2 -> `Int (x1 / x2)
      | Mod, `Int x1, `Int x2 -> `Int (x1 % x2)
      | And, `Bool x1, `Bool x2 -> `Bool (x1 && x2)
      | Or, `Bool x1, `Bool x2 -> `Bool (x1 || x2)
      | _ ->
          Error.create "Unexpected argument types." (op, v1, v2)
            [%sexp_of : op * Db.primvalue * Db.primvalue]
          |> Error.raise )
  | Varop (op, ps) ->
      let vs = List.map ps ~f:(eval_pred ctx) in
      match op with
      | And ->
          List.for_all vs ~f:(function
            | `Bool x -> x
            | _ -> failwith "Unexpected argument type." )
          |> fun x -> `Bool x
      | Or ->
          List.exists vs ~f:(function
            | `Bool x -> x
            | _ -> failwith "Unexpected argument type." )
          |> fun x -> `Bool x
      | _ ->
          Error.create "Unexpected argument types." (op, vs)
            [%sexp_of : op * Db.primvalue list]
          |> Error.raise

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
                 let pval =
                   if String.(v = "") then `Null
                   else
                     match f.dtype with
                     | DInt -> `Int (Int.of_string v)
                     | DString -> `String v
                     | DBool -> (
                       match v with
                       | "t" -> `Bool true
                       | "f" -> `Bool false
                       | _ -> failwith "Unknown boolean value." )
                     | _ -> `Unknown v
                 in
                 let value = Value.{rel= r; field= f; value= pval} in
                 value )
           in
           match m_values with
           | Ok v -> v
           | Unequal_lengths ->
               Error.create "Unexpected tuple width."
                 (r, List.length r.fields, List.length vs)
                 [%sexp_of : Relation.t * int * int]
               |> Error.raise )

  let eval ctx query =
    let sql = ralgebra_to_sql (subst ctx query) in
    let schema = Meta.(find_exn query schema) in
    Db.exec_cursor Config.conn sql
    |> Seq.map ~f:(fun t ->
           List.map schema ~f:(fun n ->
               match Map.find t n.name with
               | Some v -> (n, v)
               | None ->
                   Error.create "Mismatched tuple." (t, schema)
                     [%sexp_of : Db.primvalue Map.M(String).t * Name.t list]
                   |> Error.raise )
           |> Map.of_alist_exn (module Name) )
end

module Make_mock (Config : Config.S_mock) : S = struct
  open Config

  let eval_pred = eval_pred

  let load_relation n =
    List.find_exn (Hashtbl.keys rels) ~f:(fun r -> String.(r.rname = n))

  let eval_relation r =
    Hashtbl.find_exn rels r |> Seq.of_list
    |> Seq.map ~f:(fun t ->
           List.map t ~f:(fun (n, v) ->
               Db.{rel= r; field= Field.of_name n; Value.value= v} ) )

  let eval_join ctx p r1 r2 =
    Seq.concat_map r1 ~f:(fun t1 ->
        Seq.filter r2 ~f:(fun t2 ->
            let ctx = ctx |> Map.merge_right t1 |> Map.merge_right t2 in
            match eval_pred ctx p with
            | `Bool x -> x
            | _ -> failwith "Expected a boolean." ) )

  let eval_filter ctx p seq =
    Seq.filter seq ~f:(fun t ->
        let ctx = Map.merge_right ctx t in
        match eval_pred ctx p with
        | `Bool x -> x
        | _ -> failwith "Expected a boolean." )

  let eval_dedup seq =
    let set = Hash_set.create (module Ctx) in
    Seq.iter seq ~f:(Hash_set.add set) ;
    Hash_set.to_list set |> Seq.of_list

  let eval_select ctx out seq =
    Seq.map seq ~f:(fun t ->
        let ctx = Map.merge_right ctx t in
        List.filter_map out ~f:(fun e ->
            Option.map (pred_to_name e) ~f:(fun n -> (n, eval_pred ctx e)) )
        |> Map.of_alist_exn (module Name) )

  let eval_as rel_n seq =
    Seq.map seq ~f:(fun t ->
        Map.to_alist t
        |> List.map ~f:(fun (n, v) -> (Name.{n with relation= Some rel_n}, v))
        |> Map.of_alist_exn (module Name) )

  module Key = struct
    module T = struct
      type t = primvalue list [@@deriving compare, sexp]
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
          |> Seq.map ~f:(fun t ->
                 List.map t ~f:(fun Value.({rel; field; value}) ->
                     (Name.create ~relation:rel.rname field.fname, value) )
                 |> Map.of_alist_exn (module Name) )
      | Filter (p, r) -> eval_filter ctx p (eval r)
      | Join {pred= p; r1; r2} -> eval_join ctx p (eval r1) (eval r2)
      | Dedup r -> eval_dedup (eval r)
      | Select (out, r) -> eval_select ctx out (eval r)
      | As (n, r) -> eval_as n (eval r)
      | OrderBy {key; order; rel} -> eval_orderby ctx key order (eval rel)
      | r -> Error.create "Unsupported." r [%sexp_of : node] |> Error.raise
    in
    eval r
end
