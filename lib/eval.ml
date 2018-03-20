open Base
open Printf
open Collections
open Db
open Layout

exception EvalError of Error.t

let raise e = raise (EvalError e)

module Config = struct
  module type S = sig
    val conn : Postgresql.connection
  end
end

module Make (Config : Config.S) = struct
  open Config

  let rec eval_pred : PredCtx.t -> Ralgebra.pred -> primvalue =
    fun ctx -> function
      | Int x -> `Int x
      | String x -> `String x
      | Bool x -> `Bool x
      | Var (n, _) ->
        begin match PredCtx.find_var ctx n with
          | Some v -> v
          | None -> Error.create "Unbound variable." (n, ctx)
                      [%sexp_of:string * PredCtx.t] |> raise
        end
      | Field f ->
        begin match PredCtx.find_field ctx f with
          | Some v -> v
          | None -> Error.create "Unbound variable." (f, ctx)
                      [%sexp_of:Field.t * PredCtx.t] |> raise
        end
      | Binop (op, p1, p2) ->
        let v1 = eval_pred ctx p1 in
        let v2 = eval_pred ctx p2 in
        begin match op, v1, v2 with
          | Eq, `Bool x1, `Bool x2 -> Polymorphic_compare.(`Bool (x1 = x2))
          | Eq, `Int x1, `Int x2 -> Polymorphic_compare.(`Bool (x1 = x2))
          | Eq, `Null, `Int _ | Eq, `Int _, `Null -> `Bool false
          | Lt, `Int x1, `Int x2 -> `Bool (x1 < x2)
          | Le, `Int x1, `Int x2 -> `Bool (x1 <= x2)
          | Gt, `Int x1, `Int x2 -> `Bool (x1 > x2)
          | Ge, `Int x1, `Int x2 -> `Bool (x1 >= x2)
          | And, `Bool x1, `Bool x2 -> `Bool (x1 && x2)
          | Or, `Bool x1, `Bool x2 -> `Bool (x1 || x2)
          | _ -> Error.create "Unexpected argument types." (op, v1, v2)
                   [%sexp_of:Ralgebra.op * primvalue * primvalue]
                 |> raise
        end
      | Varop (op, ps) ->
        let vs = List.map ps ~f:(eval_pred ctx) in
        begin match op with
          | And ->
            List.for_all vs ~f:(function
                | `Bool x -> x
                | _ -> failwith "Unexpected argument type.")
            |> fun x -> `Bool x
          | Or ->
            List.exists vs ~f:(function
                | `Bool x -> x
                | _ -> failwith "Unexpected argument type.")
            |> fun x -> `Bool x
          | _ -> Error.create "Unexpected argument types." (op, vs)
                   [%sexp_of:Ralgebra.op * primvalue list] |> raise
        end

  let rec eval_layout : PredCtx.t -> t -> Tuple.t Seq.t =
    fun ctx l -> match l.node with
      | Int _ | Bool _ | String _ | Null _ -> Seq.singleton [Layout.to_value l]
      | CrossTuple ls ->
        List.fold_left ls ~init:(Seq.singleton []) ~f:(fun ts l ->
            Seq.cartesian_product ts (eval_layout ctx l)
            |> Seq.map ~f:(fun (ts, v) -> List.rev v @ ts))
        |> Seq.map ~f:List.rev
      | ZipTuple ls ->
        List.map ls ~f:(eval_layout ctx)
        |> Seq.zip_many
        |> Seq.map ~f:Tuple.merge_many
      | UnorderedList ls
      | OrderedList (ls, _) ->
        Seq.concat_map ~f:(eval_layout ctx) (Seq.of_list ls)
      | Table (ls, { lookup = k; field = f }) ->
        begin match Map.find ctx k with
          | Some v ->
            begin match Map.find ls (ValueMap.Elem.of_primvalue v) with
              | Some l -> eval_layout ctx l
              | None -> Seq.empty
            end
          | None -> Error.create "Missing key." (k, ctx)
                      [%sexp_of:(PredCtx.Key.t * PredCtx.t)] |> raise
        end
      | Empty -> Seq.empty

  let eval_relation : Relation.t -> Tuple.t Seq.t = fun r ->
    let query = "select * from $0" in
    exec ~verbose:false conn query ~params:[r.name]
    |> Seq.of_list
    |> Seq.mapi ~f:(fun i vs ->
        let m_values = List.map2 vs r.fields ~f:(fun v f ->
            let pval = if String.(v = "") then `Null else
                match f.Field.dtype with
                | DInt -> `Int (Int.of_string v)
                | DString -> `String v
                | DBool ->
                  begin match v with
                    | "t" -> `Bool true
                    | "f" -> `Bool false
                    | _ -> failwith "Unknown boolean value."
                  end
                | _ -> `Unknown v
            in
            let value = Value.({ rel = r; field = f; idx = i; value = pval }) in
            value)
        in
        match m_values with
        | Ok v -> v
        | Unequal_lengths ->
          Error.create "Unexpected tuple width."
            (r, List.length r.fields, List.length vs)
            [%sexp_of:Relation.t * int * int] |> raise)

  let eval : PredCtx.t -> Ralgebra.t -> Tuple.t Seq.t =
    fun ctx r ->
      let open Ralgebra0 in
      let rec eval = function
        | Scan l -> eval_layout ctx l
        | Count r ->
          let ct = Seq.length (eval r) in
          Seq.singleton [Value.({
              value = `Int ct; rel = Relation.dummy; field = Field.dummy; idx = 0
            })]
        | Project (fs, r) ->
          eval r |> Seq.map ~f:(fun t ->
              List.filter t ~f:(fun v ->
                  List.mem ~equal:Field.(=) fs v.Value.field))
        | Filter (p, r) ->
          eval r |> Seq.filter ~f:(fun t ->
              let ctx = Map.merge_right ctx (PredCtx.of_tuple t) in
              match eval_pred ctx p with
              | `Bool x -> x
              | _ -> failwith "Expected a boolean.")
        | EqJoin (f1, f2, r1, r2) ->
          let module V = struct
            type t = Value.t = {
              rel : Relation.t; [@compare.ignore]
              field : Field.t; [@compare.ignore]
              idx : int; [@compare.ignore]
              value : primvalue;
            } [@@deriving compare, hash, sexp]
          end in

          let tbl = Hashtbl.create (module V) () in
          eval r1 |> Seq.iter ~f:(fun t ->
              let v = Tuple.field_exn t f1 in
              Hashtbl.add_multi tbl ~key:v ~data:t);
          eval r2 |> Seq.concat_map ~f:(fun t ->
              let v = Tuple.field_exn t f2 in
              match Hashtbl.find tbl v with
              | Some ts -> List.map ts ~f:(fun t' -> t @ t') |> Seq.of_list
              | None -> Seq.empty)
        | Concat rs -> Seq.of_list rs |> Seq.concat_map ~f:eval
        | Relation r -> eval_relation r
      in
      eval r
end
