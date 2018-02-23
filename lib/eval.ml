open Base
open Collections
open Db
open Layout

module Ctx = struct
  type t = {
    mutable conn : Postgresql.connection option;
    mutable testctx : PredCtx.t option;
  }

  let global = { conn = None; testctx = None }

  let conn : unit -> Postgresql.connection = fun () ->
    Option.value_exn global.conn

  let testctx = fun () ->
    Option.value_exn global.testctx
end

exception EvalError of Error.t

let rec eval_pred : PredCtx.t -> Ralgebra.pred -> primvalue =
  fun ctx -> function
    | Int x -> `Int x
    | String x -> `String x
    | Bool x -> `Bool x
    | Var (n, _) ->
      begin match PredCtx.find_var ctx n with
        | Some v -> v
        | None -> failwith "Unbound variable."
      end
    | Field f ->
      begin match PredCtx.find_field ctx f with
        | Some v -> v
        | None -> failwith "Unbound variable."
      end
    | Binop (op, p1, p2) ->
      let v1 = eval_pred ctx p1 in
      let v2 = eval_pred ctx p2 in
      begin match op, v1, v2 with
        | Eq, `Bool x1, `Bool x2 -> Polymorphic_compare.(`Bool (x1 = x2))
        | Eq, `Int x1, `Int x2 -> Polymorphic_compare.(`Bool (x1 = x2))
        | Lt, `Int x1, `Int x2 -> `Bool (x1 < x2)
        | Le, `Int x1, `Int x2 -> `Bool (x1 <= x2)
        | Gt, `Int x1, `Int x2 -> `Bool (x1 > x2)
        | Ge, `Int x1, `Int x2 -> `Bool (x1 >= x2)
        | _ -> failwith "Unexpected argument types."
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
        | _ -> failwith "Unexpected argument types."
      end

let rec eval_layout : PredCtx.t -> t -> Tuple.t Seq.t =
  fun ctx -> function
    | Int _ | Bool _ | String _ as l -> Seq.singleton [Layout.to_value l]
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
        | None -> raise (EvalError (Error.create "Missing key." (k, ctx)
                                      [%sexp_of:(PredCtx.Key.t * PredCtx.t)]))
      end
    | Empty -> Seq.empty

let eval_relation : Relation.t -> Tuple.t Seq.t =
  fun r ->
    exec ~verbose:false (Ctx.conn ()) "select * from $0" ~params:[r.name]
    |> Seq.of_list
    |> Seq.mapi ~f:(fun i vs ->
        List.map2_exn vs r.fields ~f:(fun v f ->
            let pval =
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
            value))

let eval : PredCtx.t -> Ralgebra.t -> Tuple.t Seq.t =
  fun ctx r ->
    let open Ralgebra0 in
    let rec eval = function
      | Scan l -> eval_layout ctx l
      | Count r ->
        let ct = eval r |> Seq.length in
        Seq.singleton (Value.)
      | Project (fs, r) ->
        eval r
        |> Seq.map ~f:(fun t ->
            List.filter t ~f:(fun v ->
                List.mem ~equal:Field.(=) fs v.Value.field))
      | Filter (p, r) ->
        eval r
        |> Seq.filter ~f:(fun t ->
            let ctx = Map.merge_right ctx (PredCtx.of_tuple t) in
            match eval_pred ctx p with
            | `Bool x -> x
            | _ -> failwith "Expected a boolean.")
      | EqJoin (f1, f2, r1, r2) ->
        let m =
          eval r1
          |> Seq.fold ~init:(Map.empty (module Value)) ~f:(fun m t ->
              let v = Tuple.field_exn t f1 in
              Map.add_multi m ~key:v ~data:t)
        in
        eval r2
        |> Seq.concat_map ~f:(fun t ->
            let v = Tuple.field_exn t f2 in
            Option.value ~default:[] (Map.find m v)
            |> Seq.of_list)

      | Concat rs -> Seq.of_list rs |> Seq.concat_map ~f:eval
      | Relation r -> eval_relation r
    in
    eval r
