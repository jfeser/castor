open Base
open Collections
open Db
open Layout

exception EvalError of Error.t

let raise e = raise (EvalError e)

module Config = struct
  module type S = sig
    val conn : Postgresql.connection
  end

  module type S_relation = sig
    type relation [@@deriving sexp, compare]

    val eval_relation : relation -> Tuple.t Seq.t
  end
end

module Make_relation (Config : Config.S_relation) = struct
  include Config

  type pred = Field.t Ralgebra0.pred

  type ralgebra = (Field.t, relation, Layout.t) Ralgebra0.t

  let rec eval_pred : PredCtx.t -> pred -> primvalue =
   fun ctx -> function
    | Null -> `Null
    | Int x -> `Int x
    | String x -> `String x
    | Bool x -> `Bool x
    | Var (n, _) -> (
      match PredCtx.find_var ctx n with
      | Some v -> v
      | None ->
          Error.create "Unbound variable." (n, ctx) [%sexp_of : string * PredCtx.t]
          |> raise )
    | Field f -> (
      match PredCtx.find_field ctx f with
      | Some v -> v
      | None ->
          Error.create "Unbound variable." (f, ctx) [%sexp_of : Field.t * PredCtx.t]
          |> raise )
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
              [%sexp_of : Ralgebra.op * primvalue * primvalue]
            |> raise )
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
              [%sexp_of : Ralgebra.op * primvalue list]
            |> raise

  let eval_aggregate :
      Tuple.t -> Field.t Ralgebra0.agg list -> Tuple.t Seq.t -> Tuple.t =
   fun key agg group ->
    List.map agg ~f:(function
      | Count -> Seq.length group |> Value.of_int_exn
      | Min f ->
          Seq.fold group ~init:Int.max_value ~f:(fun m t ->
              Int.min m (Tuple.field_exn t f |> Value.to_int_exn) )
          |> Value.of_int_exn
      | Max f ->
          Seq.fold group ~init:Int.min_value ~f:(fun m t ->
              Int.max m (Tuple.field_exn t f |> Value.to_int_exn) )
          |> Value.of_int_exn
      | Sum f ->
          Seq.fold group ~init:0 ~f:(fun m t ->
              m + (Tuple.field_exn t f |> Value.to_int_exn) )
          |> Value.of_int_exn
      | Key f -> Tuple.field_exn key f
      | Avg _ -> failwith "Unsupported." )

  let rec eval_layout : PredCtx.t -> Layout.t -> Tuple.t Seq.t =
   fun ctx l ->
    match l.node with
    | Int _ | Bool _ | String _ | Null _ -> Seq.singleton [Layout.to_value l]
    | CrossTuple ls ->
        List.fold_left ls ~init:(Seq.singleton []) ~f:(fun ts l ->
            Seq.cartesian_product ts (eval_layout ctx l)
            |> Seq.map ~f:(fun (ts, v) -> List.rev v @ ts) )
        |> Seq.map ~f:List.rev
    | ZipTuple ls ->
        List.map ls ~f:(eval_layout ctx)
        |> Seq.zip_many |> Seq.map ~f:Tuple.merge_many
    | UnorderedList ls | OrderedList (ls, _) ->
        Seq.concat_map ~f:(eval_layout ctx) (Seq.of_list ls)
    | Table (ls, {lookup= k; _}) -> (
      match Map.find ctx k with
      | Some v -> (
        match Map.find ls (ValueMap.Elem.of_primvalue v) with
        | Some l -> eval_layout ctx l
        | None -> Seq.empty )
      | None ->
          Error.create "Missing key." (k, ctx)
            [%sexp_of : PredCtx.Key.t * PredCtx.t]
          |> raise )
    | Grouping (ls, {output; _}) ->
        Seq.of_list ls
        |> Seq.map ~f:(fun (kl, vl) ->
               let kt = Option.value_exn (eval_layout ctx kl |> Seq.hd) in
               eval_aggregate kt output (eval_layout ctx vl) )
    | Empty -> Seq.empty

  let eval_count seq =
    let ct = Seq.length seq in
    Seq.singleton [Value.{value= `Int ct; rel= Relation.dummy; field= Field.dummy}]

  let eval_project fs seq =
    Seq.map seq ~f:(fun t ->
        List.filter t ~f:(fun v -> List.mem ~equal:Field.( = ) fs v.Value.field) )

  let eval_filter ctx p seq =
    Seq.filter seq ~f:(fun t ->
        let ctx = Map.merge_right ctx (PredCtx.of_tuple t) in
        match eval_pred ctx p with
        | `Bool x -> x
        | _ -> failwith "Expected a boolean." )

  module V = struct
    type t = {field: Field.t; value: primvalue} [@@deriving compare, hash, sexp]

    let of_value (v: Value.t) = {field= v.field; value= v.value}

    let to_value v : Value.t = {field= v.field; value= v.value; rel= Relation.dummy}
  end

  let eval_eqjoin f1 f2 s1 s2 =
    let tbl = Hashtbl.create (module V) in
    Seq.iter s1 ~f:(fun t ->
        let v = Tuple.field_exn t f1 in
        Hashtbl.add_multi tbl ~key:(V.of_value v) ~data:t ) ;
    Seq.concat_map s2 ~f:(fun t ->
        let v = Tuple.field_exn t f2 in
        match Hashtbl.find tbl (V.of_value v) with
        | Some ts -> List.map ts ~f:(fun t' -> t @ t') |> Seq.of_list
        | None -> Seq.empty )

  let eval_concat ss = Seq.of_list ss |> Seq.concat

  let eval_agg output_spec key_fields s =
    let module K = struct
      type t = V.t list [@@deriving compare, hash, sexp]
    end in
    let tbl = Hashtbl.create (module K) in
    Seq.iter s ~f:(fun t ->
        let key =
          List.map key_fields ~f:(fun f -> Tuple.field_exn t f |> V.of_value)
        in
        Hashtbl.add_multi tbl ~key ~data:t ) ;
    Hashtbl.mapi tbl ~f:(fun ~key ~data ->
        let open Ralgebra0 in
        List.map
          (output_spec : Field.t agg list)
          ~f:(function
            | Count -> List.length data |> Value.of_int_exn
            | Min f ->
                List.fold_left data ~init:Int.max_value ~f:(fun m t ->
                    Int.min m (Tuple.field_exn t f |> Value.to_int_exn) )
                |> Value.of_int_exn
            | Max f ->
                List.fold_left data ~init:Int.min_value ~f:(fun m t ->
                    Int.max m (Tuple.field_exn t f |> Value.to_int_exn) )
                |> Value.of_int_exn
            | Sum f ->
                List.fold_left data ~init:0 ~f:(fun m t ->
                    m + (Tuple.field_exn t f |> Value.to_int_exn) )
                |> Value.of_int_exn
            | Key f ->
                Option.value_exn (List.find key ~f:(fun v -> Field.(f = v.field)))
                |> V.to_value
            | Avg _ -> failwith "Unsupported.") )
    |> Hashtbl.to_alist |> Seq.of_list
    |> Seq.map ~f:(fun (_, v) -> v)

  let eval : PredCtx.t -> ralgebra -> Tuple.t Seq.t =
   fun ctx r ->
    let open Ralgebra0 in
    let rec eval = function
      | Scan l -> eval_layout ctx l
      | Count r -> eval_count (eval r)
      | Project (fs, r) -> eval_project fs (eval r)
      | Filter (p, r) -> eval_filter ctx p (eval r)
      | EqJoin (f1, f2, r1, r2) -> eval_eqjoin f1 f2 (eval r1) (eval r2)
      | Concat rs -> eval_concat (List.map ~f:eval rs)
      | Relation r -> eval_relation r
      | Agg (output, key, r) -> eval_agg output key (eval r)
    in
    eval r

  let eval_partial : ralgebra -> ralgebra =
    let open Ralgebra0 in
    let row_layout s =
      Seq.map s ~f:(fun tup -> cross_tuple (List.map ~f:(fun v -> of_value v) tup))
      |> Seq.to_list |> unordered_list
      |> fun l -> Scan l
    in
    let rec f = function
      | Project (fs, r) -> (
        match f r with
        | `Seq s -> `Seq (eval_project fs s)
        | `Ralgebra r -> `Ralgebra (Project (fs, r)) )
      | Agg (o, k, r) -> (
        match f r with
        | `Seq s -> `Seq (eval_agg o k s)
        | `Ralgebra r -> `Ralgebra (Agg (o, k, r)) )
      | Filter (p, r) -> (
        match f r with
        | `Seq s ->
            if Set.length (Ralgebra.pred_params p) = 0 then
              `Seq (eval_filter (Map.empty (module PredCtx.Key)) p s)
            else `Ralgebra (Filter (p, row_layout s))
        | `Ralgebra r -> `Ralgebra (Filter (p, r)) )
      | EqJoin (f1, f2, r1, r2) -> (
        match (f r1, f r2) with
        | `Seq s1, `Seq s2 -> `Seq (eval_eqjoin f1 f2 s1 s2)
        | `Seq s1, `Ralgebra r2 -> `Ralgebra (EqJoin (f1, f2, row_layout s1, r2))
        | `Ralgebra r1, `Seq s2 -> `Ralgebra (EqJoin (f1, f2, r1, row_layout s2))
        | `Ralgebra r1, `Ralgebra r2 -> `Ralgebra (EqJoin (f1, f2, r1, r2)) )
      | Concat rs ->
          let ss = List.map rs ~f in
          `Ralgebra
            (Concat
               (List.map ss ~f:(function
                 | `Seq s -> row_layout s
                 | `Ralgebra r -> r )))
      | Count r -> (
        match f r with
        | `Seq s -> `Seq (eval_count s)
        | `Ralgebra r -> `Ralgebra (Count r) )
      | Relation r -> `Seq (eval_relation r)
      | Scan _ as r -> `Ralgebra r
    in
    fun r -> match f r with `Seq s -> row_layout s | `Ralgebra r -> r
end

module Make (Config : Config.S) = struct
  include Make_relation (struct
    type relation = Relation.t [@@deriving compare, sexp]

    let eval_relation : relation -> Tuple.t Seq.t =
     fun r ->
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
                 |> raise )
  end)
end
