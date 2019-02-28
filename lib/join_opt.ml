open Base
open Castor
open Collections
module A = Abslayout

type t =
  | Flat of A.t
  | Hash of {lkey: A.pred; lhs: t; rkey: A.pred; rhs: t}
  | Nest of {lhs: t; rhs: t; pred: A.pred}
[@@deriving sexp_of]

type ctx = {sql: Sql.ctx; conn: Postgresql.connection; dbconn: Db.t}

let create_ctx sql conn dbconn = {sql; conn; dbconn}

let extract_joins r =
  (object (self : 'a)
     inherit [_] A.reduce

     inherit [_] Util.list_monoid

     method! visit_Join () p r1 r2 =
       let j =
         match A.pred_relations p with [r1; r2] -> [(r1, p, r2)] | _ -> []
       in
       (j @ self#visit_t () r1) @ self#visit_t () r2
  end)
    #visit_t () r

let rec to_ralgebra = function
  | Flat r -> r
  | Nest {lhs; rhs; pred} -> A.join pred (to_ralgebra lhs) (to_ralgebra rhs)
  | Hash {lkey; rkey; lhs; rhs} ->
      A.join (Binop (Eq, lkey, rkey)) (to_ralgebra lhs) (to_ralgebra rhs)

module Cost = struct
  let read = function
    | Type.PrimType.(IntT _ | DateT _ | FixedT _ | StringT _) -> 4.0
    | BoolT _ -> 1.0
    | _ -> failwith "Unexpected type."

  let hash = function
    | Type.PrimType.(IntT _ | DateT _ | FixedT _ | BoolT _) -> 1.0
    | StringT _ -> 100.0
    | _ -> failwith "Unexpected type."

  let size = function
    | Type.PrimType.(IntT _ | DateT _ | FixedT _) -> 4.0
    | StringT _ -> 25.0
    | BoolT _ -> 1.0
    | _ -> failwith "Unexpected type."
end

let ntuples ctx r =
  let module M = Abslayout_db.Make (struct
    let conn = ctx.dbconn
  end) in
  let r = to_ralgebra r in
  M.annotate_schema r ;
  (Explain.explain ctx.conn (Sql.of_ralgebra ctx.sql r |> Sql.to_string ctx.sql))
    .nrows |> Float.of_int

let schema ctx r =
  let module M = Abslayout_db.Make (struct
    let conn = ctx.dbconn
  end) in
  let r = to_ralgebra r in
  M.annotate_schema r ;
  Meta.(find_exn r schema)

let schema_types ctx r = schema ctx r |> List.map ~f:Name.type_exn

let rec estimate_cost ctx r =
  let sum = List.sum (module Float) in
  match r with
  | Flat _ ->
      let nt = ntuples ctx r in
      let scan_cost = sum (schema_types ctx r) ~f:Cost.read *. nt in
      let size_cost = sum (schema_types ctx r) ~f:Cost.size *. nt in
      [|size_cost; scan_cost|]
  | Nest {lhs; rhs; _} ->
      let nt = ntuples ctx r in
      let nt_lhs = ntuples ctx lhs in
      let nt_rhs = ntuples ctx lhs in
      let lhs_costs = estimate_cost ctx lhs in
      let lhs_size = lhs_costs.(0) in
      let lhs_scan = lhs_costs.(1) in
      let rhs_costs = estimate_cost ctx rhs in
      let rhs_size = rhs_costs.(0) in
      let rhs_scan = rhs_costs.(1) in
      let rhs_per_tuple_size = rhs_size /. nt_rhs in
      let rhs_per_tuple_scan = rhs_scan /. nt_rhs in
      let exp_factor = nt /. nt_lhs in
      let scan_cost = lhs_scan +. (exp_factor *. rhs_per_tuple_scan) in
      let size_cost = lhs_size +. (nt *. rhs_per_tuple_size) in
      [|size_cost; scan_cost|]
  | Hash {lkey; lhs; rhs; _} ->
      let nt = ntuples ctx r in
      let nt_lhs = ntuples ctx lhs in
      let nt_rhs = ntuples ctx lhs in
      let lhs_costs = estimate_cost ctx lhs in
      let lhs_size = lhs_costs.(0) in
      let lhs_scan = lhs_costs.(1) in
      let rhs_costs = estimate_cost ctx rhs in
      let rhs_size = rhs_costs.(0) in
      let rhs_scan = rhs_costs.(1) in
      let rhs_per_tuple_scan = rhs_scan /. nt_rhs in
      let exp_factor = nt /. nt_lhs in
      let scan_cost =
        lhs_scan
        +. (nt *. Cost.hash (Name.type_exn (A.pred_to_schema lkey)))
        +. (exp_factor *. rhs_per_tuple_scan)
      in
      let size_cost = lhs_size +. rhs_size in
      [|size_cost; scan_cost|]

module ParetoSet = struct
  type 'a t = (float array * 'a) list

  let dominates x y =
    assert (Array.length x = Array.length y) ;
    let n = Array.length x in
    let rec loop i le lt =
      if i = n then le && lt
      else
        loop (i + 1) Float.(le && x.(i) <= y.(i)) Float.(lt || x.(i) < y.(i))
    in
    loop 0 true false

  let rec add s c v =
    match s with
    | [] -> [(c, v)]
    | (c', v') :: s' ->
        if dominates c c' then add s' c v
        else if dominates c' c then s
        else (c', v') :: add s' c v

  let of_list l = List.fold_left l ~init:[] ~f:(fun s (c, v) -> add s c v)
end

let join_opt cost preds =
  let rec enumerate rels =
    let nrels = Set.length rels in
    let arels = Set.to_array rels in
    if nrels = 1 then
      let r = Flat (A.scan arels.(0)) in
      [(cost r, r)]
    else
      Seq.range 1 nrels
      |> Seq.concat_map ~f:(fun t -> Combinat.Poly.combinations t arels)
      |> Seq.to_list
      |> List.concat_map ~f:(fun lhs ->
             let open List.Let_syntax in
             let lhs = Set.of_array (module String) lhs in
             let rhs = Set.diff rels lhs in
             let%bind _, best_lhs = enumerate lhs in
             let%bind _, best_rhs = enumerate rhs in
             let join_preds =
               List.filter_map preds ~f:(fun (r1, p, r2) ->
                   if Set.mem lhs r1 && Set.mem rhs r2 then Some (r1, r2, p)
                   else if Set.mem lhs r2 && Set.mem rhs r1 then
                     Some (r2, r1, p)
                   else None )
             in
             let flat_joins =
               List.filter_map join_preds ~f:(fun (_, _, p) ->
                   match (best_lhs, best_rhs) with
                   | Flat lhs, Flat rhs -> Some (Flat (A.join p lhs rhs))
                   | _ -> None )
             in
             let nest_joins =
               List.map join_preds ~f:(fun (_, _, p) ->
                   Nest {lhs= best_lhs; rhs= best_rhs; pred= p} )
             in
             let hash_joins =
               List.filter_map join_preds ~f:(fun (lhs_r, rhs_r, p) ->
                   match p with
                   | Binop (Eq, k1, k2) ->
                       let lkey =
                         if
                           List.mem (A.pred_relations k1) ~equal:String.( = )
                             lhs_r
                         then k1
                         else k2
                       in
                       let rkey =
                         if
                           List.mem (A.pred_relations k1) ~equal:String.( = )
                             rhs_r
                         then k1
                         else k2
                       in
                       Some (Hash {lkey; rkey; lhs= best_lhs; rhs= best_rhs})
                   | _ -> None )
             in
             flat_joins @ nest_joins @ hash_joins
             |> List.map ~f:(fun r -> (cost r, r))
             |> ParetoSet.of_list )
      |> ParetoSet.of_list
  in
  let rels =
    List.concat_map preds ~f:(fun (r1, _, r2) -> [r1; r2])
    |> Set.of_list (module String)
  in
  enumerate rels
