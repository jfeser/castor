open Base
open Castor
open Collections
module A = Abslayout
open Graph
module Vertex = String

module JoinGraph = struct
  module G =
    Persistent.Graph.ConcreteLabeled
      (Vertex)
      (struct
        include A.Pred

        let default = A.Bool true
      end)

  include G
  include Oper.P (G)
  module Dfs = Traverse.Dfs (G)
  include Oper.Choose (G)

  let add_or_update_edge g ((v1, l, v2) as e) =
    try
      let _, l', _ = find_edge g v1 v2 in
      add_edge_e g (v1, Binop (And, l, l'), v2)
    with Caml.Not_found -> add_edge_e g e

  let vertices g = fold_vertex (fun v l -> v :: l) g []

  let partition g vs =
    let g1, g2 =
      fold_vertex
        (fun v (lhs, rhs) ->
          let in_set = Set.mem vs v in
          let lhs = if in_set then remove_vertex lhs v else lhs in
          let rhs = if in_set then rhs else remove_vertex rhs v in
          (lhs, rhs) )
        g (g, g)
    in
    let es =
      fold_edges_e
        (fun ((v1, _, v2) as e) es ->
          if
            (Set.mem vs v1 && not (Set.mem vs v2))
            || ((not (Set.mem vs v1)) && Set.mem vs v2)
          then e :: es
          else es )
        g []
    in
    (g1, g2, es)

  let is_connected g =
    let n = nb_vertex g in
    let n = Dfs.fold_component (fun _ i -> i - 1) n g (choose_vertex g) in
    n = 0
end

module JoinSpace = struct
  type t = {graph: JoinGraph.t; filters: Set.M(A.Pred).t}

  let empty = {graph= JoinGraph.empty; filters= Set.empty (module A.Pred)}

  let of_abslayout r =
    (object (self : 'a)
       inherit [_] A.reduce

       method zero = empty

       method plus s1 s2 : t =
         { graph= JoinGraph.union s1.graph s2.graph
         ; filters= Set.union s1.filters s2.filters }

       method! visit_Join () p r1 r2 =
         let s : t = self#plus (self#visit_t () r1) (self#visit_t () r2) in
         List.fold_left (A.Pred.conjuncts p) ~init:s ~f:(fun s p ->
             match A.pred_relations p with
             | [r1; r2] ->
                 { s with
                   graph= JoinGraph.add_or_update_edge s.graph (r1, p, r2) }
             | _ ->
                 Logs.warn (fun m ->
                     m "Join-opt: Unhandled predicate %a" A.pp_pred p ) ;
                 s )

       method! visit_Filter () (p, r) : t =
         let s = self#visit_t () r in
         {s with filters= Set.add s.filters p}
    end)
      #visit_t () r

  let partition_fold ~init ~f {graph; filters} =
    let vertices = JoinGraph.vertices graph |> Array.of_list in
    let n = Array.length vertices in
    let rec loop acc k =
      if k >= n then acc
      else
        let acc =
          Combinat.Combination.fold (n, k) ~init:acc ~f:(fun acc vs ->
              let g1, g2, es =
                JoinGraph.partition graph
                  ( List.init k ~f:(fun i -> vertices.(vs.{i}))
                  |> Set.of_list (module Vertex) )
              in
              if JoinGraph.is_connected g1 && JoinGraph.is_connected g2 then
                let s1 = {graph= g1; filters} in
                let s2 = {graph= g2; filters} in
                f acc (s1, s2, es)
              else acc )
        in
        loop acc (k + 1)
    in
    loop init 1
end

type t =
  | Flat of A.t
  | Hash of {lkey: A.pred; lhs: t; rkey: A.pred; rhs: t}
  | Nest of {lhs: t; rhs: t; pred: A.pred}
[@@deriving sexp_of]

type ctx = {sql: Sql.ctx; conn: Postgresql.connection; dbconn: Db.t}

let create_ctx sql conn dbconn = {sql; conn; dbconn}

(* let rec emit_joins = function
 *   | Flat r -> r
 *   | Hash {lkey; lhs; rkey; rhs} ->
 *       Option.value_exn
 *         (apply elim_join_hash Path.root
 *            (join (Binop (Eq, lkey, rkey)) (emit_joins lhs) (emit_joins rhs)))
 *   | Nest {lhs; rhs; pred} ->
 *       Option.value_exn
 *         (apply elim_join_nest Path.root
 *            (join pred (emit_joins lhs) (emit_joins rhs))) *)

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

  (* TODO: Not all lists have 16B headers *)
  let list_size = 16.0
end

let ntuples ctx r =
  let module M = Abslayout_db.Make (struct
    let conn = ctx.dbconn
  end) in
  let r = to_ralgebra r in
  M.annotate_schema r ;
  ( Explain.explain ctx.conn
      (Sql.of_ralgebra ctx.sql r |> Sql.to_string ctx.sql)
  |> Or_error.ok_exn )
    .nrows |> Float.of_int

let schema ctx r =
  let module M = Abslayout_db.Make (struct
    let conn = ctx.dbconn
  end) in
  let r = to_ralgebra r in
  M.annotate_schema r ;
  Meta.(find_exn r schema)

let schema_types ctx r = schema ctx r |> List.map ~f:Name.type_exn

let rec to_abslayout = function
  | Flat r -> r
  | Nest {lhs; rhs; pred} -> A.join pred (to_abslayout lhs) (to_abslayout rhs)
  | Hash {lkey; rkey; lhs; rhs} ->
      A.(join (Binop (And, lkey, rkey)) (to_abslayout lhs) (to_abslayout rhs))

let estimate_cost_parted ctx parts r =
  let module M = Abslayout_db.Make (struct
    let conn = ctx.dbconn
  end) in
  let s = schema ctx r |> Set.of_list (module Name) in
  let parts =
    List.filter parts ~f:(fun (_, _, ns, _) -> Set.is_subset ns ~of_:s)
  in
  let a = to_abslayout r in
  let all_tuples =
    List.fold_left parts ~init:a ~f:(fun j (d, _, _, p) -> A.join p d j)
  in
  let part_counts =
    A.(
      group_by
        [As_pred (Count, "c")]
        (List.concat_map parts ~f:(fun (_, ns, _, _) -> Set.to_list ns))
        all_tuples)
  in
  let part_aggs =
    let c = A.(Name (Name.create "c")) in
    A.(select [Min c; Max c; Avg c] part_counts)
  in
  M.annotate_schema part_aggs ;
  let sql = Sql.of_ralgebra ctx.sql part_aggs in
  let tups =
    Db.exec_cursor_exn ctx.dbconn
      Type.PrimType.
        [ IntT {nullable= false}
        ; IntT {nullable= false}
        ; FixedT {nullable= false} ]
      (Sql.to_string ctx.sql sql)
  in
  match Gen.next tups with
  | Some [|Int min; Int max; Fixed avg|] -> (min, max, Fixed_point.to_float avg)
  | _ -> failwith "Unexpected tuples."

let rec estimate_cost ctx parts r =
  let sum = List.sum (module Float) in
  match r with
  | Flat _ ->
      let _, _, nt = estimate_cost_parted ctx parts r in
      let scan_cost = sum (schema_types ctx r) ~f:Cost.read *. nt in
      let size_cost =
        (sum (schema_types ctx r) ~f:Cost.size *. nt) +. Cost.list_size
      in
      [|size_cost; scan_cost|]
  | Nest {lhs; rhs; pred} ->
      let _, _, lhs_nt = estimate_cost_parted ctx parts lhs in
      let lhs_costs = estimate_cost ctx parts lhs in
      let lhs_size = lhs_costs.(0) in
      let lhs_scan = lhs_costs.(1) in
      let rhs_per_partition_costs =
        let lhs_names, rhs_names =
          let lhs_schema = schema ctx lhs |> Set.of_list (module Name) in
          let rhs_schema = schema ctx rhs |> Set.of_list (module Name) in
          let pred_names =
            A.Pred.names pred
            |> Set.filter ~f:(fun n ->
                   Set.mem lhs_schema n || Set.mem rhs_schema n )
          in
          Set.partition_tf pred_names ~f:(Set.mem lhs_schema)
        in
        estimate_cost ctx
          ((to_ralgebra lhs, lhs_names, rhs_names, pred) :: parts)
          rhs
      in
      let size_cost = lhs_size +. (lhs_nt *. rhs_per_partition_costs.(0)) in
      let scan_cost = lhs_scan +. (lhs_nt *. rhs_per_partition_costs.(1)) in
      [|size_cost; scan_cost|]
  | Hash {lkey; lhs; rhs; rkey} ->
      let _, _, nt_lhs = estimate_cost_parted ctx parts lhs in
      let lhs_costs = estimate_cost ctx parts lhs in
      let lhs_size = lhs_costs.(0) in
      let lhs_scan = lhs_costs.(1) in
      let rhs_costs = estimate_cost ctx parts lhs in
      let rhs_size = rhs_costs.(0) in
      let rhs_per_partition_costs =
        let pred = A.(Binop (Eq, lkey, rkey)) in
        let lhs_names, rhs_names =
          let lhs_schema = schema ctx lhs |> Set.of_list (module Name) in
          let rhs_schema = schema ctx rhs |> Set.of_list (module Name) in
          let pred_names =
            A.(Pred.names pred)
            |> Set.filter ~f:(fun n ->
                   Set.mem lhs_schema n || Set.mem rhs_schema n )
          in
          Set.partition_tf pred_names ~f:(Set.mem lhs_schema)
        in
        estimate_cost ctx
          ((to_ralgebra lhs, lhs_names, rhs_names, pred) :: parts)
          rhs
      in
      let size_cost = lhs_size +. rhs_size in
      let scan_cost =
        lhs_scan
        +. nt_lhs
           *. ( Cost.hash (Name.type_exn (A.pred_to_schema lkey))
              +. rhs_per_partition_costs.(1) )
      in
      [|size_cost; scan_cost|]

module ParetoSet = struct
  type 'a t = (float array * 'a) list

  let empty = []

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
      List.range 1 nrels
      |> List.concat_map ~f:(fun t ->
             Combinat.Combination.to_list (t, nrels)
             |> List.map ~f:(fun c -> Array.init t ~f:(fun i -> arels.(c.{i})))
         )
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
