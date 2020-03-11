open Graph
open Printf
open Collections
open Ast
module A = Abslayout
module P = Pred.Infix

module Config = struct
  module type My_S = sig
    val cost_conn : Db.t

    val params : Set.M(Name).t
  end

  module type S = sig
    include Ops.Config.S

    include Filter_tactics.Config.S

    include Simple_tactics.Config.S

    include My_S
  end
end

module Make (C : Config.S) = struct
  module My_C : Config.My_S = C

  open My_C

  open Ops.Make (C)

  open Simple_tactics.Make (C)

  module R = Resolve

  module JoinGraph = struct
    module Vertex = struct
      module T = struct
        type t = Ast.t [@@deriving compare, hash, sexp_of]
      end

      include T
      include Comparator.Make (T)

      let equal = [%compare.equal: t]
    end

    module Edge = struct
      include Pred

      let default = Bool true
    end

    module G = Persistent.Graph.ConcreteLabeled (Vertex) (Edge)
    include G
    include Oper.P (G)
    module Dfs = Traverse.Dfs (G)
    include Oper.Choose (G)

    let to_string g =
      sprintf "graph (|V|=%d) (|E|=%d)" (nb_vertex g) (nb_edges g)

    let sexp_of_t g =
      fold_edges_e (fun e l -> e :: l) g []
      |> [%sexp_of: (Vertex.t * Edge.t * Vertex.t) list]

    let compare g1 g2 = Sexp.compare ([%sexp_of: t] g1) ([%sexp_of: t] g2)

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
            (lhs, rhs))
          g (g, g)
      in
      let es =
        fold_edges_e
          (fun ((v1, _, v2) as e) es ->
            if
              (Set.mem vs v1 && not (Set.mem vs v2))
              || ((not (Set.mem vs v1)) && Set.mem vs v2)
            then e :: es
            else es)
          g []
      in
      (g1, g2, es)

    let is_connected g =
      let n = nb_vertex g in
      let n = Dfs.fold_component (fun _ i -> i - 1) n g (choose_vertex g) in
      n = 0
  end

  let source_relation leaves n =
    List.find_map leaves ~f:(fun (r, s) -> if Set.mem s n then Some r else None)
    |> Result.of_option
         ~error:
           Error.(
             create "No source found for name."
               (n, List.map leaves ~f:(fun (_, ns) -> ns))
               [%sexp_of: Name.t * Set.M(Name).t list])

  module JoinSpace = struct
    module T = struct
      type t = {
        graph : JoinGraph.t;
        filters : Set.M(Pred).t Map.M(JoinGraph.Vertex).t;
      }
      [@@deriving compare, sexp_of]

      let t_of_sexp _ = failwith "unimplemented"
    end

    include T
    module C = Comparable.Make (T)

    module O : Comparable.Infix with type t := t = C

    let to_string { graph; _ } = JoinGraph.to_string graph

    let empty =
      { graph = JoinGraph.empty; filters = Map.empty (module JoinGraph.Vertex) }

    let union s1 s2 =
      let merger ~key:_ = function
        | `Left x | `Right x -> Some x
        | `Both (x, y) -> Some (Set.union x y)
      in
      {
        graph = JoinGraph.union s1.graph s2.graph;
        filters = Map.merge ~f:merger s1.filters s2.filters;
      }

    let length { graph; _ } = JoinGraph.nb_vertex graph

    let choose { graph; _ } = JoinGraph.choose_vertex graph

    let contract join g =
      let open JoinGraph in
      (* if the edge is to be removed (property = true):
         * make a union of the two union-sets of start and end node;
         * put this set in the map for all nodes in this set *)
      let f edge m =
        let s_src, j_src = Map.find_exn m (E.src edge) in
        let s_dst, j_dst = Map.find_exn m (E.dst edge) in
        let s = Set.union s_src s_dst in
        let j = join ~label:(G.E.label edge) j_src j_dst in
        Set.fold
          ~f:(fun m vertex -> Map.set m ~key:vertex ~data:(s, j))
          s ~init:m
      in
      (* initialize map with singleton-sets for every node (of itself) *)
      let m =
        G.fold_vertex
          (fun vertex m ->
            Map.set m ~key:vertex
              ~data:(Set.singleton (module Vertex) vertex, vertex))
          g
          (Map.empty (module Vertex))
      in
      G.fold_edges_e f g m |> Map.data |> List.hd_exn |> fun (_, j) -> j

    let to_ralgebra { graph; _ } =
      if JoinGraph.nb_vertex graph = 1 then JoinGraph.choose_vertex graph
      else contract (fun ~label:p j1 j2 -> A.join p j1 j2) graph

    (** Collect the leaves of the join tree rooted at r. *)
    let rec to_leaves r =
      let open JoinGraph in
      match r.node with
      | Join { r1; r2; _ } -> Set.union (to_leaves r1) (to_leaves r2)
      | _ -> Set.singleton (module Vertex) r

    (** Convert a join tree to a join graph. *)
    let rec to_graph leaves r =
      match r.node with
      | Join { r1; r2; pred = p } ->
          let s = union (to_graph leaves r1) (to_graph leaves r2) in
          (* Collect the set of relations that this join depends on. *)
          List.fold_left (Pred.conjuncts p) ~init:s ~f:(fun s p ->
              let pred_rels =
                List.map
                  (Pred.names p |> Set.to_list)
                  ~f:(source_relation leaves)
                |> Or_error.all
              in
              match pred_rels with
              | Ok [] ->
                  Logs.warn (fun m ->
                      m "Join-opt: Unhandled predicate %a. Constant predicate."
                        A.pp_pred p);
                  s
              | Ok [ r ] ->
                  {
                    s with
                    filters =
                      Map.update s.filters r ~f:(function
                        | Some fs -> Set.add fs p
                        | None -> Set.singleton (module Pred) p);
                  }
              | Ok [ r1; r2 ] ->
                  {
                    s with
                    graph = JoinGraph.add_or_update_edge s.graph (r1, p, r2);
                  }
              | Ok _ ->
                  Logs.warn (fun m ->
                      m "Join-opt: Unhandled predicate %a. Too many relations."
                        A.pp_pred p);
                  s
              | Error e ->
                  Logs.warn (fun m ->
                      m "Join opt: Unhandled predicate %a. %a" A.pp_pred p
                        Error.pp e);
                  s)
      | _ -> empty

    let of_abslayout r =
      Logs.debug (fun m -> m "Join-opt: Planning join for %a." A.pp r);
      let leaves =
        to_leaves r |> Set.to_list
        |> List.map ~f:(fun r ->
               let s = Schema.schema r |> Set.of_list (module Name) in
               (r, s))
      in
      to_graph leaves r

    let partition_fold ~init ~f s =
      let vertices = JoinGraph.vertices s.graph |> Array.of_list in
      let n = Array.length vertices in
      let rec loop acc k =
        if k >= n then acc
        else
          let acc =
            let open Combinat.Combination in
            create ~n ~k
            |> fold ~init:acc ~f:(fun acc vs ->
                   let g1, g2, es =
                     JoinGraph.partition s.graph
                       ( List.init k ~f:(fun i -> vertices.(vs.{i}))
                       |> Set.of_list (module JoinGraph.Vertex) )
                   in
                   if JoinGraph.is_connected g1 && JoinGraph.is_connected g2
                   then
                     let s1 = { s with graph = g1 } in
                     let s2 = { s with graph = g2 } in
                     f acc (s1, s2, es)
                   else acc)
          in
          loop acc (k + 1)
      in
      loop init 1
  end

  type t =
    | Flat of Ast.t
    | Hash of { lkey : Pred.t; lhs : t; rkey : Pred.t; rhs : t }
    | Nest of { lhs : t; rhs : t; pred : Pred.t }
    | Id of Ast.t
  [@@deriving sexp_of]

  let rec to_ralgebra = function
    | Flat r | Id r -> r
    | Nest { lhs; rhs; pred } -> A.join pred (to_ralgebra lhs) (to_ralgebra rhs)
    | Hash { lkey; rkey; lhs; rhs } ->
        A.join (Binop (Eq, lkey, rkey)) (to_ralgebra lhs) (to_ralgebra rhs)

  module Cost = struct
    let read = function
      | Prim_type.(IntT _ | DateT _ | FixedT _ | StringT _) -> 4.0
      | BoolT _ -> 1.0
      | _ -> failwith "Unexpected type."

    let hash = function
      | Prim_type.(IntT _ | DateT _ | FixedT _ | BoolT _) -> 1.0
      | StringT _ -> 100.0
      | _ -> failwith "Unexpected type."

    let size = function
      | Prim_type.(IntT _ | DateT _ | FixedT _) -> 4.0
      | StringT _ -> 25.0
      | BoolT _ -> 1.0
      | _ -> failwith "Unexpected type."

    (* TODO: Not all lists have 16B headers *)
    let list_size = 16.0
  end

  let ntuples r =
    let r = to_ralgebra r in
    ( Explain.explain cost_conn (Sql.of_ralgebra r |> Sql.to_string)
    |> Or_error.ok_exn )
      .nrows |> Float.of_int

  let estimate_ntuples_parted parts join =
    let r = to_ralgebra join in
    let s = Schema.schema r |> Set.of_list (module Name) in

    (* Remove parameters from the join nest where possible. *)
    let static_r =
      let open Abslayout_visitors in
      let rec annot r = map_annot query r
      and query q = map_query annot pred q
      and pred p = Pred.to_static ~params p in
      annot r
    in

    (* Generate a group-by using the partition fields. *)
    let parts = Set.filter parts ~f:(Set.mem s) in
    let parted_r =
      let c = P.name (Name.create "c") in
      A.(
        select [ Min c; Max c; Avg c ]
        @@ group_by [ P.as_ Count "c" ] (Set.to_list parts) static_r)
    in
    let tups =
      let sql = Sql.of_ralgebra parted_r |> Sql.to_string
      and schema = Prim_type.[ int_t; int_t; fixed_t ] in
      Db.exec_exn cost_conn schema sql
    in

    match tups with
    | [| Int min; Int max; Fixed avg |] :: _ ->
        (min, max, Fixed_point.to_float avg)
    | _ -> failwith "Unexpected tuples."

  let to_parts rhs pred =
    let rhs_schema = Schema.schema rhs |> Set.of_list (module Name) in
    Pred.names pred |> Set.filter ~f:(Set.mem rhs_schema)

  let rec size_cost parts r =
    let sum = List.sum (module Float) in
    match r with
    | Flat _ | Id _ ->
        let _, _, nt = estimate_ntuples_parted parts r in
        (sum (Schema.types (to_ralgebra r)) ~f:Cost.size *. nt)
        +. Cost.list_size
    | Nest { lhs; rhs; pred } ->
        let _, _, lhs_nt = estimate_ntuples_parted parts lhs in
        let rhs_per_partition_cost =
          size_cost (Set.union (to_parts (to_ralgebra rhs) pred) parts) rhs
        in
        size_cost parts lhs +. (lhs_nt *. rhs_per_partition_cost)
    | Hash { lhs; rhs; _ } -> size_cost parts lhs +. size_cost parts rhs

  let rec scan_cost parts r =
    let sum = List.sum (module Float) in
    match r with
    | Flat _ | Id _ ->
        let _, _, nt = estimate_ntuples_parted parts r in
        sum (Schema.types (to_ralgebra r)) ~f:Cost.read *. nt
    | Nest { lhs; rhs; pred } ->
        let _, _, lhs_nt = estimate_ntuples_parted parts lhs in
        let rhs_per_partition_cost =
          scan_cost (Set.union (to_parts (to_ralgebra rhs) pred) parts) rhs
        in
        scan_cost parts lhs +. (lhs_nt *. rhs_per_partition_cost)
    | Hash { lkey; lhs; rhs; rkey } ->
        let _, _, nt_lhs = estimate_ntuples_parted parts lhs in
        let rhs_per_partition_cost =
          let pred = Pred.Infix.(lkey = rkey) in
          scan_cost (Set.union (to_parts (to_ralgebra rhs) pred) parts) rhs
        in
        scan_cost parts lhs
        +. (nt_lhs *. (Cost.hash (Pred.to_type lkey) +. rhs_per_partition_cost))

  module ParetoSet = struct
    type 'a t = (float array * 'a) list

    let empty = []

    let singleton c v = [ (c, v) ]

    let dominates x y =
      assert (Array.length x = Array.length y);
      let n = Array.length x in
      let rec loop i le lt =
        if i = n then le && lt
        else
          loop (i + 1) Float.(le && x.(i) <= y.(i)) Float.(lt || x.(i) < y.(i))
      in
      loop 0 true false

    let rec add s c v =
      match s with
      | [] -> [ (c, v) ]
      | (c', v') :: s' ->
          if Array.equal Float.( = ) c c' || dominates c' c then s
          else if dominates c c' then add s' c v
          else (c', v') :: add s' c v

    let min_elt f s =
      List.map s ~f:(fun (c, x) -> (f c, x))
      |> List.min_elt ~compare:(fun (c1, _) (c2, _) -> Float.compare c1 c2)
      |> Option.map ~f:(fun (_, x) -> x)

    let of_list l = List.fold_left l ~init:[] ~f:(fun s (c, v) -> add s c v)

    let length = List.length

    let union_all ss = List.concat ss |> of_list
  end

  let leaf_flat r =
    let open Option.Let_syntax in
    if Set.is_empty @@ Set.inter (A.free r) params then return @@ Flat r
    else (
      Logs.info (fun m -> m "Flat join does not apply to@ %a." A.pp r);
      None )

  let leaf_id r =
    let open Option.Let_syntax in
    return @@ Id r

  let enum_flat_join opt parts pred s1 s2 =
    let select_flat s =
      List.filter_map (opt parts s) ~f:(fun (_, j) ->
          match j with Flat r -> Some r | _ -> None)
    in
    List.cartesian_product (select_flat s1) (select_flat s2)
    |> List.map ~f:(fun (r1, r2) ->
           let j = Flat (A.join pred r1 r2) in
           ([| scan_cost parts j |], j))

  let enum_hash_join opt parts pred s1 s2 =
    let open List.Let_syntax in
    let lhs = JoinSpace.to_ralgebra s1 and rhs = JoinSpace.to_ralgebra s2 in
    let lhs_schema = Schema.schema lhs and rhs_schema = Schema.schema rhs in
    Log.info (fun m ->
        m "LHS schema %a, RHS schema %a"
          Fmt.Dump.(list Name.pp)
          lhs_schema
          Fmt.Dump.(list Name.pp)
          rhs_schema);
    (* Figure out which partition a key comes from. *)
    let key_side k =
      let rs = Pred.names k in
      let all_in s =
        Set.for_all rs ~f:(List.mem ~equal:[%compare.equal: Name.t] s)
      in
      if all_in lhs_schema then return (`Lhs (s1, k))
      else if all_in rhs_schema then return (`Rhs (s2, k))
      else (
        Log.info (fun m -> m "Unknown key %a" Pred.pp k);
        [] )
    in
    let%bind k1, k2 =
      match pred with
      | Binop (Eq, k1, k2) -> return (k1, k2)
      | _ ->
          Log.info (fun m -> m "Adding hash join failed.");
          []
    in
    let%bind s1 = key_side k1 in
    let%bind s2 = key_side k2 in
    let joins =
      match (s1, s2) with
      | `Lhs (s1, k1), `Rhs (s2, k2) | `Rhs (s2, k2), `Lhs (s1, k1) ->
          let rhs_parts = Set.union (to_parts rhs pred) parts in
          List.cartesian_product (opt parts s1) (opt rhs_parts s2)
          |> List.map ~f:(fun ((_, r1), (_, r2)) ->
                 Hash { lkey = k1; rkey = k2; lhs = r1; rhs = r2 })
      | _ ->
          Logs.info (fun m ->
              m "Keys come from same partition %a %a" Pred.pp k1 Pred.pp k2);
          []
    in
    List.map joins ~f:(fun j -> ([| scan_cost parts j |], j))

  let enum_nest_join opt parts pred s1 s2 =
    let lhs_parts =
      Set.union (to_parts (JoinSpace.to_ralgebra s1) pred) parts
    in
    let rhs_parts =
      Set.union (to_parts (JoinSpace.to_ralgebra s2) pred) parts
    in
    let lhs_set = List.map (opt lhs_parts s1) ~f:(fun (_, j) -> j) in
    let rhs_set = List.map (opt rhs_parts s2) ~f:(fun (_, j) -> j) in
    List.cartesian_product lhs_set rhs_set
    |> List.map ~f:(fun (j1, j2) ->
           let j = Nest { lhs = j1; rhs = j2; pred } in
           ([| scan_cost parts j |], j))

  let opt_nonrec opt parts s =
    Logs.info (fun m -> m "Choosing join for space %s." (JoinSpace.to_string s));
    Logs.info (fun m -> m "%a" Sexp.pp_hum ([%sexp_of: JoinSpace.t] s));
    let joins =
      if JoinSpace.length s = 1 then
        (* Select strategy for the leaves of the join tree. *)
        let r = JoinSpace.choose s in
        [ leaf_flat r; leaf_id r ] |> List.filter_map ~f:Fun.id
        |> List.map ~f:(fun j -> ([| scan_cost parts j |], j))
        |> ParetoSet.of_list
      else
        JoinSpace.partition_fold s ~init:ParetoSet.empty
          ~f:(fun cs (s1, s2, es) ->
            let pred = Pred.conjoin (List.map es ~f:(fun (_, p, _) -> p)) in
            let _nest_joins = enum_nest_join opt parts pred s1 s2
            and flat_joins = enum_flat_join opt parts pred s1 s2
            and hash_joins = enum_hash_join opt parts pred s1 s2 in
            ParetoSet.(union_all [ cs; of_list (flat_joins @ hash_joins) ]))
    in
    Logs.info (fun m ->
        m "Found %d pareto-optimal joins." (ParetoSet.length joins));
    joins

  module Key = struct
    type t = Set.M(Name).t * Set.M(JoinGraph.Vertex).t
    [@@deriving compare, hash, sexp_of]

    let create p s =
      ( p,
        JoinGraph.fold_vertex
          (fun v vs -> Set.add vs v)
          s.JoinSpace.graph
          (Set.empty (module JoinGraph.Vertex)) )
  end

  let opt =
    let tbl = Hashtbl.create (module Key) in
    let rec opt p s =
      let key = Key.create p s in
      match Hashtbl.find tbl key with
      | Some v -> v
      | None ->
          let v = opt_nonrec opt p s in
          Hashtbl.add_exn tbl ~key ~data:v;
          v
    in
    opt

  let opt r = opt (Set.empty (module Name)) (JoinSpace.of_abslayout r)

  let reshape j _ = Some (to_ralgebra j)

  let rec emit_joins =
    let module J = Join_elim_tactics.Make (C) in
    let open J in
    function
    | Flat _ -> row_store
    | Id _ -> id
    | Hash { lhs; rhs; _ } ->
        seq_many
          [
            at_ (emit_joins lhs) (child 0);
            at_ (emit_joins rhs) (child 1);
            elim_join_hash;
          ]
    | Nest { lhs; rhs; _ } ->
        seq_many
          [
            at_ (emit_joins lhs) (child 0);
            at_ (emit_joins rhs) (child 1);
            elim_join_nest;
          ]

  let transform =
    let open Option.Let_syntax in
    let f p r =
      let joins = opt (Castor.Path.get_exn p r) in
      Log.info (fun m -> m "Found %d join options." (ParetoSet.length joins));
      let%bind j = ParetoSet.min_elt (fun a -> a.(0)) joins in
      Log.info (fun m -> m "Chose %a." Sexp.pp_hum ([%sexp_of: t] j));
      let tf = seq (local (reshape j) "reshape") (emit_joins j) in
      apply (traced tf) p r
    in
    global f "join-opt"
end
