open Graph
open Printf
open Collections
open Ast
module A = Abslayout
module P = Pred.Infix
module V = Visitors

include (val Log.make ~level:(Some Warning) "castor-opt.join-opt")

let max_nest_joins = ref 0

let param =
  let open Command.Let_syntax in
  [%map_open
    let () = param
    and nest_joins =
      flag "max-nest-joins" (optional int)
        ~doc:"N maximum number of nested layout joins"
    in
    Option.iter nest_joins ~f:(fun x -> max_nest_joins := x)]

let filter p r = { node = Filter (p, r); meta = r.meta }

let join pred r1 r2 = { node = Join { pred; r1; r2 }; meta = r1.meta }

module Join_graph = struct
  module Vertex = struct
    module T = struct
      type t =
        (< stage : Name.t -> [ `Compile | `Run | `No_scope ] >
        [@ignore] [@opaque])
        annot
      [@@deriving compare, hash, sexp]

      let equal = [%compare.equal: t]
    end

    include T
    include Comparator.Make (T)
  end

  module Edge = struct
    module T = struct
      type t =
        (< stage : Name.t -> [ `Compile | `Run | `No_scope ] >
        [@ignore] [@opaque])
        annot
        pred
      [@@deriving compare, sexp]
    end

    include T
    include Comparator.Make (T)

    let default = Bool true
  end

  module G = Persistent.Graph.ConcreteLabeled (Vertex) (Edge)
  include G
  include Oper.P (G)
  include Oper.Choose (G)
  module Dfs = Traverse.Dfs (G)

  let source_relation leaves n =
    List.find_map leaves ~f:(fun (r, s) -> if Set.mem s n then Some r else None)
    |> Result.of_option
         ~error:
           (Error.create "No source found for name."
              (n, List.map leaves ~f:(fun (_, ns) -> ns))
              [%sexp_of: Name.t * Set.M(Name).t list])

  let to_string g = sprintf "graph (|V|=%d) (|E|=%d)" (nb_vertex g) (nb_edges g)

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
      let f v (lhs, rhs) =
        let in_set = Set.mem vs v in
        let lhs = if in_set then remove_vertex lhs v else lhs
        and rhs = if in_set then rhs else remove_vertex rhs v in
        (lhs, rhs)
      in
      fold_vertex f g (g, g)
    in
    let es =
      let f ((v1, _, v2) as e) es =
        let v1_in = Set.mem vs v1 and v2_in = Set.mem vs v2 in
        if (v1_in && not v2_in) || ((not v1_in) && v2_in) then e :: es else es
      in
      fold_edges_e f g []
    in
    (g1, g2, es)

  let is_connected g =
    let n = nb_vertex g in
    let n = Dfs.fold_component (fun _ i -> i - 1) n g (choose_vertex g) in
    n = 0

  let contract join g =
    (* if the edge is to be removed (property = true):
       * make a union of the two union-sets of start and end node;
       * put this set in the map for all nodes in this set *)
    let f edge m =
      let s_src, j_src = Map.find_exn m (E.src edge) in
      let s_dst, j_dst = Map.find_exn m (E.dst edge) in
      let s = Set.union s_src s_dst in
      let j = join ~label:(G.E.label edge) j_src j_dst in
      Set.fold s ~init:m ~f:(fun m vertex -> Map.set m ~key:vertex ~data:(s, j))
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
    G.fold_edges_e f g m |> Map.data |> List.hd_exn |> Tuple.T2.get2

  let to_ralgebra graph =
    if nb_vertex graph = 1 then choose_vertex graph
    else contract (fun ~label:p j1 j2 -> join p j1 j2) graph

  (** Collect the leaves of the join tree rooted at r. *)
  let rec to_leaves r =
    match r.node with
    | Join { r1; r2; _ } -> Set.union (to_leaves r1) (to_leaves r2)
    | _ -> Set.singleton (module Vertex) r

  type graph_filters = {
    graph : t;
    leaf_filters : Set.M(Edge).t Map.M(Vertex).t;
    top_filters : Edge.t list;
  }

  (** Convert a join tree to a join graph.

      Returns a graph where the nodes are the queries at the leaves of the join
     tree and the edges are join predicates. *)
  let rec to_graph leaves r =
    let open Option.Let_syntax in
    let union_filters f1 f2 =
      let merger ~key:_ = function
        | `Left x | `Right x -> Some x
        | `Both (x, y) -> Some (Set.union x y)
      in
      Map.merge ~f:merger f1 f2
    in
    match r.node with
    | Join { r1; r2; pred = p } ->
        let x1 = to_graph leaves r1 and x2 = to_graph leaves r2 in
        let graph = union x1#graph x2#graph
        and leaf_filters = union_filters x1#leaf_filters x2#leaf_filters
        and top_filters = x1#top_filters @ x2#top_filters in
        let x =
          object
            method graph = graph

            method leaf_filters = leaf_filters

            method top_filters = top_filters
          end
        in

        (* Collect the set of relations that this join depends on. *)
        List.fold_left (Pred.conjuncts p) ~init:x ~f:(fun acc p ->
            let pred_rels =
              Pred.names p |> Set.to_list
              |> List.map ~f:(source_relation leaves)
              |> Or_error.all
            in
            match pred_rels with
            | Ok [ r ] ->
                let leaf_filters =
                  Map.update acc#leaf_filters r ~f:(function
                    | Some fs -> Set.add fs p
                    | None -> Set.singleton (module Edge) p)
                in
                object
                  method graph = acc#graph

                  method leaf_filters = leaf_filters

                  method top_filters = acc#top_filters
                end
            | Ok [ r1; r2 ] ->
                let graph = add_or_update_edge acc#graph (r1, p, r2) in
                object
                  method graph = graph

                  method leaf_filters = acc#leaf_filters

                  method top_filters = acc#top_filters
                end
            | _ ->
                object
                  method graph = acc#graph

                  method leaf_filters = acc#leaf_filters

                  method top_filters = p :: acc#top_filters
                end)
    | _ ->
        object
          method graph = empty

          method leaf_filters = Map.empty (module Vertex)

          method top_filters = []
        end

  let of_abslayout r =
    let open Option.Let_syntax in
    debug (fun m -> m "Planning join for %a." A.pp r);
    let leaves =
      to_leaves r |> Set.to_list
      |> List.map ~f:(fun r ->
             (r, Schema.schema r |> Set.of_list (module Name)))
    in
    let x = to_graph leaves r in
    (* Put the filters back onto the leaves of the graph. *)
    let graph =
      map_vertex
        (fun r ->
          match Map.find x#leaf_filters r with
          | Some preds -> filter (Set.to_list preds |> Pred.conjoin) r
          | None -> r)
        x#graph
    in
    object
      method graph = graph

      method top_filters = x#top_filters
    end

  let partition_fold ~init ~f graph =
    let vertices = vertices graph |> Array.of_list in
    let n = Array.length vertices in
    let rec loop acc k =
      if k >= n then acc
      else
        let acc =
          let open Combinat.Combination in
          create ~n ~k
          |> fold ~init:acc ~f:(fun acc vs ->
                 let g1, g2, es =
                   partition graph
                     ( List.init k ~f:(fun i -> vertices.(vs.{i}))
                     |> Set.of_list (module Vertex) )
                 in
                 if is_connected g1 && is_connected g2 then f acc (g1, g2, es)
                 else acc)
        in
        loop acc (k + 1)
    in
    loop init 1
end

module Pareto_set = struct
  type 'a t = (float array * 'a) list

  let empty = []

  let singleton c v = [ (c, v) ]

  let dominates x y =
    assert (Array.length x = Array.length y);
    let n = Array.length x in
    let rec loop i le lt =
      if i = n then le && lt
      else loop (i + 1) Float.(le && x.(i) <= y.(i)) Float.(lt || x.(i) < y.(i))
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

module Config = struct
  module type My_S = sig
    val cost_conn : Db.t

    val params : Set.M(Name).t

    val random : Mcmc.Random_choice.t
  end

  module type S = sig
    include Ops.Config.S

    include Simple_tactics.Config.S

    include My_S
  end
end

module Make (Config : Config.S) = struct
  open Config

  open Ops.Make (Config)

  module G = Join_graph

  type t =
    | Flat of G.Vertex.t
    | Id of G.Vertex.t
    | Hash of { lkey : G.Edge.t; lhs : t; rkey : G.Edge.t; rhs : t }
    | Nest of { lhs : t; rhs : t; pred : G.Edge.t }
  [@@deriving sexp_of]

  let rec num_nest = function
    | Flat _ | Id _ -> 0
    | Hash { lhs; rhs } -> num_nest lhs + num_nest rhs
    | Nest { lhs; rhs } -> 1 + num_nest lhs + num_nest rhs

  let rec to_ralgebra = function
    | Flat r | Id r -> r
    | Nest { lhs; rhs; pred } -> join pred (to_ralgebra lhs) (to_ralgebra rhs)
    | Hash { lkey; rkey; lhs; rhs } ->
        join (Binop (Eq, lkey, rkey)) (to_ralgebra lhs) (to_ralgebra rhs)

  module Cost = struct
    let read = function
      | Prim_type.(IntT _ | DateT _ | FixedT _ | StringT _) -> 4.0
      | BoolT _ -> 1.0
      | _ -> failwith "Unexpected type."

    let hash = function
      | Prim_type.(IntT _ | DateT _ | FixedT _ | BoolT _) -> 40.0
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
      let open Visitors in
      let rec annot r = V.Map.annot query r
      and query q = V.Map.query annot pred q
      and pred p = Pred.to_static ~params p in
      annot @@ strip_meta r
    in

    (* Generate a group-by using the partition fields. *)
    let parts = Set.filter parts ~f:(Set.mem s) in
    let parted_r =
      let c = P.name (Name.create "c") in
      A.(
        select
          [
            As_pred (Min c, "min");
            As_pred (Max c, "max");
            As_pred (Avg c, "avg");
          ]
        @@ group_by [ P.as_ Count "c" ] (Set.to_list parts) static_r)
      |> Simplify_tactic.simplify ~dedup:true cost_conn
    in
    let sql = Sql.of_ralgebra parted_r |> Sql.to_string
    and schema = Prim_type.[ int_t; int_t; fixed_t ] in
    let tups = Db.exec_exn cost_conn schema sql in

    match tups with
    | [ Int min; Int max; Fixed avg ] :: _ ->
        (min, max, Fixed_point.to_float avg)
    | [ Null; Null; Null ] :: _ -> (0, 0, 0.0)
    | _ ->
        err (fun m -> m "Unexpected tuples: %s" sql);
        failwith "Unexpected tuples."

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

  let rec is_static_join = function
    | Id r -> Is_serializable.is_static ~params r
    | Flat _ -> true
    | Hash { lhs; rhs; _ } | Nest { lhs; rhs; _ } ->
        is_static_join lhs && is_static_join rhs

  let leaf_flat r =
    let open Option.Let_syntax in
    if Is_serializable.is_static ~params r then return @@ Flat r else None

  let leaf_id r =
    let open Option.Let_syntax in
    return @@ Id r

  let enum_flat_join opt parts pred s1 s2 =
    let select_flat s =
      List.filter_map (opt parts s) ~f:(fun (_, j) ->
          if is_static_join j then Some (to_ralgebra j) else None)
    in
    List.cartesian_product (select_flat s1) (select_flat s2)
    |> List.map ~f:(fun (r1, r2) -> Flat (join pred r1 r2))

  let enum_hash_join opt parts pred s1 s2 =
    let open List.Let_syntax in
    let lhs = G.to_ralgebra s1 and rhs = G.to_ralgebra s2 in
    let lhs_schema = Schema.schema lhs and rhs_schema = Schema.schema rhs in
    (* Figure out which partition a key comes from. *)
    let key_side k =
      let rs = Pred.names k in
      let all_in s =
        Set.for_all rs ~f:(List.mem ~equal:[%compare.equal: Name.t] s)
      in
      if all_in lhs_schema then return (`Lhs (s1, k))
      else if all_in rhs_schema then return (`Rhs (s2, k))
      else (
        debug (fun m -> m "Unknown key %a" Pred.pp k);
        [] )
    in
    let%bind k1, k2 =
      match pred with
      | Binop (Eq, k1, k2) -> return (k1, k2)
      | _ ->
          debug (fun m -> m "Adding hash join failed.");
          []
    in
    let%bind s1 = key_side k1 and s2 = key_side k2 in
    match (s1, s2) with
    | `Lhs (s1, k1), `Rhs (s2, k2) | `Rhs (s2, k2), `Lhs (s1, k1) ->
        let rhs_parts = Set.union (to_parts rhs pred) parts in
        let lhs_set = opt parts s1 |> List.map ~f:(fun (_, j) -> j)
        and rhs_set =
          opt rhs_parts s2
          |> List.map ~f:(fun (_, j) -> j)
          |> List.filter ~f:is_static_join
        in
        List.cartesian_product lhs_set rhs_set
        |> List.map ~f:(fun (lhs, rhs) ->
               Hash { lkey = k1; rkey = k2; lhs; rhs })
    | _ ->
        debug (fun m ->
            m "Keys come from same partition %a %a" Pred.pp k1 Pred.pp k2);
        []

  let enum_nest_join opt parts pred s1 s2 =
    let lhs_parts = Set.union (to_parts (G.to_ralgebra s1) pred) parts
    and rhs_parts = Set.union (to_parts (G.to_ralgebra s2) pred) parts in
    let lhs_set =
      opt lhs_parts s1
      |> List.map ~f:(fun (_, j) -> j)
      |> List.filter ~f:is_static_join
    and rhs_set = opt rhs_parts s2 |> List.map ~f:(fun (_, j) -> j) in
    List.cartesian_product lhs_set rhs_set
    |> List.map ~f:(fun (j1, j2) -> Nest { lhs = j1; rhs = j2; pred })

  let opt_nonrec opt parts s =
    info (fun m -> m "Choosing join for space %s." (G.to_string s));

    let filter_nest_joins =
      List.filter ~f:(fun j -> num_nest j <= !max_nest_joins)
    in
    let add_cost = List.map ~f:(fun j -> ([| scan_cost parts j |], j)) in

    let joins =
      if G.nb_vertex s = 1 then
        (* Select strategy for the leaves of the join tree. *)
        let r = G.choose_vertex s in
        [ leaf_flat r; leaf_id r ] |> List.filter_map ~f:Fun.id
        |> filter_nest_joins |> add_cost |> Pareto_set.of_list
      else
        G.partition_fold s ~init:Pareto_set.empty ~f:(fun cs (s1, s2, es) ->
            let pred = Pred.conjoin (List.map es ~f:(fun (_, p, _) -> p)) in
            let r = join pred (G.to_ralgebra s1) (G.to_ralgebra s2) in

            let open Mcmc.Random_choice in
            let flat_joins =
              if rand random "flat-join" (strip_meta r) then
                enum_flat_join opt parts pred s1 s2
              else []
            and hash_joins =
              if rand random "hash-join" (strip_meta r) then
                enum_hash_join opt parts pred s1 s2
              else []
            and nest_joins =
              if rand random "nest-join" (strip_meta r) then
                enum_nest_join opt parts pred s1 s2
              else []
            in
            let all_joins =
              flat_joins @ hash_joins @ nest_joins
              |> filter_nest_joins |> add_cost
            in

            Pareto_set.(union_all [ cs; of_list all_joins ]))
    in
    info (fun m -> m "Found %d pareto-optimal joins." (Pareto_set.length joins));
    joins

  let opt =
    let module Key = struct
      type t = Set.M(Name).t * Set.M(G.Vertex).t
      [@@deriving compare, hash, sexp_of]

      let create p graph =
        let vertices =
          G.fold_vertex
            (fun v vs -> Set.add vs v)
            graph
            (Set.empty (module G.Vertex))
        in
        (p, vertices)
    end in
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

  let opt r =
    let open Option.Let_syntax in
    let s = G.of_abslayout r in
    let joins = opt (Set.empty (module Name)) s#graph in
    object
      method joins = joins

      method top_filters = s#top_filters
    end

  let reshape top_filters j _ =
    Some
      ( A.filter (Pred.conjoin (top_filters :> Pred.t list))
      @@ (to_ralgebra j :> Ast.t) )

  let rec emit_joins =
    let open Join_elim_tactics.Make (Config) in
    let open Simple_tactics.Make (Config) in
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
      let r =
        Is_serializable.annotate_stage r
        |> Visitors.map_meta (fun meta ->
               object
                 method stage = meta#stage
               end)
      in
      let x = opt (Castor.Path.get_exn p r) in
      info (fun m -> m "Found %d join options." (Pareto_set.length x#joins));
      let%bind j = Pareto_set.min_elt (fun a -> a.(0)) x#joins in
      info (fun m -> m "Chose %a." Sexp.pp_hum ([%sexp_of: t] j));
      let tf =
        seq
          (local (reshape x#top_filters j) "reshape")
          (at_ (emit_joins j) (child 0))
      in
      apply (traced tf) p (strip_meta r)
    in
    global f "join-opt"
end
