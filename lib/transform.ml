open Base
open Castor
open Collections
open Abslayout

type t = Abslayout.t -> Abslayout.t option

module Config = struct
  module type S = sig
    val conn : Db.t

    val dbconn : Postgresql.connection

    val params : Set.M(Name.Compare_no_type).t
  end
end

let deepest ps =
  Seq.fold ps ~init:None ~f:(fun p_max_m p ->
      match p_max_m with
      | None -> Some p
      | Some p_max ->
          Some (if Path.length p > Path.length p_max then p else p_max) )

let shallowest ps =
  Seq.fold ps ~init:None ~f:(fun p_min_m p ->
      match p_min_m with
      | None -> Some p
      | Some p_min ->
          Some (if Path.length p < Path.length p_min then p else p_min) )

let is_join r =
  Seq.filter ~f:(fun p ->
      match (Path.get_exn p r).node with Join _ -> true | _ -> false )

let is_groupby r =
  Seq.filter ~f:(fun p ->
      match (Path.get_exn p r).node with GroupBy _ -> true | _ -> false )

let last_child _ = Some [Path.Child_last]

let rec fix tf r =
  match tf r with
  | Some r' -> if Abslayout.O.(r = r') then Some r else fix tf r'
  | None -> Some r

let seq t1 t2 r = match t1 r with Some r' -> t2 r' | None -> Some r

let rec seq_many = function
  | [] -> failwith "Empty transform list."
  | [t] -> t
  | t :: ts -> seq t (seq_many ts)

let at_ tf pspec r =
  let open Option.Let_syntax in
  let%bind p = pspec r in
  let%map r' = tf (Path.get_exn p r) in
  Path.set_exn p r r'

module Make (Config : Config.S) () = struct
  open Config
  module M = Abslayout_db.Make (Config)

  module Tf =
    Transform.Make (struct
        include Config

        let check_transforms = false
      end)
      (M)
      ()

  let fresh = Fresh.create ()

  let sql_ctx = Sql.create_ctx ~fresh ()

  let extend_select ~with_ ps r =
    let needed_fields =
      let of_list = Set.of_list (module Name.Compare_no_type) in
      (* These are the fields that are emitted by r, used in with_ and not
         exposed already by ps. *)
      Set.diff
        (Set.inter with_ (of_list Meta.(find_exn r schema)))
        (of_list (List.filter_map ~f:pred_to_name ps))
      |> Set.to_list
      |> List.map ~f:(fun n -> Name n)
    in
    ps @ needed_fields

  let hoist_filter r =
    M.annotate_schema r ;
    match r.node with
    | OrderBy {key; rel= {node= Filter (p, r); _}} ->
        Some (filter p (order_by key r))
    | GroupBy (ps, key, {node= Filter (p, r); _}) ->
        Some (filter p (group_by ps key r))
    | Filter (p, {node= Filter (p', r); _}) ->
        Some (filter (Binop (And, p, p')) r)
    | Select (ps, {node= Filter (p, r); _}) ->
        Some (filter p (select (extend_select ps r ~with_:(pred_free p)) r))
    | Join {pred; r1= {node= Filter (p, r); _}; r2} ->
        Some (filter p (join pred r r2))
    | Join {pred; r1; r2= {node= Filter (p, r); _}} ->
        Some (filter p (join pred r1 r))
    | Dedup {node= Filter (p, r); _} -> Some (filter p (dedup r))
    | _ -> None

  let split_filter r =
    match r.node with
    | Filter (Binop (And, p, p'), r) -> Some (filter p (filter p' r))
    | _ -> None

  let elim_groupby r =
    M.annotate_schema r ;
    annotate_free r ;
    match r.node with
    | GroupBy (ps, key, r) ->
        let key_name = Fresh.name fresh "k%d" in
        let key_preds = List.map key ~f:(fun n -> Name n) in
        let filter_pred =
          List.map key ~f:(fun n ->
              Binop (Eq, Name n, Name {n with relation= Some key_name}) )
          |> List.fold_left1_exn ~f:(fun acc p -> Binop (And, acc, p))
        in
        if Set.is_empty (free r) then
          (* Use precise version. *)
          Some
            (list
               (as_ key_name (dedup (select key_preds r)))
               (select ps (filter filter_pred r)))
        else if List.for_all key ~f:(fun n -> Option.is_some n.relation) then (
          (* Otherwise, if all grouping keys are from base relations,
             imprecisely select all of them. *)
          let rels = Hashtbl.create (module String) in
          List.iter key ~f:(fun n ->
              Hashtbl.add_multi rels
                ~key:(Option.value_exn n.relation)
                ~data:(Name n) ) ;
          let key_rel =
            Hashtbl.to_alist rels
            |> List.map ~f:(fun (r, ns) -> dedup (select ns (scan r)))
            |> List.fold_left1_exn ~f:(join (Bool true))
          in
          Some (list (as_ key_name key_rel) (select ps (filter filter_pred r))) )
        else (* Otherwise, if some keys are computed, fail. *) None
    | _ -> None

  let deepest_param_filter r =
    Path.all r
    |> Seq.filter ~f:(fun p ->
           match (Path.get_exn p r).node with
           | Filter (pred, _) ->
               Set.inter (pred_free pred) params |> Set.length > 0
           | _ -> false )
    |> deepest

  let hoist_deepest_filter = at_ hoist_filter deepest_param_filter

  let elim_eq_filter r =
    match r.node with
    | Filter (p, r) ->
        let eqs, rest =
          conjuncts p
          |> List.partition_map ~f:(function
               | Binop (Eq, p1, p2) -> `Fst (p1, Fresh.name fresh "k%d", p2)
               | p -> `Snd p )
        in
        if List.length eqs = 0 then None
        else
          let select_list =
            List.map eqs ~f:(fun (p, k, _) -> As_pred (p, k))
          in
          let inner_filter_pred =
            List.map eqs ~f:(fun (p, k, _) ->
                Binop (Eq, Name (Name.create k), p) )
            |> and_
          in
          let key = List.map eqs ~f:(fun (_, _, p) -> p) in
          let outer_filter r =
            match rest with [] -> r | _ -> filter (and_ rest) r
          in
          Some
            (outer_filter
               (hash_idx
                  (dedup (select select_list r))
                  (filter inner_filter_pred r)
                  key))
    | _ -> None

  let elim_join_nest r =
    match r.node with
    | Join {pred; r1; r2} -> Some (tuple [r1; filter pred r2] Cross)
    | _ -> None

  let id r _ = r

  let elim_join_hash = seq elim_join_nest (at_ elim_eq_filter last_child)

  let hoist_all_filters = seq (fix hoist_deepest_filter) (fix split_filter)

  module Join_opt = struct
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
             match A.pred_relations p with
             | [r1; r2] -> [(r1, p, r2)]
             | _ -> []
           in
           (j @ self#visit_t () r1) @ self#visit_t () r2
      end)
        #visit_t () r

    let rec emit_joins = function
      | Flat r -> r
      | Hash {lkey; lhs; rkey; rhs} ->
          Option.value_exn
            (elim_join_hash
               (join (Binop (Eq, lkey, rkey)) (emit_joins lhs) (emit_joins rhs)))
      | Nest {lhs; rhs; pred} ->
          Option.value_exn
            (elim_join_nest (join pred (emit_joins lhs) (emit_joins rhs)))

    let rec to_ralgebra = function
      | Flat r -> r
      | Nest {lhs; rhs; pred} ->
          A.join pred (to_ralgebra lhs) (to_ralgebra rhs)
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
      (Explain.explain ctx.conn
         (Sql.of_ralgebra ctx.sql r |> Sql.to_string ctx.sql))
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
            loop (i + 1)
              Float.(le && x.(i) <= y.(i))
              Float.(lt || x.(i) < y.(i))
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
                 |> List.map ~f:(fun c ->
                        Array.init t ~f:(fun i -> arels.(c.{i})) ) )
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
                               List.mem (A.pred_relations k1)
                                 ~equal:String.( = ) lhs_r
                             then k1
                             else k2
                           in
                           let rkey =
                             if
                               List.mem (A.pred_relations k1)
                                 ~equal:String.( = ) rhs_r
                             then k1
                             else k2
                           in
                           Some
                             (Hash {lkey; rkey; lhs= best_lhs; rhs= best_rhs})
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

    let cost = estimate_cost {conn= dbconn; dbconn= conn; sql= sql_ctx}

    let transform r =
      let preds = extract_joins r in
      match join_opt cost preds |> List.hd with
      | Some (_, join) -> Some (emit_joins join)
      | None -> None
  end

  let opt =
    seq_many
      [ (* fix
       *     (at_ Join_opt.transform (fun r ->
       *          Path.all r |> is_join r |> shallowest ))
       * ; *)
        fix
          (at_ elim_groupby (fun r -> Path.all r |> is_groupby r |> shallowest))
      ; hoist_all_filters ]
end
