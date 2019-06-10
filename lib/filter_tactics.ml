open Core
open Castor
open Abslayout
open Collections

module Config = struct
  module type S = sig
    include Ops.Config.S

    val params : Set.M(Name).t
  end
end

module Make (C : Config.S) = struct
  module O = Ops.Make (C)
  module S = Simplify_tactic.Make (C)
  open O
  open C
  open S
  module Tactics_util = Tactics_util.Make (C)

  let extend_select ~with_ ps r =
    let needed_fields =
      (* These are the fields that are emitted by r, used in with_ and not
         exposed already by ps. *)
      Set.diff
        (Set.inter with_ (Set.of_list (module Name) (schema_exn r)))
        (Set.of_list (module Name) (List.filter_map ~f:Pred.to_name ps))
      |> Set.to_list
      |> List.map ~f:(fun n -> Name n)
    in
    ps @ needed_fields

  let schema_set_exn r = schema_exn r |> Set.of_list (module Name)

  (** Split predicates that sit under a binder into the parts that depend on
       bound variables and the parts that do not. *)
  let split_bound binder p =
    List.partition_tf (Pred.conjuncts p) ~f:(fun p' ->
        overlaps (pred_free p') (schema_set_exn binder) )

  (** Check that a predicate is supported by a relation (it does not depend on
     anything in the context that it did not previously depend on.) *)
  let invariant_support orig_bound new_bound pred =
    let supported = Set.inter (pred_free pred) orig_bound in
    Set.is_subset supported ~of_:new_bound

  let to_filter r =
    match r.node with Filter (p, r) -> Some (p, r) | _ -> None

  let hoist_filter r =
    let open Option.Let_syntax in
    match r.node with
    | OrderBy {key; rel} ->
        let%map p, r = to_filter rel in
        filter p (order_by key r)
    | GroupBy (ps, key, r) ->
        let%bind p, r = to_filter r in
        if
          invariant_support (schema_set_exn r)
            (schema_set_exn (group_by ps key r))
            p
        then Some (filter p (group_by ps key r))
        else None
    | Filter (p', r) ->
        let%map p, r = to_filter r in
        filter (Binop (And, p, p')) r
    | Select (ps, r) -> (
        let%bind p, r = to_filter r in
        match select_kind ps with
        | `Scalar ->
            Some
              (filter p (select (extend_select ps r ~with_:(pred_free p)) r))
        | `Agg -> None )
    | Join {pred; r1; r2} -> (
      match (to_filter r1, to_filter r2) with
      | Some (p1, r1), Some (p2, r2) ->
          Some (filter (Pred.conjoin [p1; p2]) (join pred r1 r2))
      | None, Some (p, r2) -> Some (filter p (join pred r1 r2))
      | Some (p, r1), None -> Some (filter p (join pred r1 r2))
      | None, None -> None )
    | Dedup r ->
        let%map p, r = to_filter r in
        filter p (dedup r)
    | AList (rk, rv) ->
        let%map p, r = to_filter rv in
        let below, above = split_bound rk p in
        filter (Pred.conjoin above)
          (list rk (scope_exn rk) (filter (Pred.conjoin below) r))
    | AHashIdx ({hi_keys= rk; hi_values= rv; _} as h) ->
        let%map p, r = to_filter rv in
        let below, above = split_bound rk p in
        filter (Pred.conjoin above)
          (hash_idx' {h with hi_values= filter (Pred.conjoin below) r})
    | AOrderedIdx (rk, rv, m) ->
        let%map p, r = to_filter rv in
        let below, above = split_bound rk p in
        filter (Pred.conjoin above)
          (ordered_idx rk (scope_exn rk) (filter (Pred.conjoin below) r) m)
    | Relation _ | AEmpty | AScalar _ | ATuple _ | As (_, _) | DepJoin _ ->
        None

  let hoist_filter = of_func hoist_filter ~name:"hoist-filter"

  let split_filter r =
    match r.node with
    | Filter (Binop (And, p, p'), r) -> Some (filter p (filter p' r))
    | _ -> None

  let split_filter = of_func split_filter ~name:"split-filter"

  let rec first_ok = function
    | Ok x :: _ -> Some x
    | _ :: xs -> first_ok xs
    | [] -> None

  let qualify rn p =
    let visitor =
      object
        inherit [_] Abslayout.endo

        method! visit_Name () _ n = Name (Name.copy ~scope:(Some rn) n)
      end
    in
    visitor#visit_pred () p

  let gen_ordered_idx ?lb ?ub p rk rv =
    let t = Pred.to_type p in
    let default_min =
      let open Type.PrimType in
      match t with
      | IntT _ -> Int (Int.min_value + 1)
      | FixedT _ -> Fixed Fixed_point.(min_value + of_int 1)
      | DateT _ -> Date (Date.of_string "0000-01-01")
      | _ -> failwith "Unexpected type."
    in
    let default_max =
      let open Type.PrimType in
      match t with
      | IntT _ -> Int Int.max_value
      | FixedT _ -> Fixed Fixed_point.max_value
      | DateT _ -> Date (Date.of_string "9999-01-01")
      | _ -> failwith "Unexpected type."
    in
    let lp, lk = Option.value lb ~default:(default_min, `Closed) in
    let up, uk = Option.value ub ~default:(default_max, `Closed) in
    let k = Fresh.name Global.fresh "k%d" in
    ordered_idx
      (dedup (select [p] rk))
      k
      ((* Drop the key from the values relation. *)
       Tactics_util.select_out
         (Option.to_list (Pred.to_name p))
         (filter (Binop (Eq, qualify k p, p)) rv))
      { oi_key_layout= None
      ; lookup_low= lp
      ; bound_low= lk
      ; lookup_high= up
      ; bound_high= uk
      ; order= `Desc }

  (** A predicate `p` is a candidate lookup key into a partitioning of `r` if it
     does not depend on any of the fields in `r`.

      TODO: In practice we also want it to have a parameter in it. Is this correct? *)
  let is_candidate_key p r =
    let pfree = pred_free p in
    (not (overlaps (schema_set_exn r) pfree)) && overlaps params pfree

  let elim_cmp_filter r =
    match r.node with
    | Filter (p, r') ->
        (* Select the comparisons which have a parameter on exactly one side and
           partition by the unparameterized side of the comparison. *)
        let cmps, rest =
          Pred.conjuncts p
          |> List.partition_map ~f:(function
               | (Binop (Gt, p1, p2) | Binop (Lt, p2, p1)) as p ->
                   if is_candidate_key p1 r' && not (is_candidate_key p2 r')
                   then `Fst (p2, (`Lt, p1))
                   else if
                     is_candidate_key p2 r' && not (is_candidate_key p1 r')
                   then `Fst (p1, (`Gt, p2))
                   else `Snd p
               | (Binop (Ge, p1, p2) | Binop (Le, p2, p1)) as p ->
                   if is_candidate_key p1 r' && not (is_candidate_key p2 r')
                   then `Fst (p2, (`Le, p1))
                   else if
                     is_candidate_key p2 r' && not (is_candidate_key p1 r')
                   then `Fst (p1, (`Ge, p2))
                   else `Snd p
               | p -> `Snd p )
        in
        let cmps = Map.of_alist_multi (module Pred) cmps in
        (* Order by the number of available bounds. *)
        Map.to_alist cmps
        |> List.filter_map ~f:(fun (key, bounds) ->
               let x =
                 let open Or_error.Let_syntax in
                 let lb =
                   List.filter_map bounds ~f:(fun (f, p) ->
                       match f with
                       | `Gt -> Some (p, `Open)
                       | `Ge -> Some (p, `Closed)
                       | _ -> None )
                 in
                 let ub =
                   List.filter_map bounds ~f:(fun (f, p) ->
                       match f with
                       | `Lt -> Some (p, `Open)
                       | `Le -> Some (p, `Closed)
                       | _ -> None )
                 in
                 let rest' =
                   Map.remove cmps key |> Map.to_alist
                   |> List.concat_map ~f:(fun (p, xs) ->
                          List.map xs ~f:(fun (f, p') ->
                              let op =
                                match f with
                                | `Gt -> Gt
                                | `Lt -> Lt
                                | `Ge -> Ge
                                | `Le -> Le
                              in
                              Binop (op, p, p') ) )
                 in
                 let%map rk = Tactics_util.all_values [key] r' in
                 let idx =
                   gen_ordered_idx ?lb:(List.hd lb) ?ub:(List.hd ub) key rk r'
                 in
                 (rest', idx)
               in
               match x with
               | Ok (rest', r) -> Some (filter (Pred.conjoin (rest @ rest')) r)
               | Error err ->
                   Logs.warn (fun m -> m "Elim-cmp: %a" Error.pp err) ;
                   None )
        |> Seq.of_list
    | _ -> Seq.empty

  let elim_cmp_filter =
    Branching.of_func elim_cmp_filter ~name:"elim-cmp-filter"

  (* | ATuple (_rs, Cross) ->
   *     None
   *     (\* TODO: Figure out when this rule is useful. *\)
   *     (\* let rs_rev, _ =
   *      *   List.fold_left rs ~init:([], orig_bound) ~f:(fun (rs, bound) r ->
   *      *       let bound = Set.union (M.bound r) bound in
   *      *       let rs =
   *      *         if invariant_support orig_bound bound p then
   *      *           filter p r :: rs
   *      *         else r :: rs
   *      *       in
   *      *       (rs, bound) )
   *      * in
   *      * let rs = List.rev rs_rev in
   *      * Some (tuple rs Cross) *\) *)

  let push_filter r =
    let open Option.Let_syntax in
    let%bind p, r = to_filter r in
    match r.node with
    | Filter (p', r') -> Some (filter (Binop (And, p, p')) r')
    | ATuple (rs, Concat) -> Some (tuple (List.map rs ~f:(filter p)) Concat)
    | ATuple (rs, Cross) ->
        let ps = Pred.conjuncts p in
        (* Find the earliest placement for each predicate. *)
        let preds = Array.create ~len:(List.length rs) [] in
        let rec place_all ps i =
          if i >= List.length rs then ps
          else
            let bnd =
              List.nth_exn rs i |> schema_exn |> Set.of_list (module Name)
            in
            let pl, up =
              List.partition_tf ps ~f:(Tactics_util.is_supported bnd)
            in
            preds.(i) <- pl ;
            place_all up (i + 1)
        in
        let rest = place_all ps 0 in
        let rs = List.mapi rs ~f:(fun i -> filter (Pred.conjoin preds.(i))) in
        Some (filter (Pred.conjoin rest) (tuple rs Cross))
    | _ ->
        let%map rk, scope, rv, mk =
          match r.node with
          | DepJoin {d_lhs= rk; d_rhs= rv; d_alias} ->
              Some (rk, d_alias, rv, dep_join)
          | AList (rk, rv) -> Some (strip_scope rk, scope_exn rk, rv, list)
          | AHashIdx h ->
              Some
                ( h.hi_keys
                , h.hi_scope
                , h.hi_values
                , fun rk s rv ->
                    hash_idx' {h with hi_keys= rk; hi_scope= s; hi_values= rv}
                )
          | AOrderedIdx (rk, rv, m) ->
              Some
                ( strip_scope rk
                , scope_exn rk
                , rv
                , fun rk s rv -> ordered_idx rk s rv m )
          | _ -> None
        in
        let rk_bnd = Set.of_list (module Name) (schema_exn rk) in
        let pushed_key, pushed_val =
          Pred.conjuncts p
          |> List.partition_map ~f:(fun p ->
                 if Tactics_util.is_supported rk_bnd p then `Fst p else `Snd p
             )
        in
        let inner_key_pred = Pred.conjoin pushed_key in
        let inner_val_pred = Pred.conjoin pushed_val in
        mk (filter inner_key_pred rk) scope (filter inner_val_pred rv)

  let push_filter =
    (* NOTE: Simplify is necessary to make push-filter safe under fixpoints. *)
    seq' (of_func push_filter ~name:"push-filter") simplify

  let elim_eq_filter_src =
    let src = Logs.Src.create "elim-eq-filter" in
    Logs.Src.set_level src None ;
    src

  let elim_eq_filter r =
    match r.node with
    | Filter (p, r) -> (
        let eqs, rest =
          Pred.conjuncts p
          |> List.partition_map ~f:(function
               | Binop (Eq, p1, p2) as p ->
                   if is_candidate_key p1 r && not (is_candidate_key p2 r) then
                     `Fst (p2, p1)
                   else if is_candidate_key p2 r && not (is_candidate_key p1 r)
                   then `Fst (p1, p2)
                   else `Snd p
               | p -> `Snd p )
        in
        if List.length eqs = 0 then (
          Logs.info ~src:elim_eq_filter_src (fun m -> m "Found no equalities.") ;
          None )
        else
          let scope = Fresh.name Global.fresh "s%d" in
          let select_list, key = List.unzip eqs in
          let inner_filter_pred =
            let s = schema_exn r in
            List.map select_list ~f:(fun v ->
                Binop (Eq, v, Pred.scoped s scope v) )
            |> and_
          in
          let outer_filter r =
            match rest with [] -> r | _ -> filter (and_ rest) r
          in
          match Tactics_util.all_values select_list r with
          | Ok r' ->
              Some
                (outer_filter
                   (hash_idx r' scope
                      ((* Drop the key from the values relation. *)
                       Tactics_util.select_out (schema_exn r')
                         (filter inner_filter_pred r))
                      key))
          | Error err ->
              Logs.info ~src:elim_eq_filter_src (fun m ->
                  m "Could not get all values: %a" Error.pp err ) ;
              None )
    | _ ->
        Logs.debug ~src:elim_eq_filter_src (fun m -> m "Not a filter.") ;
        None

  let elim_eq_filter = of_func elim_eq_filter ~name:"elim-eq-filter"

  (* let precompute_filter n =
   *   let exception Failed of Error.t in
   *   let run_exn r =
   *     M.annotate_schema r ;
   *     match r.node with
   *     | Filter (p, r') ->
   *         let schema = Meta.(find_exn r' schema) in
   *         let free_vars =
   *           Set.diff (pred_free p) (Set.of_list (module Name) schema)
   *           |> Set.to_list
   *         in
   *         let free_var =
   *           match free_vars with
   *           | [v] -> v
   *           | _ ->
   *               let err =
   *                 Error.of_string
   *                   "Unexpected number of free variables in predicate."
   *               in
   *               raise (Failed err)
   *         in
   *         let witness_name = Fresh.name fresh "wit%d_" in
   *         let witnesses =
   *           List.mapi values ~f:(fun i v ->
   *               As_pred
   *                 ( subst_pred (Map.singleton (module Name) free_var v) p
   *                 , sprintf "%s_%d" witness_name i ) )
   *         in
   *         let filter_pred =
   *           List.foldi values ~init:p ~f:(fun i else_ v ->
   *               If
   *                 ( Binop (Eq, Name free_var, v)
   *                 , Name (Name.create (sprintf "%s_%d" witness_name i))
   *                 , else_ ) )
   *         in
   *         let select_list = witnesses @ List.map schema ~f:(fun n -> Name n) in
   *         Some (filter filter_pred (select select_list r'))
   *     | _ -> None
   *   in
   *   let f r = try run_exn r with Failed _ -> None in
   *   of_func f ~name:"precompute-filter" *)
end
