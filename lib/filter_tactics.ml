open Core
open Castor
open Abslayout
open Collections

module Config = struct
  module type S = sig
    val fresh : Fresh.t

    include Ops.Config.S
  end
end

module Make (C : Config.S) = struct
  module O = Ops.Make (C)
  open O
  open C

  let extend_select ~with_ ps r =
    M.annotate_schema r ;
    let needed_fields =
      (* These are the fields that are emitted by r, used in with_ and not
         exposed already by ps. *)
      Set.diff
        (Set.inter with_ (Set.of_list (module Name) Meta.(find_exn r schema)))
        (Set.of_list (module Name) (List.filter_map ~f:pred_to_name ps))
      |> Set.to_list
      |> List.map ~f:(fun n -> Name n)
    in
    ps @ needed_fields

  (** Split predicates that sit under a binder into the parts that depend on
       bound variables and the parts that do not. *)
  let split_bound binder p =
    List.partition_tf (conjuncts p) ~f:(fun p' ->
        overlaps (pred_free p') (M.bound binder) )

  let is_supported orig_bound new_bound pred =
    let supported = Set.inter (pred_free pred) orig_bound in
    Set.is_subset supported ~of_:new_bound

  let hoist_filter r =
    M.annotate_schema r ;
    match r.node with
    | OrderBy {key; rel= {node= Filter (p, r); _}} ->
        Some (filter p (order_by key r))
    | GroupBy (ps, key, {node= Filter (p, r); _}) ->
        if is_supported (M.bound r) (M.bound (group_by ps key r)) p then
          Some (filter p (group_by ps key r))
        else None
    | Filter (p, {node= Filter (p', r); _}) ->
        Some (filter (Binop (And, p, p')) r)
    | Select (ps, {node= Filter (p, r); _}) -> (
      match select_kind ps with
      | `Scalar ->
          Some (filter p (select (extend_select ps r ~with_:(pred_free p)) r))
      | `Agg -> None )
    | Join {pred; r1= {node= Filter (p, r); _}; r2} ->
        Some (filter p (join pred r r2))
    | Join {pred; r1; r2= {node= Filter (p, r); _}} ->
        Some (filter p (join pred r1 r))
    | Dedup {node= Filter (p, r); _} -> Some (filter p (dedup r))
    | AList (rk, {node= Filter (p, r); _}) ->
        let below, above = split_bound rk p in
        Some (filter (conjoin above) (list rk (filter (conjoin below) r)))
    | AHashIdx (rk, {node= Filter (p, r); _}, m) ->
        let below, above = split_bound rk p in
        Some
          (filter (conjoin above) (hash_idx' rk (filter (conjoin below) r) m))
    | AOrderedIdx (rk, {node= Filter (p, r); _}, m) ->
        let below, above = split_bound rk p in
        Some
          (filter (conjoin above) (ordered_idx rk (filter (conjoin below) r) m))
    | _ -> None

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

  let gen_ordered_idx ?lb ?ub p r =
    let t = pred_to_schema p |> Name.type_exn in
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
    let fix_upper_bound bound kind =
      match kind with
      | `Open -> Ok bound
      | `Closed -> (
          let open Type.PrimType in
          match t with
          | IntT _ -> Ok (Binop (Add, bound, Int 1))
          | DateT _ -> Ok (Binop (Add, bound, Unop (Day, Int 1)))
          | FixedT _ -> Error "No open inequalities with fixed."
          | NullT | StringT _ | BoolT _ | TupleT _ | VoidT ->
              failwith "Unexpected type." )
    in
    let fix_lower_bound bound kind =
      match kind with
      | `Closed -> Ok bound
      | `Open -> (
          let open Type.PrimType in
          match t with
          | IntT _ -> Ok (Binop (Add, bound, Int 1))
          | DateT _ -> Ok (Binop (Add, bound, Unop (Day, Int 1)))
          | FixedT _ -> Error "No open inequalities with fixed."
          | NullT | StringT _ | BoolT _ | TupleT _ | VoidT ->
              failwith "Unexpected type." )
    in
    let open Result.Let_syntax in
    let%bind lb =
      match lb with
      | None -> Ok default_min
      | Some (b, k) -> fix_lower_bound b k
    in
    let%map ub =
      match ub with
      | None -> Ok default_max
      | Some (b, k) -> fix_upper_bound b k
    in
    let k = Fresh.name fresh "k%d" in
    let select_list = [As_pred (p, k)] in
    let filter_pred = Binop (Eq, Name (Name.create k), p) in
    ordered_idx
      (dedup (select select_list r))
      (filter filter_pred r)
      {oi_key_layout= None; lookup_low= lb; lookup_high= ub; order= `Desc}

  let elim_cmp_filter r =
    match r.node with
    | Filter (p, r') ->
        let has_param p =
          Set.diff (pred_free p) (M.bound r') |> Set.is_empty |> not
        in
        (* Select the comparisons which have a parameter on exactly one side and
           partition by the unparameterized side of the comparison. *)
        let cmps, rest =
          conjuncts p
          |> List.partition_map ~f:(function
               | (Binop (Gt, p1, p2) | Binop (Lt, p2, p1)) as p ->
                   if has_param p1 && not (has_param p2) then
                     `Fst (p2, (`Lt, p1))
                   else if has_param p2 && not (has_param p1) then
                     `Fst (p1, (`Gt, p2))
                   else `Snd p
               | (Binop (Ge, p1, p2) | Binop (Le, p2, p1)) as p ->
                   if has_param p1 && not (has_param p2) then
                     `Fst (p2, (`Le, p1))
                   else if has_param p2 && not (has_param p1) then
                     `Fst (p1, (`Ge, p2))
                   else `Snd p
               | p -> `Snd p )
        in
        let cmps = Map.of_alist_multi (module Pred) cmps in
        (* Order by the number of available bounds. *)
        List.sort
          ~compare:(fun (_, b1) (_, b2) ->
            [%compare: int] (List.length b1) (List.length b2) )
          (Map.to_alist cmps)
        |> List.find_map ~f:(fun (key, bounds) ->
               let open Option.Let_syntax in
               let lb =
                 List.filter_map bounds ~f:(fun (f, p) ->
                     match f with
                     | `Gt -> Some (p, `Closed)
                     | `Ge -> Some (p, `Open)
                     | _ -> None )
               in
               let ub =
                 List.filter_map bounds ~f:(fun (f, p) ->
                     match f with
                     | `Lt -> Some (p, `Closed)
                     | `Le -> Some (p, `Open)
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
               let%bind r' = Tactics_util.all_values [key] r' in
               match
                 gen_ordered_idx ?lb:(List.hd lb) ?ub:(List.hd ub) key r'
               with
               | Ok r -> Some (filter (conjoin (rest @ rest')) r)
               | Error err ->
                   Logs.warn (fun m -> m "Elim-cmp: %s" err) ;
                   None )
    | _ -> None

  let elim_cmp_filter = of_func elim_cmp_filter ~name:"elim-cmp-filter"

  let push_filter r =
    M.annotate_schema r ;
    match r.node with
    | Filter (p, {node= Filter (p', r'); _}) ->
        Some (filter (Binop (And, p, p')) r')
    | Filter (p, r') -> (
        let orig_bound = M.bound r' in
        match r'.node with
        | AList (rk, rv) ->
            if is_supported orig_bound (M.bound rk) p then
              Some (list (filter p rk) rv)
            else if is_supported orig_bound (M.bound rv) p then
              Some (list (filter p rk) rv)
            else None
        | AHashIdx (rk, rv, m) ->
            if is_supported orig_bound (M.bound rk) p then
              Some (hash_idx' (filter p rk) rv m)
            else if is_supported orig_bound (M.bound rv) p then
              Some (hash_idx' rk (filter p rv) m)
            else None
        | AOrderedIdx (rk, rv, m) ->
            if is_supported orig_bound (M.bound rk) p then
              Some (ordered_idx (filter p rk) rv m)
            else if is_supported orig_bound (M.bound rv) p then
              Some (ordered_idx rk (filter p rv) m)
            else None
        | ATuple (rs, Concat) ->
            Some (tuple (List.map rs ~f:(filter p)) Concat)
        | ATuple (rs, Cross) ->
            let rs_rev, _ =
              List.fold_left rs ~init:([], orig_bound) ~f:(fun (rs, bound) r ->
                  let bound = Set.union (M.bound r) bound in
                  let rs =
                    if is_supported orig_bound bound p then filter p r :: rs
                    else r :: rs
                  in
                  (rs, bound) )
            in
            let rs = List.rev rs_rev in
            Some (tuple rs Cross)
        | _ -> None )
    | _ -> None

  let push_filter = of_func push_filter ~name:"push-filter"

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
          let open Option.Let_syntax in
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
          let%map r' = Tactics_util.all_values select_list r in
          outer_filter (hash_idx r' (filter inner_filter_pred r) key)
    | _ -> None

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
