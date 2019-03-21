open Base
open Core
open Castor
open Collections
open Abslayout

module Config = struct
  module type S = sig
    val conn : Db.t

    val dbconn : Postgresql.connection

    val params : Set.M(Name).t

    val param_ctx : Value.t Map.M(Name).t

    val validate : bool
  end
end

module Make (Config : Config.S) () = struct
  open Config
  module M = Abslayout_db.Make (Config)
  module O = Ops.Make (Config)
  open O
  module J = Join_elim_tactics.Make (Config)
  open J

  let is_serializable r p =
    M.annotate_schema r ;
    is_serializeable (Path.get_exn p r)

  let fresh = Fresh.create ()

  let sql_ctx = Sql.create_ctx ~fresh ()

  let is_param_filter r p =
    M.annotate_schema r ;
    match (Path.get_exn p r).node with
    | Filter (pred, _) -> overlaps (pred_free pred) params
    | _ -> false

  let has_params r p =
    let r' = Path.get_exn p r in
    overlaps (names r') params

  let project r =
    M.annotate_schema r ;
    Some (project r)

  let project = of_func project ~name:"project"

  let push_orderby r =
    let open Option.Let_syntax in
    let orderby_cross_tuple key rs =
      List.iter rs ~f:M.annotate_schema ;
      match rs with
      | r :: rs ->
          let schema = Meta.(find_exn r schema) in
          let sschema = Set.of_list (module Name) schema in
          let skey =
            Set.of_list
              (module Name)
              (List.filter_map ~f:(fun (p, _) -> pred_to_name p) key)
          in
          if Set.is_subset skey ~of_:sschema then
            Some (tuple (order_by key r :: rs) Cross)
          else None
      | _ -> None
    in
    let orderby_list key r1 r2 =
      M.annotate_schema r1 ;
      M.annotate_schema r2 ;
      annotate_eq r1 ;
      annotate_eq r2 ;
      let schema1 = Meta.(find_exn r1 schema) in
      let open Core in
      let eqs = Meta.(find_exn r2 eq) in
      let names =
        List.concat_map eqs ~f:(fun (n, n') -> [n; n'])
        @ List.filter_map ~f:(fun (p, _) -> pred_to_name p) key
        @ schema1
      in
      (* Create map from names to sets of equal names. *)
      let eq_map =
        names
        |> List.dedup_and_sort ~compare:[%compare: Name.t]
        |> List.map ~f:(fun n -> (n, Union_find.create n))
        |> Hashtbl.of_alist_exn (module Name)
      in
      (* Add known equalities. *)
      List.iter eqs ~f:(fun (n, n') ->
          let s = Hashtbl.find_exn eq_map n in
          let s' = Hashtbl.find_exn eq_map n' in
          Union_find.union s s' ) ;
      let exception No_key in
      try
        let new_key =
          List.map key ~f:(fun (p, o) ->
              let p' =
                match pred_to_name p with
                | Some n -> (
                    let s = Hashtbl.find_exn eq_map n in
                    (* Find an equivalent name in schema 1. *)
                    let n' =
                      List.find schema1 ~f:(fun n' ->
                          let s' = Hashtbl.find_exn eq_map n' in
                          Union_find.same_class s s' )
                    in
                    match n' with Some n' -> Name n' | None -> raise No_key )
                | None -> raise No_key
              in
              (p', o) )
        in
        Some (list (order_by new_key r1) r2)
      with No_key -> None
    in
    let same_orders r1 r2 =
      M.annotate_schema r1 ;
      M.annotate_schema r2 ;
      annotate_eq r1 ;
      annotate_orders r1 ;
      annotate_eq r2 ;
      annotate_orders r2 ;
      [%compare.equal: (pred * order) list]
        Meta.(find_exn r1 order)
        Meta.(find_exn r2 order)
    in
    let%bind r' =
      match r.node with
      | OrderBy {key; rel= {node= Select (ps, r); _}} ->
          Some (select ps (order_by key r))
      | OrderBy {key; rel= {node= Filter (ps, r); _}} ->
          Some (filter ps (order_by key r))
      | OrderBy {key; rel= {node= AHashIdx (r1, r2, m); _}} ->
          Some (hash_idx' r1 (order_by key r2) m)
      | OrderBy {key; rel= {node= AList (r1, r2); _}} ->
          (* If we order a lists keys then the keys will be ordered in the
                   list. *)
          orderby_list key r1 r2
      | OrderBy {key; rel= {node= ATuple (rs, Cross); _}} ->
          orderby_cross_tuple key rs
      | _ -> None
    in
    if same_orders r r' then Some r' else None

  let push_orderby = of_func push_orderby ~name:"push-orderby"

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

  let hoist_filter r =
    M.annotate_schema r ;
    match r.node with
    | OrderBy {key; rel= {node= Filter (p, r); _}} ->
        Some (filter p (order_by key r))
    | GroupBy (ps, key, {node= Filter (p, r); _}) ->
        Some (filter p (group_by ps key r))
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

  let elim_groupby r =
    M.annotate_schema r ;
    annotate_free r ;
    match r.node with
    | GroupBy (ps, key, r) ->
        let key_name = Fresh.name fresh "k%d" in
        let key_preds = List.map key ~f:(fun n -> Name n) in
        let filter_pred =
          List.map key ~f:(fun n ->
              Binop (Eq, Name n, Name (Name.copy n ~relation:(Some key_name)))
          )
          |> List.fold_left1_exn ~f:(fun acc p -> Binop (And, acc, p))
        in
        if Set.is_empty (free r) then
          (* Use precise version. *)
          Some
            (list
               (as_ key_name (dedup (select key_preds r)))
               (select ps (filter filter_pred r)))
        else if List.for_all key ~f:(fun n -> Option.is_some (Name.rel n)) then (
          (* Otherwise, if all grouping keys are from named relations,
             select all possible grouping keys. *)
          let rels = Hashtbl.create (module Abslayout) in
          let alias_map = aliases r |> Map.of_alist_exn (module String) in
          List.iter key ~f:(fun n ->
              let r_name = Name.rel_exn n in
              let r =
                Option.value_exn
                  ~error:
                    (Error.create "No relation matching name." r_name
                       [%sexp_of: string])
                  (Map.find alias_map r_name)
              in
              Hashtbl.add_multi rels ~key:r ~data:(Name n) ) ;
          let key_rel =
            Hashtbl.to_alist rels
            |> List.map ~f:(fun (r, ns) -> dedup (select ns r))
            |> List.fold_left1_exn ~f:(join (Bool true))
          in
          Some (list (as_ key_name key_rel) (select ps (filter filter_pred r))) )
        else (* Otherwise, if some keys are computed, fail. *) None
    | _ -> None

  let elim_groupby = of_func elim_groupby ~name:"elim-groupby"

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

  let rec first_ok = function
    | Ok x :: _ -> Some x
    | _ :: xs -> first_ok xs
    | [] -> None

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
               match
                 gen_ordered_idx ?lb:(List.hd lb) ?ub:(List.hd ub) key r'
               with
               | Ok r -> Some (filter (conjoin (rest @ rest')) r)
               | Error err ->
                   Logs.warn (fun m -> m "Elim-cmp: %s" err) ;
                   None )
    | _ -> None

  let elim_cmp_filter = of_func elim_cmp_filter ~name:"elim-cmp-filter"

  let no_params r = Set.is_empty (Set.inter (names r) params)

  let is_supported orig_bound new_bound pred =
    let supported = Set.inter (pred_free pred) orig_bound in
    Set.is_subset supported ~of_:new_bound

  let row_store r =
    if no_params r then
      let s = M.to_schema r in
      let scalars = List.map s ~f:(fun n -> scalar (Name n)) in
      Some (list r (tuple scalars Cross))
    else None

  let row_store = of_func row_store ~name:"to-row-store"

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
        | _ -> None )
    | _ -> None

  let push_filter = of_func push_filter ~name:"push-filter"

  let push_select =
    of_func (Push_select.push_select (module M)) ~name:"push-select"

  module Join_opt = Join_opt.Make (Config)

  let opt =
    let open Infix in
    seq_many
      [ (* Eliminate groupby operators. *)
        fix (at_ elim_groupby (Path.all >>? is_groupby >>| shallowest))
      ; (* Hoist parameterized filters as far up as possible. *)
        fix
          (at_ hoist_filter
             (Path.all >>? is_param_filter >>| deepest >>= Path.parent))
      ; at_ Join_opt.transform (Path.all >>? is_join >>| shallowest)
      ; (* Push orderby operators into compile time position if possible. *)
        fix
          (at_ push_orderby
             Path.(all >>? is_orderby >>? is_run_time >>| shallowest))
      ; (* Eliminate the shallowest equality filter. *)
        at_ elim_eq_filter
          Path.(all >>? is_param_filter >>? is_run_time >>| shallowest)
      ; (* Eliminate the shallowest comparison filter. *)
        at_ elim_cmp_filter
          Path.(all >>? is_param_filter >>? is_run_time >>| shallowest)
      ; (* Push all unparameterized filters. *)
        fix
          (first push_filter
             Path.(all >>? is_run_time >>? is_filter >>? not is_param_filter))
      ; (* Push selections above collections. *)
        fix
          (at_ push_select
             (Path.all >>? is_select >>? above is_collection >>| deepest))
      ; (* Eliminate all unparameterized relations. *)
        fix
          (seq_many
             [ at_ row_store
                 Path.(
                   all >>? is_run_time >>? not has_params
                   >>? not is_serializable >>| shallowest)
             ; project ])
        (* Cleanup*)
      ; fix project ]

  let is_serializable r =
    M.annotate_schema r ;
    annotate_free r ;
    let bad_runtime_op =
      Path.(
        all >>? is_run_time
        >>? Infix.(is_join || is_groupby || is_orderby || is_dedup || is_scan))
        r
      |> Seq.is_empty |> not
    in
    let mis_bound_params =
      Path.(all >>? is_compile_time) r
      |> Seq.for_all ~f:(fun p ->
             not (overlaps (free (Path.get_exn p r)) params) )
      |> not
    in
    if bad_runtime_op then Error (Error.of_string "Bad runtime operation.")
    else if mis_bound_params then
      Error (Error.of_string "Parameters referenced at compile time.")
    else Ok ()
end
