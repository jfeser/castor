open Ast
open Collections
module A = Abslayout
module P = Pred.Infix
module V = Visitors
open Match

(** Enable partitioning when a parameter is used in a range predicate. *)
let enable_partition_cmp = ref false

module Config = struct
  module type My_S = sig
    val params : Set.M(Name).t
  end

  module type S = sig
    include Ops.Config.S

    include Simplify_tactic.Config.S

    include Tactics_util.Config.S

    include My_S
  end
end

module Make (C : Config.S) = struct
  open Ops.Make (C)

  open Simplify_tactic.Make (C)

  module Tactics_util = Tactics_util.Make (C)

  module My_C : Config.My_S = C

  open My_C

  let fresh_name = Fresh.name Global.fresh

  let schema_set r = Schema.schema r |> Set.of_list (module Name)

  (** Split predicates that sit under a binder into the parts that depend on
       bound variables and the parts that don't. *)
  let split_bound binder p =
    List.partition_tf (Pred.conjuncts p) ~f:(fun p' ->
        overlaps (Free.pred_free p') (schema_set binder))

  (** Check that a predicate is supported by a relation (it does not depend on
     anything in the context that it did not previously depend on.) *)
  let invariant_support orig_bound new_bound pred =
    let supported = Set.inter (Free.pred_free pred) orig_bound in
    Set.is_subset supported ~of_:new_bound

  let merge_select s1 s2 =
    s1 @ s2
    |> List.dedup_and_sort ~compare:(fun p1 p2 ->
           [%compare: Name.t option] (Schema.to_name p1) (Schema.to_name p2))

  let filter_many ps r =
    if List.is_empty ps then r else A.filter (Pred.conjoin ps) r

  let hoist_filter r =
    let open Option.Let_syntax in
    match r.node with
    | OrderBy { key; rel } ->
        let%map p, r = to_filter rel in
        A.filter p (A.order_by key r)
    | GroupBy (ps, key, r) ->
        let%bind p, r = to_filter r in
        if invariant_support (schema_set r) (schema_set (A.group_by ps key r)) p
        then Some (A.filter p (A.group_by ps key r))
        else None
    | Filter (p', r) ->
        let%map p, r = to_filter r in
        A.filter (Binop (And, p, p')) r
    | Select (ps, r) -> (
        let%bind p, r = to_filter r in
        match A.select_kind ps with
        | `Scalar ->
            if Tactics_util.select_contains (Free.pred_free p) ps r then
              Some (A.filter p (A.select ps r))
            else None
        | `Agg -> None )
    | Join { pred; r1; r2 } -> (
        match (to_filter r1, to_filter r2) with
        | Some (p1, r1), Some (p2, r2) ->
            Some (filter_many [ p1; p2 ] (A.join pred r1 r2))
        | None, Some (p, r2) -> Some (A.filter p (A.join pred r1 r2))
        | Some (p, r1), None -> Some (A.filter p (A.join pred r1 r2))
        | None, None -> None )
    | Dedup r ->
        let%map p, r = to_filter r in
        A.filter p (A.dedup r)
    | AList l ->
        let%map p, r = to_filter l.l_values in
        A.filter (Pred.unscoped l.l_scope p) (A.list' { l with l_values = r })
    | AHashIdx ({ hi_keys = rk; hi_values = rv; _ } as h) ->
        let%map p, r = to_filter rv in
        let below, above = split_bound rk p in
        let above = List.map above ~f:(Pred.unscoped h.hi_scope) in
        filter_many above
        @@ A.hash_idx' { h with hi_values = filter_many below r }
    | AOrderedIdx o ->
        let%map p, r = to_filter o.oi_values in
        let below, above = split_bound o.oi_keys p in
        let above = List.map above ~f:(Pred.unscoped o.oi_scope) in
        filter_many above
        @@ A.ordered_idx' { o with oi_values = filter_many below r }
    | DepJoin _ | Relation _ | AEmpty | AScalar _ | ATuple _ | Range _ -> None

  let hoist_filter = of_func hoist_filter ~name:"hoist-filter"

  let split_filter r =
    match r.node with
    | Filter (Binop (And, p, p'), r) -> Some (A.filter p (A.filter p' r))
    | _ -> None

  let split_filter = of_func split_filter ~name:"split-filter"

  let rec first_ok = function
    | Ok x :: _ -> Some x
    | _ :: xs -> first_ok xs
    | [] -> None

  let qualify rn p =
    let visitor =
      object
        inherit [_] V.endo

        method! visit_Name () _ n = Name (Name.copy ~scope:(Some rn) n)
      end
    in
    visitor#visit_pred () p

  let gen_ordered_idx ?lb ?ub p rk rv =
    let k = fresh_name "k%d" in
    let n = fresh_name "x%d" in
    A.ordered_idx
      (A.dedup (A.select [ P.as_ p n ] rk))
      k
      (A.filter (Binop (Eq, Name (Name.create ~scope:k n), p)) rv)
      [ (lb, ub) ]

  (** A predicate `p` is a candidate lookup key into a partitioning of `r` if it
     does not depend on any of the fields in `r`.

      TODO: In practice we also want it to have a parameter in it. Is this correct? *)
  let is_candidate_key p r =
    let pfree = Free.pred_free p in
    (not (overlaps (schema_set r) pfree)) && overlaps params pfree

  (** A predicate is a candidate to be matched if all its free variables are
     bound by the relation that it is above. *)
  let is_candidate_match p r =
    Set.is_subset (Free.pred_free p) ~of_:(schema_set r)

  let elim_cmp_filter r =
    match r.node with
    | Filter (p, r') -> (
        let orig_schema = Schema.schema r' in

        (* Select the comparisons which have a parameter on exactly one side and
           partition by the unparameterized side of the comparison. *)
        let cmps, rest =
          Pred.conjuncts p
          |> List.partition_map ~f:(function
               | (Binop (Gt, p1, p2) | Binop (Lt, p2, p1)) as p ->
                   if is_candidate_key p1 r' && is_candidate_match p2 r' then
                     `Fst (p2, (`Lt, p1))
                   else if is_candidate_key p2 r' && is_candidate_match p1 r'
                   then `Fst (p1, (`Gt, p2))
                   else `Snd p
               | (Binop (Ge, p1, p2) | Binop (Le, p2, p1)) as p ->
                   if is_candidate_key p1 r' && is_candidate_match p2 r' then
                     `Fst (p2, (`Le, p1))
                   else if is_candidate_key p2 r' && is_candidate_match p1 r'
                   then `Fst (p1, (`Ge, p2))
                   else `Snd p
               | p -> `Snd p)
        in
        let cmps, rest' =
          Map.of_alist_multi (module Pred) cmps
          |> Map.to_alist
          |> List.map ~f:(fun (key, bounds) ->
                 let lb, rest =
                   let open_lb =
                     List.filter_map bounds ~f:(fun (f, p) ->
                         match f with `Gt -> Some p | _ -> None)
                   in
                   let closed_lb =
                     List.filter_map bounds ~f:(fun (f, p) ->
                         match f with `Ge -> Some p | _ -> None)
                   in
                   match
                     (List.length open_lb = 0, List.length closed_lb = 0)
                   with
                   | true, true -> (None, [])
                   | _, true ->
                       ( Option.map (List.reduce ~f:Pred.max_of open_lb)
                           ~f:(fun max -> (max, `Open)),
                         [] )
                   | _ ->
                       ( Option.map
                           (List.reduce ~f:Pred.max_of (open_lb @ closed_lb))
                           ~f:(fun max -> (max, `Closed)),
                         List.map open_lb ~f:(fun v -> P.(key > v)) )
                 in
                 let ub, rest' =
                   let open_ub =
                     List.filter_map bounds ~f:(fun (f, p) ->
                         match f with `Lt -> Some p | _ -> None)
                   in
                   let closed_ub =
                     List.filter_map bounds ~f:(fun (f, p) ->
                         match f with `Le -> Some p | _ -> None)
                   in
                   match
                     (List.length open_ub = 0, List.length closed_ub = 0)
                   with
                   | true, true -> (None, [])
                   | _, true ->
                       ( Option.map (List.reduce ~f:Pred.min_of open_ub)
                           ~f:(fun p -> (p, `Open)),
                         [] )
                   | _ ->
                       ( Option.map
                           (List.reduce ~f:Pred.min_of (open_ub @ closed_ub))
                           ~f:(fun p -> (p, `Closed)),
                         List.map open_ub ~f:(fun v -> P.(key > v)) )
                 in
                 ((key, (lb, ub)), rest @ rest'))
          |> List.unzip
        in
        let rest = rest @ List.concat rest' in
        let key, cmps = List.unzip cmps in
        let x =
          let open Or_error.Let_syntax in
          if List.is_empty key then
            Or_error.error_string "No candidate keys found."
          else
            let%map all_keys = Tactics_util.all_values key r' in
            let scope = fresh_name "s%d" in
            let keys_schema = Schema.schema all_keys in
            A.select (Schema.to_select_list orig_schema)
            @@ A.ordered_idx all_keys scope
                 (filter_many
                    (List.map key ~f:(fun p ->
                         P.(p = Pred.scoped keys_schema scope p)))
                    r')
                 cmps
        in
        match x with
        | Ok r -> Seq.singleton (filter_many rest r)
        | Error err ->
            Logs.warn (fun m -> m "Elim-cmp: %a" Error.pp err);
            Seq.empty )
    | _ -> Seq.empty

  let elim_cmp_filter =
    Branching.(local elim_cmp_filter ~name:"elim-cmp-filter")

  let push_filter_cross_tuple stage p rs =
    let ps = Pred.conjuncts p in
    (* Find the earliest placement for each predicate. *)
    let preds = Array.create ~len:(List.length rs) [] in
    let rec place_all ps i =
      if i >= List.length rs then ps
      else
        let bnd =
          List.nth_exn rs i |> Schema.schema |> Set.of_list (module Name)
        in
        let pl, up =
          List.partition_tf ps ~f:(Tactics_util.is_supported stage bnd)
        in
        preds.(i) <- pl;
        place_all up (i + 1)
    in
    let rest = place_all ps 0 in
    let rs = List.mapi rs ~f:(fun i -> filter_many preds.(i)) in
    filter_many rest (A.tuple rs Cross)

  let push_filter_list stage p l =
    let rk_bnd = Set.of_list (module Name) (Schema.schema l.l_keys) in
    let pushed_key, pushed_val =
      Pred.conjuncts p
      |> List.partition_map ~f:(fun p ->
             if Tactics_util.is_supported stage rk_bnd p then `Fst p else `Snd p)
    in
    A.list
      (filter_many pushed_key l.l_keys)
      l.l_scope
      (filter_many pushed_val l.l_values)

  let push_filter_select stage p ps r =
    match A.select_kind ps with
    | `Scalar ->
        let ctx =
          List.filter_map ps ~f:(fun p ->
              Option.map (Pred.to_name p) ~f:(fun n -> (n, Pred.remove_as p)))
          |> Map.of_alist_exn (module Name)
        in
        let p' = Pred.subst ctx p in
        A.select ps (A.filter p' r)
    | `Agg ->
        let scalar_ctx =
          List.filter_map ps ~f:(fun p ->
              if Poly.(Pred.kind p = `Scalar) then
                Option.map (Pred.to_name p) ~f:(fun n -> (n, Pred.remove_as p))
              else None)
          |> Map.of_alist_exn (module Name)
        in
        let names = Map.keys scalar_ctx |> Set.of_list (module Name) in
        let pushed, unpushed =
          Pred.conjuncts p
          |> List.partition_map ~f:(fun p ->
                 if Tactics_util.is_supported stage names p then
                   `Fst (Pred.subst scalar_ctx p)
                 else `Snd p)
        in
        filter_many unpushed @@ A.select ps @@ filter_many pushed r

  let push_filter r =
    let open Option.Let_syntax in
    let stage = r.meta#stage in
    let r = strip_meta r in
    let%bind p, r = to_filter r in
    match r.node with
    | Filter (p', r') -> Some (A.filter (Binop (And, p, p')) r')
    | Dedup r' -> Some (A.dedup (A.filter p r'))
    | Select (ps, r') -> Some (push_filter_select stage p ps r')
    | ATuple (rs, Concat) -> Some (A.tuple (List.map rs ~f:(A.filter p)) Concat)
    | ATuple (rs, Cross) -> Some (push_filter_cross_tuple stage p rs)
    (* Lists are a special case because their keys are bound at compile time and
       are not available at runtime. *)
    | AList l -> Some (push_filter_list stage p l)
    | _ ->
        let%map rk, scope, rv, mk =
          match r.node with
          | DepJoin { d_lhs = rk; d_rhs = rv; d_alias } ->
              Some (rk, d_alias, rv, A.dep_join)
          | AList l -> Some (l.l_keys, l.l_scope, l.l_values, A.list)
          | AHashIdx h ->
              Some
                ( h.hi_keys,
                  h.hi_scope,
                  h.hi_values,
                  fun rk s rv ->
                    A.hash_idx'
                      { h with hi_keys = rk; hi_scope = s; hi_values = rv } )
          | AOrderedIdx o ->
              Some
                ( o.oi_keys,
                  o.oi_scope,
                  o.oi_values,
                  fun rk s rv ->
                    A.ordered_idx'
                      { o with oi_keys = rk; oi_scope = s; oi_values = rv } )
          | _ -> None
        in
        let rk_bnd = Set.of_list (module Name) (Schema.schema rk) in
        let pushed_key, pushed_val =
          Pred.conjuncts p
          |> List.partition_map ~f:(fun p ->
                 if Tactics_util.is_supported stage rk_bnd p then `Fst p
                 else `Snd p)
        in
        let pushed_val =
          List.map pushed_val ~f:(Pred.scoped (Set.to_list rk_bnd) scope)
        in
        mk (filter_many pushed_key rk) scope (filter_many pushed_val rv)

  let push_filter =
    (* NOTE: Simplify is necessary to make push-filter safe under fixpoints. *)
    seq'
      (of_func_pre push_filter ~name:"push-filter"
         ~pre:(Resolve.resolve_exn ~params))
      simplify

  let elim_eq_filter_src =
    let src = Logs.Src.create "elim-eq-filter" in
    Logs.Src.set_level src (Some Warning);
    src

  let contains_not p =
    let visitor =
      object (self)
        inherit [_] V.reduce

        inherit [_] Util.disj_monoid

        method! visit_Unop () op p =
          match op with Not -> true | _ -> self#visit_pred () p
      end
    in
    visitor#visit_pred () p

  let is_eq_subtree p =
    let visitor =
      object (self)
        inherit [_] V.reduce

        inherit [_] Util.conj_monoid

        method! visit_Binop () op p1 p2 =
          match op with
          | And | Or -> self#visit_pred () p1 && self#visit_pred () p2
          | Eq -> true
          | _ -> false

        method! visit_Unop () op p =
          match op with Not -> false | _ -> self#visit_pred () p
      end
    in
    visitor#visit_pred () p

  (** Domain computations for predicates containing conjunctions, disjunctions
     and equalities. *)
  module EqDomain = struct
    type domain =
      | And of domain * domain
      | Or of domain * domain
      | Domain of Ast.t
    [@@deriving compare]

    type t = domain Map.M(Pred).t

    let intersect d1 d2 =
      Map.merge d1 d2 ~f:(fun ~key:_ v ->
          let ret =
            match v with
            | `Both (d1, d2) ->
                if [%compare.equal: domain] d1 d2 then d1 else And (d1, d2)
            | `Left d | `Right d -> d
          in
          Some ret)

    let union d1 d2 =
      Map.merge d1 d2 ~f:(fun ~key:_ v ->
          let ret =
            match v with
            | `Both (d1, d2) ->
                if [%compare.equal: domain] d1 d2 then d1 else Or (d1, d2)
            | `Left d | `Right d -> d
          in
          Some ret)

    let rec of_pred r =
      let open Or_error.Let_syntax in
      function
      | Binop (And, p1, p2) ->
          let%bind ds1 = of_pred r p1 in
          let%map ds2 = of_pred r p2 in
          intersect ds1 ds2
      | Binop (Or, p1, p2) ->
          let%bind ds1 = of_pred r p1 in
          let%map ds2 = of_pred r p2 in
          union ds1 ds2
      | Binop (Eq, p1, p2) -> (
          match
            (Tactics_util.all_values [ p1 ] r, Tactics_util.all_values [ p2 ] r)
          with
          | _, Ok vs2 when is_candidate_key p1 r && is_candidate_match p2 r ->
              Ok (Map.singleton (module Pred) p1 (Domain vs2))
          | Ok vs1, _ when is_candidate_key p2 r && is_candidate_match p1 r ->
              Ok (Map.singleton (module Pred) p2 (Domain vs1))
          | _, Ok _ | Ok _, _ ->
              Or_error.error "No candidate keys." (p1, p2)
                [%sexp_of: Pred.t * Pred.t]
          | Error e1, Error e2 -> Error (Error.of_list [ e1; e2 ]) )
      | p ->
          Or_error.error "Not part of an equality predicate." p
            [%sexp_of: Pred.t]

    let to_ralgebra d =
      let schema r = List.hd_exn (Schema.schema r) in
      let rec extract = function
        | And (d1, d2) ->
            let e1 = extract d1 and e2 = extract d2 in
            let n1 = schema e1 and n2 = schema e2 in
            A.dedup
            @@ A.select [ Name n1 ]
            @@ A.join (Binop (Eq, Name n1, Name n2)) e1 e2
        | Or (d1, d2) ->
            let e1 = extract d1 and e2 = extract d2 in
            let n1 = schema e1 and n2 = schema e2 and n = fresh_name "x%d" in
            A.dedup
            @@ A.tuple
                 [
                   A.select [ P.as_ (Name n1) n ] e1;
                   A.select [ P.as_ (Name n2) n ] e2;
                 ]
                 Concat
        | Domain d ->
            let n = schema d and n' = fresh_name "x%d" in
            A.select [ P.as_ (Name n) n' ] d
      in
      Map.map d ~f:extract
  end

  let elim_eq_filter_limit = 3

  let elim_eq_check_limit n =
    if n > elim_eq_filter_limit then (
      Logs.info ~src:elim_eq_filter_src (fun m ->
          m "Would need to join too many relations (%d > %d)" n
            elim_eq_filter_limit);
      None )
    else Some ()

  let elim_eq_filter r =
    let open Option.Let_syntax in
    let%bind p, r = to_filter r in
    let orig_schema = Schema.schema r in

    (* Extract equalities from the filter predicate. *)
    let eqs, rest =
      Pred.to_nnf p |> Pred.conjuncts
      |> List.partition_map ~f:(fun p ->
             match EqDomain.of_pred r p with
             | Ok d -> `Fst (p, d)
             | Error e ->
                 Logs.info ~src:elim_eq_filter_src (fun m -> m "%a" Error.pp e);
                 `Snd p)
    in

    let inner, eqs = List.unzip eqs in
    let eqs = List.reduce ~f:EqDomain.intersect eqs
    and inner = Pred.conjoin inner in
    match eqs with
    | None ->
        Logs.info ~src:elim_eq_filter_src (fun m -> m "Found no equalities.");
        None
    | Some eqs ->
        let eqs = EqDomain.to_ralgebra eqs in
        let key, rels = Map.to_alist eqs |> List.unzip in

        let%map () = elim_eq_check_limit (List.length rels) in

        let r_keys = A.dedup (A.tuple rels Cross) in
        let scope = fresh_name "s%d" in
        let inner_filter_pred =
          let ctx =
            Map.map eqs ~f:(fun r ->
                Schema.schema r |> List.hd_exn |> Name.scoped scope |> P.name)
          in
          Pred.subst_tree ctx inner
        and select_list =
          Schema.to_select_list
          @@ List.dedup_and_sort ~compare:[%compare: Name.t] orig_schema
        in
        A.select select_list @@ filter_many rest
        @@ A.hash_idx r_keys scope (A.filter inner_filter_pred r) key

  let elim_eq_filter =
    seq' (of_func elim_eq_filter ~name:"elim-eq-filter") (try_ filter_const)

  let elim_disjunct r =
    let open Option.Let_syntax in
    let%bind p, r = to_filter r in
    let clauses = Pred.disjuncts p in
    if List.length clauses > 1 then
      if
        ( try
            Tactics_util.all_disjoint
              (List.map ~f:(Pred.to_static ~params) clauses)
              r
          with _ -> false )
        && List.length clauses > 1
      then Some (A.tuple (List.map clauses ~f:(fun p -> A.filter p r)) Concat)
      else None
    else None

  let elim_disjunct = of_func elim_disjunct ~name:"elim-disjunct"

  let eq_bound n ps =
    List.find_map
      ~f:(function
        | Binop (Eq, Name n', p) when Name.O.(n' = n) -> Some p
        | Binop (Eq, p, Name n') when Name.O.(n' = n) -> Some p
        | _ -> None)
      ps

  let to_lower_bound n ps =
    let cmp =
      if !enable_partition_cmp then
        List.find_map
          ~f:(function
            | Binop (Lt, Name n', p) when Name.O.(n' = n) -> Some p
            | Binop (Le, Name n', p) when Name.O.(n' = n) -> Some p
            | Binop (Gt, p, Name n') when Name.O.(n' = n) -> Some p
            | Binop (Ge, p, Name n') when Name.O.(n' = n) -> Some p
            | Binop (Lt, Binop (Add, Name n', p'), p) when Name.O.(n' = n) ->
                Some (Binop (Sub, p, p'))
            | Binop (Le, Binop (Add, Name n', p'), p) when Name.O.(n' = n) ->
                Some (Binop (Sub, p, p'))
            | Binop (Gt, p, Binop (Add, Name n', p')) when Name.O.(n' = n) ->
                Some (Binop (Sub, p, p'))
            | Binop (Ge, p, Binop (Add, Name n', p')) when Name.O.(n' = n) ->
                Some (Binop (Sub, p, p'))
            | _ -> None)
          ps
      else None
    in
    Option.first_some (eq_bound n ps) cmp

  let to_upper_bound n ps =
    let cmp =
      if !enable_partition_cmp then
        List.find_map
          ~f:(function
            | Binop (Lt, p, Name n') when Name.O.(n' = n) -> Some p
            | Binop (Le, p, Name n') when Name.O.(n' = n) -> Some p
            | Binop (Gt, Name n', p) when Name.O.(n' = n) -> Some p
            | Binop (Ge, Name n', p) when Name.O.(n' = n) -> Some p
            | Binop (Lt, p, Binop (Add, Name n', p')) when Name.O.(n' = n) ->
                Some (Binop (Add, p, p'))
            | Binop (Le, p, Binop (Add, Name n', p')) when Name.O.(n' = n) ->
                Some (Binop (Add, p, p'))
            | Binop (Gt, Binop (Add, Name n', p'), p) when Name.O.(n' = n) ->
                Some (Binop (Add, p, p'))
            | Binop (Ge, Binop (Add, Name n', p'), p) when Name.O.(n' = n) ->
                Some (Binop (Add, p, p'))
            | _ -> None)
          ps
      else None
    in
    Option.first_some (eq_bound n ps) cmp

  let to_range n ps = (to_lower_bound n ps, to_upper_bound n ps)

  let relevant_conjuncts r n =
    let visitor =
      object
        inherit [_] V.reduce as super

        inherit [_] Util.list_monoid

        method! visit_Filter () (p, r) =
          super#visit_Filter () (p, r)
          @ ( Pred.conjuncts p
            |> List.filter ~f:(fun p -> Set.mem (Pred.names p) n) )
      end
    in
    visitor#visit_t () r

  let subst_no_subquery ctx =
    let v =
      object
        inherit [_] V.endo

        method! visit_Name _ this v =
          match Map.find ctx v with Some x -> x | None -> this

        method! visit_Exists _ this _ = this

        method visit_First _ this _ = this
      end
    in
    v#visit_t ()

  let partition_with_bounds field aliases lo hi r n =
    let open Option.Let_syntax in
    let key_name = fresh_name "k%d" in
    let%bind keys =
      match Pred.to_type field with
      | IntT _ | DateT _ ->
          let%map vals = Tactics_util.all_values [ field ] r |> Or_error.ok in
          let vals =
            let val_name = List.hd_exn (Schema.schema vals) in
            let select_list =
              let alias_binds =
                List.filter_map aliases ~f:Fun.id
                |> List.map ~f:(fun n -> P.as_ (Name val_name) @@ Name.name n)
              in
              P.name val_name :: alias_binds
            in
            A.select select_list vals
          and scope = fresh_name "k%d" in

          let open P in
          A.dep_join
            (A.select [ as_ (Min lo) "lo"; as_ (Max hi) "hi" ] vals)
            scope
          @@ A.select [ as_ (name (Name.create "range")) key_name ]
          @@ A.range
               (name (Name.create ~scope "lo"))
               (name (Name.create ~scope "hi"))
      | StringT _ ->
          let%map keys = Tactics_util.all_values [ field ] r |> Or_error.ok in
          let select_list =
            [ P.(as_ (name (List.hd_exn (Schema.schema keys))) key_name) ]
          in
          A.select select_list keys
      | _ -> None
    in
    let scope = fresh_name "s%d" in
    let r' =
      let ctx =
        Map.singleton
          (module Name)
          n
          (P.name @@ Name.scoped scope @@ Name.create key_name)
      in
      subst_no_subquery ctx r
    in
    if Set.mem (A.names r') n then None
    else
      return
      @@ A.select Schema.(schema r' |> to_select_list)
      @@ A.hash_idx keys scope r' [ Name n ]

  (** Try to partition a layout on values of an attribute. *)
  let partition_on r n =
    let open Option.Let_syntax in
    let preds = relevant_conjuncts r n in
    let key_range = to_range n preds in
    let%bind fields =
      List.map preds ~f:(fun p -> Set.remove (Pred.names p) n)
      |> List.reduce ~f:Set.union |> Option.map ~f:Set.to_list
    in
    let fields =
      let m = Tactics_util.alias_map r in
      List.map fields ~f:(fun n ->
          (* If n is an alias for a field in a base relation, find the name of
             that field. *)
          match Map.find m n with
          | Some n' -> (n', Some n)
          | None -> (Name n, None))
      |> Map.of_alist_multi (module Pred)
      |> Map.to_alist
    in

    let fail msg =
      Logs.debug (fun m ->
          m "Partition: %s %a" msg (Fmt.Dump.list Pred.pp)
            (List.map ~f:Tuple.T2.get1 fields))
    in
    match (fields, key_range) with
    | [ (f, aliases) ], (Some l, Some h) ->
        partition_with_bounds f aliases l h r n
    | fs, (None, _ | _, None) ->
        fail "Could not find bounds for fields";
        None
    | fs, _ ->
        fail "Found too many fields";
        None

  let partition _ r =
    Set.to_sequence params |> Seq.filter_map ~f:(partition_on r)

  let partition = Branching.global partition ~name:"partition"

  let elim_subquery _ r =
    let open Option.Let_syntax in
    let can_hoist r = Set.is_subset (Free.free r) ~of_:params in

    let scope = fresh_name "s%d" in
    let visitor =
      object (self : 'self)
        inherit [_] V.mapreduce

        inherit [_] Util.list_monoid

        method! visit_AList () l =
          let rv, ret = self#visit_t () l.l_values in
          (AList { l with l_values = rv }, ret)

        method! visit_AHashIdx () h =
          let hi_values, ret = self#visit_t () h.hi_values in
          (AHashIdx { h with hi_values }, ret)

        method! visit_AOrderedIdx () o =
          let rv, ret = self#visit_t () o.oi_values in
          (AOrderedIdx { o with oi_values = rv }, ret)

        method! visit_AScalar () x = (AScalar x, [])

        method! visit_Exists () r =
          if can_hoist r then
            let qname = fresh_name "q%d" in
            ( Name (Name.create ~scope qname),
              [
                A.select [ As_pred (Exists r, qname) ]
                @@ A.scalar (As_pred (Int 0, "dummy"));
              ] )
          else (Exists r, [])

        method! visit_First () r =
          if can_hoist r then
            let qname = fresh_name "q%d" in
            ( Name (Name.create ~scope qname),
              [
                A.select [ As_pred (First r, qname) ]
                @@ A.scalar (As_pred (Int 0, "dummy"));
              ] )
          else (First r, [])
      end
    in
    let rhs, subqueries = visitor#visit_t () r in
    let%map lhs =
      match subqueries with
      | [] -> None
      | [ x ] -> Some x
      | xs -> Some (A.tuple subqueries Cross)
    in
    A.dep_join lhs scope rhs

  let elim_subquery = global elim_subquery "elim-subquery"

  let simplify r =
    let open Option.Let_syntax in
    match r.node with
    | Filter (p, r) -> return @@ A.filter (Pred.simplify p) r
    | Join { pred; r1; r2 } -> return @@ A.join (Pred.simplify pred) r1 r2
    | _ -> None

  let simplify = of_func ~name:"simplify" simplify

  (* let precompute_filter n =
   *   let exception Failed of Error.t in
   *   let run_exn r =
   *     M.annotate_schema r;
   *     match r.node with
   *     | Filter (p, r') ->
   *         let schema = Meta.(find_exn r' schema) in
   *         let free_vars =
   *           Set.diff (pred_free p) (Set.of_list (module Name) schema)
   *           |> Set.to_list
   *         in
   *         let free_var =
   *           match free_vars with
   *           | [ v ] -> v
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
   *                 ( subst_pred (Map.singleton (module Name) free_var v) p,
   *                   sprintf "%s_%d" witness_name i ))
   *         in
   *         let filter_pred =
   *           List.foldi values ~init:p ~f:(fun i else_ v ->
   *               If
   *                 ( Binop (Eq, Name free_var, v),
   *                   Name (Name.create (sprintf "%s_%d" witness_name i)),
   *                   else_ ))
   *         in
   *         let select_list = witnesses @ List.map schema ~f:(fun n -> Name n) in
   *         Some (filter filter_pred (select select_list r'))
   *     | _ -> None
   *   in
   *   let f r = try run_exn r with Failed _ -> None in
   *   of_func f ~name:"precompute-filter" *)
end
