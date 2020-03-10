open Ast
open Abslayout
open Collections
module A = Abslayout
module P = Pred.Infix
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

  let schema_set_exn r = Schema.schema r |> Set.of_list (module Name)

  (** Split predicates that sit under a binder into the parts that depend on
       bound variables and the parts that do not. *)
  let split_bound binder p =
    List.partition_tf (Pred.conjuncts p) ~f:(fun p' ->
        overlaps (pred_free p') (schema_set_exn binder))

  (** Check that a predicate is supported by a relation (it does not depend on
     anything in the context that it did not previously depend on.) *)
  let invariant_support orig_bound new_bound pred =
    let supported = Set.inter (pred_free pred) orig_bound in
    Set.is_subset supported ~of_:new_bound

  let merge_select s1 s2 =
    s1 @ s2
    |> List.dedup_and_sort ~compare:(fun p1 p2 ->
           [%compare: Name.t option] (Schema.to_name p1) (Schema.to_name p2))

  let hoist_filter r =
    let open Option.Let_syntax in
    match r.node with
    | OrderBy { key; rel } ->
        let%map p, r = to_filter rel in
        A.filter p (order_by key r)
    | GroupBy (ps, key, r) ->
        let%bind p, r = to_filter r in
        if
          invariant_support (schema_set_exn r)
            (schema_set_exn (group_by ps key r))
            p
        then Some (A.filter p (group_by ps key r))
        else None
    | Filter (p', r) ->
        let%map p, r = to_filter r in
        A.filter (Binop (And, p, p')) r
    | Select (ps, r) -> (
        let%bind p, r = to_filter r in
        match select_kind ps with
        | `Scalar ->
            if Tactics_util.select_contains (pred_free p) ps r then
              Some (A.filter p (select ps r))
            else None
        | `Agg -> None )
    | Join { pred; r1; r2 } -> (
        match (to_filter r1, to_filter r2) with
        | Some (p1, r1), Some (p2, r2) ->
            Some (A.filter (Pred.conjoin [ p1; p2 ]) (join pred r1 r2))
        | None, Some (p, r2) -> Some (A.filter p (join pred r1 r2))
        | Some (p, r1), None -> Some (A.filter p (join pred r1 r2))
        | None, None -> None )
    | Dedup r ->
        let%map p, r = to_filter r in
        A.filter p (dedup r)
    | AList (rk, rv) ->
        let%map p, r = to_filter rv in
        A.filter (Pred.unscoped (scope_exn rk) p) (list rk (scope_exn rk) r)
    | AHashIdx ({ hi_keys = rk; hi_values = rv; _ } as h) ->
        let%map p, r = to_filter rv in
        let below, above = split_bound rk p in
        let above = List.map above ~f:(Pred.unscoped h.hi_scope) in
        A.filter (Pred.conjoin above)
          (hash_idx' { h with hi_values = A.filter (Pred.conjoin below) r })
    | AOrderedIdx (rk, rv, m) ->
        let%map p, r = to_filter rv in
        let below, above = split_bound rk p in
        let above = List.map above ~f:(Pred.unscoped (scope_exn rk)) in
        A.filter (Pred.conjoin above)
          (ordered_idx rk (scope_exn rk) (A.filter (Pred.conjoin below) r) m)
    | DepJoin { d_lhs; d_alias; d_rhs } ->
        let%map p, r = to_filter d_rhs in
        (* Ensure all the required names are selected. *)
        let select_list =
          let lhs_schema = Schema.schema d_lhs in
          let lhs_select =
            lhs_schema |> Schema.to_select_list
            |> List.map ~f:(Pred.scoped lhs_schema d_alias)
          and rhs_select = Schema.(schema d_rhs |> to_select_list) in
          merge_select lhs_select rhs_select
        in
        A.filter (Pred.unscoped d_alias p)
          (dep_join d_lhs d_alias (select select_list r))
    | Relation _ | AEmpty | AScalar _ | ATuple _ | As (_, _) | Range _ -> None

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
        inherit [_] Abslayout.endo

        method! visit_Name () _ n = Name (Name.copy ~scope:(Some rn) n)
      end
    in
    visitor#visit_pred () p

  let gen_ordered_idx ?lb ?ub p rk rv =
    let k = fresh_name "k%d" in
    let n = fresh_name "x%d" in
    ordered_idx
      (dedup (select [ P.as_ p n ] rk))
      k
      (A.filter (Binop (Eq, Name (Name.create ~scope:k n), p)) rv)
      { oi_key_layout = None; oi_lookup = [ (lb, ub) ] }

  (** A predicate `p` is a candidate lookup key into a partitioning of `r` if it
     does not depend on any of the fields in `r`.

      TODO: In practice we also want it to have a parameter in it. Is this correct? *)
  let is_candidate_key p r =
    let pfree = pred_free p in
    (not (overlaps (schema_set_exn r) pfree)) && overlaps params pfree

  (** A predicate is a candidate to be matched if all its free variables are
     bound by the relation that it is above. *)
  let is_candidate_match p r =
    Set.is_subset (pred_free p) ~of_:(schema_set_exn r)

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
            select (Schema.to_select_list orig_schema)
            @@ ordered_idx all_keys scope
                 (A.filter
                    ( List.map key ~f:(fun p ->
                          P.(p = Pred.scoped keys_schema scope p))
                    |> Pred.conjoin )
                    r')
                 { oi_key_layout = None; oi_lookup = cmps }
        in
        match x with
        | Ok r -> Seq.singleton (A.filter (Pred.conjoin rest) r)
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
    let rs = List.mapi rs ~f:(fun i -> A.filter (Pred.conjoin preds.(i))) in
    A.filter (Pred.conjoin rest) (tuple rs Cross)

  let push_filter_list stage p rk rv =
    let scope = scope_exn rk in
    let rk = strip_scope rk in
    let rk_bnd = Set.of_list (module Name) (Schema.schema rk) in
    let pushed_key, pushed_val =
      Pred.conjuncts p
      |> List.partition_map ~f:(fun p ->
             if Tactics_util.is_supported stage rk_bnd p then `Fst p else `Snd p)
    in
    let inner_key_pred = Pred.conjoin pushed_key in
    let inner_val_pred = Pred.conjoin pushed_val in
    list (A.filter inner_key_pred rk) scope (A.filter inner_val_pred rv)

  let push_filter_select stage p ps r =
    match select_kind ps with
    | `Scalar ->
        let ctx =
          List.filter_map ps ~f:(fun p ->
              Option.map (Pred.to_name p) ~f:(fun n -> (n, Pred.remove_as p)))
          |> Map.of_alist_exn (module Name)
        in
        let p' = Pred.subst ctx p in
        select ps (A.filter p' r)
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
        A.filter (Pred.conjoin unpushed)
          (select ps (A.filter (Pred.conjoin pushed) r))

  let push_filter r =
    let open Option.Let_syntax in
    let stage = r.meta#stage in
    let r = strip_meta r in
    let%bind p, r = to_filter r in
    match r.node with
    | Filter (p', r') -> Some (A.filter (Binop (And, p, p')) r')
    | Dedup r' -> Some (dedup (A.filter p r'))
    | Select (ps, r') -> Some (push_filter_select stage p ps r')
    | ATuple (rs, Concat) -> Some (tuple (List.map rs ~f:(A.filter p)) Concat)
    | ATuple (rs, Cross) -> Some (push_filter_cross_tuple stage p rs)
    (* Lists are a special case because their keys are bound at compile time and
       are not available at runtime. *)
    | AList (rk, rv) -> Some (push_filter_list stage p rk rv)
    | _ ->
        let%map rk, scope, rv, mk =
          match r.node with
          | DepJoin { d_lhs = rk; d_rhs = rv; d_alias } ->
              Some (rk, d_alias, rv, dep_join)
          | AList (rk, rv) -> Some (strip_scope rk, scope_exn rk, rv, list)
          | AHashIdx h ->
              Some
                ( h.hi_keys,
                  h.hi_scope,
                  h.hi_values,
                  fun rk s rv ->
                    hash_idx'
                      { h with hi_keys = rk; hi_scope = s; hi_values = rv } )
          | AOrderedIdx (rk, rv, m) ->
              Some
                ( strip_scope rk,
                  scope_exn rk,
                  rv,
                  fun rk s rv -> ordered_idx rk s rv m )
          | _ -> None
        in
        let rk_bnd = Set.of_list (module Name) (Schema.schema rk) in
        let pushed_key, pushed_val =
          Pred.conjuncts p
          |> List.partition_map ~f:(fun p ->
                 if Tactics_util.is_supported stage rk_bnd p then `Fst p
                 else `Snd p)
        in
        let inner_key_pred = Pred.conjoin pushed_key in
        let inner_val_pred =
          let pushed_val =
            List.map pushed_val ~f:(Pred.scoped (Set.to_list rk_bnd) scope)
          in
          Pred.conjoin pushed_val
        in
        mk (A.filter inner_key_pred rk) scope (A.filter inner_val_pred rv)

  let push_filter =
    (* NOTE: Simplify is necessary to make push-filter safe under fixpoints. *)
    seq'
      (of_func_pre push_filter ~name:"push-filter"
         ~pre:(Resolve.resolve ~params))
      simplify

  let elim_eq_filter_src =
    let src = Logs.Src.create "elim-eq-filter" in
    Logs.Src.set_level src (Some Debug);
    src

  let contains_not p =
    let visitor =
      object (self)
        inherit [_] reduce

        inherit [_] Util.disj_monoid

        method! visit_Unop () op p =
          match op with Not -> true | _ -> self#visit_pred () p
      end
    in
    visitor#visit_pred () p

  let is_eq_subtree p =
    let visitor =
      object (self)
        inherit [_] reduce

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
            let e1 = extract d1 in
            let e2 = extract d2 in
            let n1 = schema e1 in
            let n2 = schema e2 in
            dedup
              (select [ Name n1 ] (join (Binop (Eq, Name n1, Name n2)) e1 e2))
        | Or (d1, d2) ->
            let e1 = extract d1 in
            let e2 = extract d2 in
            let n1 = schema e1 in
            let n2 = schema e2 in
            let n = fresh_name "x%d" in
            dedup
              (tuple
                 [
                   select [ P.as_ (Name n1) n ] e1;
                   select [ P.as_ (Name n2) n ] e2;
                 ]
                 Concat)
        | Domain d ->
            let n = schema d in
            let n' = fresh_name "x%d" in
            select [ P.as_ (Name n) n' ] d
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

        let r_keys = dedup (tuple rels Cross) in
        let scope = fresh_name "s%d" in
        let inner_filter_pred =
          let ctx =
            Map.map eqs ~f:(fun r ->
                Schema.schema r |> List.hd_exn |> Name.scoped scope |> P.name)
          in
          Pred.subst_tree ctx inner
        and outer_filter_pred = Pred.conjoin rest in
        select
          ( Schema.to_select_list
          @@ List.dedup_and_sort ~compare:[%compare: Name.t] orig_schema )
        @@ A.filter outer_filter_pred
        @@ hash_idx r_keys scope (A.filter inner_filter_pred r) key

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
      then Some (tuple (List.map clauses ~f:(fun p -> A.filter p r)) Concat)
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
        inherit [_] reduce as super

        inherit [_] Util.list_monoid

        method! visit_Filter () (p, r) =
          super#visit_Filter () (p, r)
          @ ( Pred.conjuncts p
            |> List.filter ~f:(fun p -> Set.mem (Pred.names p) n) )
      end
    in
    visitor#visit_t () r

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
    match (fields, key_range) with
    | [ (f, aliases) ], (Some l, Some h) ->
        let key_name = fresh_name "k%d" in
        let%bind keys =
          match Pred.to_type f with
          | IntT _ | DateT _ ->
              let%map vals = Tactics_util.all_values [ f ] r |> Or_error.ok in
              let vals =
                let val_name = List.hd_exn (Schema.schema vals) in
                let select_list =
                  let alias_binds =
                    List.filter_map aliases ~f:Fun.id
                    |> List.map ~f:(fun n ->
                           P.as_ (Name val_name) @@ Name.name n)
                  in
                  P.name val_name :: alias_binds
                in
                select select_list vals
              and scope = fresh_name "k%d" in
              let open P in
              dep_join
                (select [ as_ (Min l) "lo"; as_ (Max h) "hi" ] vals)
                scope
              @@ select [ as_ (name (Name.create "range")) key_name ]
              @@ range
                   (name (Name.create ~scope "lo"))
                   (name (Name.create ~scope "hi"))
          | StringT _ ->
              let%map keys = Tactics_util.all_values [ f ] r |> Or_error.ok in
              let select_list =
                [ P.(as_ (name (List.hd_exn (Schema.schema keys))) key_name) ]
              in
              select select_list keys
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
          subst ctx r
        in
        if Set.mem (names r') n then None
        else
          return
          @@ select Schema.(schema r' |> to_select_list)
          @@ hash_idx keys scope r' [ Name n ]
    | fs, _ ->
        Logs.debug (fun m ->
            m "Partition: Found too many fields. %a" (Fmt.list Pred.pp)
              (List.map ~f:Tuple.T2.get1 fs));
        None

  let partition _ r =
    Set.to_sequence params |> Seq.filter_map ~f:(partition_on r)

  let partition = Branching.global partition ~name:"partition"

  let elim_subquery _ r =
    let open Option.Let_syntax in
    let%bind p, r = to_filter r in
    let schema_names = Schema.schema r |> Set.of_list (module Name) in
    let visitor =
      object
        inherit [_] mapreduce

        inherit [_] Util.list_monoid

        method! visit_Exists () r =
          if Set.inter schema_names (names r) |> Set.is_empty then
            let qname = fresh_name "q%d" in
            ( Name (Name.create qname),
              [ select [ As_pred (Binop (Gt, Count, Int 0), qname) ] r ] )
          else (Exists r, [])

        method! visit_First () r =
          let n = Schema.schema r |> List.hd_exn in
          if Set.inter schema_names (names r) |> Set.is_empty then
            let qname = fresh_name "q%d" in
            ( Name (Name.create qname),
              [ select [ As_pred (Min (Name n), qname) ] r ] )
          else (Exists r, [])
      end
    in
    let p, subqueries = visitor#visit_pred () p in
    if List.length subqueries > 0 then
      let scope = fresh_name "s%d" in
      let sq_tuple = tuple subqueries Cross in
      Some
        (dep_join sq_tuple scope
           (A.filter (Pred.scoped (Schema.schema sq_tuple) scope p) r))
    else None

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
