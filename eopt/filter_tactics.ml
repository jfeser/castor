open Core
open Castor.Ast
open Castor.Collections
module Subst = Castor.Subst
module Schema = Castor.Schema
module Name = Castor.Name
module Free = Castor.Free
module Abslayout = Castor.Abslayout
module Pred = Castor.Pred
module Select_list = Castor.Select_list
module Fresh = Castor.Fresh
module Global = Castor.Global
module Egraph = Castor.Egraph
open Egraph_matcher
module P = Pred.Infix

(* (\** Enable partitioning when a parameter is used in a range predicate. *\) *)
(* let enable_partition_cmp = ref false *)

(* let fresh_name = Fresh.name Global.fresh *)
let schema_set r = Schema.schema r |> Set.of_list (module Name)
let free = Free.pred_free_open (fun _ -> Set.empty (module Name))

(** Split predicates that sit under a binder into the parts that depend on
       bound variables and the parts that don't. *)
let split_bound binder p =
  List.partition_tf (Pred.conjuncts p) ~f:(fun p' ->
      Set.overlaps (free p') (schema_set binder))

(** Check that a predicate is supported by a relation (it does not depend on
     anything in the context that it did not previously depend on.) *)
let invariant_support orig_bound new_bound pred =
  let supported = Set.inter (free pred) orig_bound in
  Set.is_subset supported ~of_:new_bound

let hoist_filter_orderby g _ =
  let%bind root, { key; rel } = M.any_orderby g in
  let%map p, r = M.filter g rel in
  (root, C.filter g p (C.order_by g key r))

let hoist_filter_groupby g _ =
  let%bind root, (ps, key, r) = M.any_groupby g in
  let%bind p, r = M.filter g r in
  if
    invariant_support
      (schema_set @@ to_annot g r)
      (schema_set (to_annot g @@ C.group_by g ps key r))
      p
  then return (root, C.filter g p (C.group_by g ps key r))
  else empty

let hoist_filter_filter g _ =
  let%bind root, (p', r) = M.any_filter g in
  let%map p, r = M.filter g r in
  (root, C.filter g P.(p && p') r)

let hoist_filter_select g _ =
  let%bind root, (ps, r) = M.any_select g in
  let%bind p, r = M.filter g r in
  match Abslayout.select_kind ps with
  | `Scalar ->
      if Tactics_util.select_contains (free p) ps (to_annot g r) then
        return (root, C.filter g p (C.select g ps r))
      else empty
  | `Agg -> empty

let hoist_filter_join_both g _ =
  let%bind root, { pred; r1; r2 } = M.any_join g in
  let%bind p1, r1 = M.filter g r1 in
  let%map p2, r2 = M.filter g r2 in
  (root, C.filter g P.(p1 && p2) (C.join g pred r1 r2))

let hoist_filter_join_left g _ =
  let%bind root, { pred; r1; r2 } = M.any_join g in
  let%map p, r1 = M.filter g r1 in
  (root, C.filter g p (C.join g pred r1 r2))

let hoist_filter_join_right g _ =
  let%bind root, { pred; r1; r2 } = M.any_join g in
  let%map p, r2 = M.filter g r2 in
  (root, C.filter g p (C.join g pred r1 r2))

let hoist_filter_dedup g _ =
  let%bind root, r = M.any_dedup g in
  let%map p, r = M.filter g r in
  (root, C.filter g p (C.dedup g r))

let hoist_filter_list g _ =
  let%bind root, l = M.any_list g in
  let%bind p, r = M.filter g l.l_values in
  (* checks that p doesn't refer to anything bound by l *)
  match Subst.decr_pred p with
  | Some p' -> return (root, C.filter g p' (C.list g { l with l_values = r }))
  | None -> empty

let hoist_filter_hashidx g _ =
  let%bind root, h = M.any_hashidx g in
  let%bind p, r = M.filter g h.hi_values in
  let below, above = split_bound (to_annot g h.hi_keys) p in
  match List.map ~f:Subst.decr_pred above |> Option.all with
  | Some above ->
      return
        ( root,
          C.filter g (P.and_ above)
          @@ C.hash_idx g { h with hi_values = C.filter g (P.and_ below) r } )
  | None -> empty

let hoist_filter_orderedidx g _ =
  let%bind root, o = M.any_orderedidx g in
  let%bind p, r = M.filter g o.oi_values in
  let below, above = split_bound (to_annot g o.oi_keys) p in
  match List.map ~f:Subst.decr_pred above |> Option.all with
  | Some above ->
      return
        ( root,
          C.filter g (P.and_ above)
          @@ C.ordered_idx g { o with oi_values = C.filter g (P.and_ below) r }
        )
  | None -> empty

let hoist_filter =
  Ops.all
    [
      hoist_filter_orderby;
      hoist_filter_groupby;
      hoist_filter_filter;
      hoist_filter_select;
      hoist_filter_join_left;
      hoist_filter_join_both;
      hoist_filter_join_right;
      hoist_filter_dedup;
      hoist_filter_list;
      hoist_filter_hashidx;
      hoist_filter_orderedidx;
    ]

let hoist_filter_agg g ctx =
  let params = Univ_map.find_exn ctx Ops.params in
  let%bind root, (ps, r) = M.any_select g in
  let%bind p, r = M.filter g r in
  match Abslayout.select_kind ps with
  | `Scalar -> empty
  | `Agg ->
      if
        Tactics_util.select_contains
          (Set.diff (free p) params)
          ps (to_annot g r)
      then return (root, C.filter g p (C.select g ps r))
      else empty

let split_filter g _ =
  let%bind root, (p, r) = M.any_filter g in
  match p with
  | `Binop (And, p, p') -> return (root, C.filter g p (C.filter g p' r))
  | _ -> empty

(** A predicate `p` is a candidate lookup key into a partitioning of `r` if it
     does not depend on any of the fields in `r`.

      TODO: In practice we also want it to have a parameter in it. Is this correct? *)
let is_candidate_key params p r_schema =
  let pfree = free p in
  (not (Set.overlaps (Set.of_list (module Name) r_schema) pfree))
  && Set.overlaps params pfree

(** A predicate is a candidate to be matched if all its free variables are
     bound by the relation that it is above. *)
let is_candidate_match p r_schema =
  Set.is_subset (free p) ~of_:(Set.of_list (module Name) r_schema)

module EPred = struct
  module T = struct
    type t = Egraph.Id.t pred [@@deriving compare, sexp_of]
  end

  include T
  include Comparator.Make (T)
end

let elim_cmp_filter g ctx =
  let params = Univ_map.find_exn ctx Ops.params in
  let%bind root, (p, r') = M.any_filter g in
  let orig_schema = Schema.schema (to_annot g r') in

  (* Select the comparisons which have a parameter on exactly one side and
     partition by the unparameterized side of the comparison. *)
  let cmps, rest =
    Pred.conjuncts p
    |> List.partition_map ~f:(function
         | (`Binop (Binop.Gt, p1, p2) | `Binop (Lt, p2, p1)) as p ->
             if
               is_candidate_key params p1 orig_schema
               && is_candidate_match p2 orig_schema
             then First (p2, (`Lt, p1))
             else if
               is_candidate_key params p2 orig_schema
               && is_candidate_match p1 orig_schema
             then First (p1, (`Gt, p2))
             else Second p
         | (`Binop (Ge, p1, p2) | `Binop (Le, p2, p1)) as p ->
             if
               is_candidate_key params p1 orig_schema
               && is_candidate_match p2 orig_schema
             then First (p2, (`Le, p1))
             else if
               is_candidate_key params p2 orig_schema
               && is_candidate_match p1 orig_schema
             then First (p1, (`Ge, p2))
             else Second p
         | p -> Second p)
  in
  let cmps, rest' =
    Map.of_alist_multi (module EPred) cmps
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
             match (List.length open_lb = 0, List.length closed_lb = 0) with
             | true, true -> (None, [])
             | _, true ->
                 ( Option.map (List.reduce ~f:Pred.max_of open_lb)
                     ~f:(fun max -> (max, `Open)),
                   [] )
             | _ ->
                 ( Option.map
                     (List.reduce ~f:Pred.max_of (open_lb @ closed_lb))
                     ~f:(fun max -> (max, `Closed)),
                   List.map open_lb ~f:(fun v -> Pred.Infix.(key > v)) )
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
             match (List.length open_ub = 0, List.length closed_ub = 0) with
             | true, true -> (None, [])
             | _, true ->
                 ( Option.map (List.reduce ~f:Pred.min_of open_ub) ~f:(fun p ->
                       (p, `Open)),
                   [] )
             | _ ->
                 ( Option.map
                     (List.reduce ~f:Pred.min_of (open_ub @ closed_ub))
                     ~f:(fun p -> (p, `Closed)),
                   List.map open_ub ~f:(fun v -> Pred.Infix.(key > v)) )
           in
           ((key, (lb, ub)), rest @ rest'))
    |> List.unzip
  in
  let rest = rest @ List.concat rest' in
  let key, cmps = List.unzip cmps in
  let x =
    let open Or_error.Let_syntax in
    if List.is_empty key then Or_error.error_string "No candidate keys found."
    else
      let key =
        List.map key ~f:(function
          | `Name n -> n
          | _ -> failwith "expected a name")
      in
      let%map all_keys =
        Tactics_util.all_values_precise (Select_list.of_names key)
          (to_annot g r')
      in
      C.select g (Schema.to_select_list orig_schema)
      @@ C.ordered_idx g
           {
             oi_keys = of_annot g all_keys;
             oi_values =
               C.filter g
                 (P.and_
                 @@ List.map key ~f:(fun p -> P.(name p = name (Name.zero p))))
                 (of_annot g @@ Subst.incr @@ to_annot g r');
             oi_key_layout = None;
             oi_lookup = cmps;
           }
  in
  match x with
  | Ok r -> return (root, C.filter g (P.and_ rest) r)
  | Error err ->
      Logs.warn (fun m -> m "Elim-cmp: %a" Error.pp err);
      empty

let elim_cmp_filter = Ops.register elim_cmp_filter "elim-cmp-filter"

(* (\** Eliminate a filter with one parameter and one attribute. *\) *)
(* let elim_simple_filter r = *)
(*   let open Option.Let_syntax in *)
(*   let%bind p, r = to_filter r in *)
(*   let%bind r = if Is_serializable.is_static ~params r then Some r else None in *)
(*   let names = Pred.names p |> Set.to_list in *)

(*   let is_param = Set.mem params in *)
(*   let is_field n = *)
(*     Db.relation_has_field C.conn (Name.name n) |> Option.is_some *)
(*   in *)
(*   let%bind _param, attr = *)
(*     match names with *)
(*     | [ n1; n2 ] when is_param n1 && is_field n2 -> return (n1, n2) *)
(*     | [ n2; n1 ] when is_param n1 && is_field n2 -> return (n1, n2) *)
(*     | _ -> None *)
(*   in *)
(*   let scope = Fresh.name Global.fresh "s%d" in *)
(*   let sattr = Name.scoped scope attr in *)
(*   let select_list = *)
(*     Schema.schema r *)
(*     |> List.filter ~f:(fun n -> Name.O.(n <> attr)) *)
(*     |> Schema.to_select_list *)
(*   in *)
(*   return *)
(*   @@ A.list (A.dedup @@ A.select (Select_list.of_names [ attr ]) r) scope *)
(*   @@ A.tuple *)
(*        [ *)
(*          A.filter p @@ A.scalar_n sattr; *)
(*          A.select select_list @@ A.filter P.(`Name attr = `Name sattr) r; *)
(*        ] *)
(*        Cross *)

(* let elim_simple_filter = *)
(*   of_func_pre ~pre:Is_serializable.annotate_stage elim_simple_filter *)
(*     ~name:"elim-simple-filter" *)

(* let push_filter_cross_tuple stage p rs = *)
(*   let ps = Pred.conjuncts p in *)
(*   (\* Find the earliest placement for each predicate. *\) *)
(*   let preds = Array.create ~len:(List.length rs) [] in *)
(*   let rec place_all ps i = *)
(*     if i >= List.length rs then ps *)
(*     else *)
(*       let bnd = *)
(*         List.nth_exn rs i |> Schema.schema |> Set.of_list (module Name) *)
(*       in *)
(*       let pl, up = *)
(*         List.partition_tf ps ~f:(Tactics_util.is_supported stage bnd) *)
(*       in *)
(*       preds.(i) <- pl; *)
(*       place_all up (i + 1) *)
(*   in *)
(*   let rest = place_all ps 0 in *)
(*   let rs = List.mapi rs ~f:(fun i -> filter_many preds.(i)) in *)
(*   filter_many rest (A.tuple rs Cross) *)

(* let push_filter_list stage p l = *)
(*   let rk_bnd = Set.of_list (module Name) (Schema.schema l.l_keys) in *)
(*   let pushed_key, pushed_val = *)
(*     Pred.conjuncts p *)
(*     |> List.partition_map ~f:(fun p -> *)
(*            if Tactics_util.is_supported stage rk_bnd p then First p *)
(*            else Second p) *)
(*   in *)
(*   A.list *)
(*     (filter_many pushed_key l.l_keys) *)
(*     l.l_scope *)
(*     (filter_many pushed_val l.l_values) *)

(* let push_filter_select stage p ps r = *)
(*   match Abslayout.select_kind ps with *)
(*   | `Scalar -> *)
(*       let ctx = *)
(*         List.map ps ~f:(fun (p, n) -> (Name.create n, p)) *)
(*         |> Map.of_alist_exn (module Name) *)
(*       in *)
(*       let p' = Pred.subst ctx p in *)
(*       A.select ps (A.filter p' r) *)
(*   | `Agg -> *)
(*       let scalar_ctx = *)
(*         List.filter_map ps ~f:(fun (p, n) -> *)
(*             if Poly.(Pred.kind p = `Scalar) then Some (Name.create n, p) *)
(*             else None) *)
(*         |> Map.of_alist_exn (module Name) *)
(*       in *)
(*       let names = Map.keys scalar_ctx |> Set.of_list (module Name) in *)
(*       let pushed, unpushed = *)
(*         Pred.conjuncts p *)
(*         |> List.partition_map ~f:(fun p -> *)
(*                if Tactics_util.is_supported stage names p then *)
(*                  First (Pred.subst scalar_ctx p) *)
(*                else Second p) *)
(*       in *)
(*       filter_many unpushed @@ A.select ps @@ filter_many pushed r *)

(* let push_filter r = *)
(*   let open Option.Let_syntax in *)
(*   let stage = r.meta#stage in *)
(*   let r = strip_meta r in *)
(*   let%bind p, r = to_filter r in *)
(*   match r.node with *)
(*   | Filter (p', r') -> Some (A.filter (`Binop (And, p, p')) r') *)
(*   | Dedup r' -> Some (A.dedup (A.filter p r')) *)
(*   | Select (ps, r') -> Some (push_filter_select stage p ps r') *)
(*   | ATuple (rs, Concat) -> Some (A.tuple (List.map rs ~f:(A.filter p)) Concat) *)
(*   | ATuple (rs, Cross) -> Some (push_filter_cross_tuple stage p rs) *)
(*   (\* Lists are a special case because their keys are bound at compile time and *)
(*      are not available at runtime. *\) *)
(*   | AList l -> Some (push_filter_list stage p l) *)
(*   | _ -> *)
(*       let%map rk, scope, rv, mk = *)
(*         match r.node with *)
(*         | DepJoin { d_lhs = rk; d_rhs = rv; d_alias } -> *)
(*             Some (rk, d_alias, rv, A.dep_join) *)
(*         | AList l -> Some (l.l_keys, l.l_scope, l.l_values, A.list) *)
(*         | AHashIdx h -> *)
(*             Some *)
(*               ( h.hi_keys, *)
(*                 h.hi_scope, *)
(*                 h.hi_values, *)
(*                 fun rk s rv -> *)
(*                   A.hash_idx' *)
(*                     { h with hi_keys = rk; hi_scope = s; hi_values = rv } ) *)
(*         | AOrderedIdx o -> *)
(*             Some *)
(*               ( o.oi_keys, *)
(*                 o.oi_scope, *)
(*                 o.oi_values, *)
(*                 fun rk s rv -> *)
(*                   A.ordered_idx' *)
(*                     { o with oi_keys = rk; oi_scope = s; oi_values = rv } ) *)
(*         | _ -> None *)
(*       in *)
(*       let rk_bnd = Set.of_list (module Name) (Schema.schema rk) in *)
(*       let pushed_key, pushed_val = *)
(*         Pred.conjuncts p *)
(*         |> List.partition_map ~f:(fun p -> *)
(*                if Tactics_util.is_supported stage rk_bnd p then First p *)
(*                else Second p) *)
(*       in *)
(*       let pushed_val = *)
(*         List.map pushed_val ~f:(Pred.scoped (Set.to_list rk_bnd) scope) *)
(*       in *)
(*       mk (filter_many pushed_key rk) scope (filter_many pushed_val rv) *)

(* let push_filter = *)
(*   (\* NOTE: Simplify is necessary to make push-filter safe under fixpoints. *\) *)
(*   seq' *)
(*     (of_func_cond ~name:"push-filter" *)
(*        ~pre:(fun r -> Some (Resolve.resolve_exn ~params r)) *)
(*        push_filter *)
(*        ~post:(fun r -> Resolve.resolve ~params r |> Result.ok)) *)
(*     simplify *)

(* let elim_eq_filter_src = *)
(*   let src = Logs.Src.create "elim-eq-filter" in *)
(*   Logs.Src.set_level src (Some Warning); *)
(*   src *)

(* let contains_not p = *)
(*   let visitor = *)
(*     object (self) *)
(*       inherit [_] V.reduce *)
(*       inherit [_] Util.disj_monoid *)

(*       method! visit_Unop () op p = *)
(*         match op with Not -> true | _ -> self#visit_pred () p *)
(*     end *)
(*   in *)
(*   visitor#visit_pred () p *)

(* let is_eq_subtree p = *)
(*   let visitor = *)
(*     object (self) *)
(*       inherit [_] V.reduce *)
(*       inherit [_] Util.conj_monoid *)

(*       method! visit_Binop () op p1 p2 = *)
(*         match op with *)
(*         | And | Or -> self#visit_pred () p1 && self#visit_pred () p2 *)
(*         | Eq -> true *)
(*         | _ -> false *)

(*       method! visit_Unop () op p = *)
(*         match op with Not -> false | _ -> self#visit_pred () p *)
(*     end *)
(*   in *)
(*   visitor#visit_pred () p *)

(* (\** Domain computations for predicates containing conjunctions, disjunctions *)
(*      and equalities. *\) *)
(* module EqDomain = struct *)
(*   type domain = *)
(*     | And of domain * domain *)
(*     | Or of domain * domain *)
(*     | Domain of Ast.t *)
(*   [@@deriving compare] *)

(*   type t = domain Map.M(Pred).t *)

(*   let intersect d1 d2 = *)
(*     Map.merge d1 d2 ~f:(fun ~key:_ v -> *)
(*         let ret = *)
(*           match v with *)
(*           | `Both (d1, d2) -> *)
(*               if [%compare.equal: domain] d1 d2 then d1 else And (d1, d2) *)
(*           | `Left d | `Right d -> d *)
(*         in *)
(*         Some ret) *)

(*   let union d1 d2 = *)
(*     Map.merge d1 d2 ~f:(fun ~key:_ v -> *)
(*         let ret = *)
(*           match v with *)
(*           | `Both (d1, d2) -> *)
(*               if [%compare.equal: domain] d1 d2 then d1 else Or (d1, d2) *)
(*           | `Left d | `Right d -> d *)
(*         in *)
(*         Some ret) *)

(*   let rec of_pred r : _ pred -> _ = *)
(*     let open Or_error.Let_syntax in *)
(*     function *)
(*     | `Binop (And, p1, p2) -> *)
(*         let%bind ds1 = of_pred r p1 in *)
(*         let%map ds2 = of_pred r p2 in *)
(*         intersect ds1 ds2 *)
(*     | `Binop (Or, p1, p2) -> *)
(*         let%bind ds1 = of_pred r p1 in *)
(*         let%map ds2 = of_pred r p2 in *)
(*         union ds1 ds2 *)
(*     | `Binop (Eq, p1, p2) -> ( *)
(*         match *)
(*           ( Tactics_util.all_values [ (p1, "p1") ] r, *)
(*             Tactics_util.all_values [ (p2, "p2") ] r ) *)
(*         with *)
(*         | _, Ok vs2 when is_candidate_key p1 r && is_candidate_match p2 r -> *)
(*             Ok (Map.singleton (module Pred) p1 (Domain vs2)) *)
(*         | Ok vs1, _ when is_candidate_key p2 r && is_candidate_match p1 r -> *)
(*             Ok (Map.singleton (module Pred) p2 (Domain vs1)) *)
(*         | _, Ok _ | Ok _, _ -> *)
(*             Or_error.error "No candidate keys." (p1, p2) *)
(*               [%sexp_of: Pred.t * Pred.t] *)
(*         | Error e1, Error e2 -> Error (Error.of_list [ e1; e2 ])) *)
(*     | p -> *)
(*         Or_error.error "Not part of an equality predicate." p [%sexp_of: Pred.t] *)

(*   let to_ralgebra d = *)
(*     let schema r = List.hd_exn (Schema.schema r) in *)
(*     let rec extract = function *)
(*       | And (d1, d2) -> *)
(*           let e1 = extract d1 and e2 = extract d2 in *)
(*           let n1 = schema e1 and n2 = schema e2 in *)
(*           A.dedup *)
(*           @@ A.select (Select_list.of_names [ n1 ]) *)
(*           @@ A.join (`Binop (Eq, `Name n1, `Name n2)) e1 e2 *)
(*       | Or (d1, d2) -> *)
(*           let e1 = extract d1 and e2 = extract d2 in *)
(*           let n1 = schema e1 and n2 = schema e2 and n = fresh_name "x%d" in *)
(*           A.dedup *)
(*           @@ A.tuple *)
(*                [ A.select [ (`Name n1, n) ] e1; A.select [ (`Name n2, n) ] e2 ] *)
(*                Concat *)
(*       | Domain d -> *)
(*           let n = schema d and n' = fresh_name "x%d" in *)
(*           A.select [ (`Name n, n') ] d *)
(*     in *)
(*     Map.map d ~f:extract *)
(* end *)

(* let elim_eq_filter_limit = 3 *)

(* let elim_eq_check_limit n = *)
(*   if n > elim_eq_filter_limit then ( *)
(*     Logs.info ~src:elim_eq_filter_src (fun m -> *)
(*         m "Would need to join too many relations (%d > %d)" n *)
(*           elim_eq_filter_limit); *)
(*     None) *)
(*   else Some () *)

(* let elim_eq_filter r = *)
(*   let open Option.Let_syntax in *)
(*   let%bind p, r = to_filter r in *)
(*   let orig_schema = Schema.schema r in *)

(*   (\* Extract equalities from the filter predicate. *\) *)
(*   let eqs, rest = *)
(*     Pred.to_nnf p |> Pred.conjuncts *)
(*     |> List.partition_map ~f:(fun p -> *)
(*            match EqDomain.of_pred r p with *)
(*            | Ok d -> First (p, d) *)
(*            | Error e -> *)
(*                Logs.info ~src:elim_eq_filter_src (fun m -> m "%a" Error.pp e); *)
(*                Second p) *)
(*   in *)

(*   let inner, eqs = List.unzip eqs in *)
(*   let eqs = List.reduce ~f:EqDomain.intersect eqs *)
(*   and inner = Pred.conjoin inner in *)
(*   match eqs with *)
(*   | None -> *)
(*       Logs.info ~src:elim_eq_filter_src (fun m -> m "Found no equalities."); *)
(*       None *)
(*   | Some eqs -> *)
(*       let eqs = EqDomain.to_ralgebra eqs in *)
(*       let key, rels = Map.to_alist eqs |> List.unzip in *)

(*       let%map () = elim_eq_check_limit (List.length rels) in *)

(*       let r_keys = A.dedup (A.tuple rels Cross) in *)
(*       let scope = fresh_name "s%d" in *)
(*       let inner_filter_pred = *)
(*         let ctx = *)
(*           Map.map eqs ~f:(fun r -> *)
(*               Schema.schema r |> List.hd_exn |> Name.scoped scope |> P.name) *)
(*         in *)
(*         Pred.subst_tree ctx inner *)
(*       and select_list = Schema.to_select_list orig_schema in *)
(*       A.select select_list @@ filter_many rest *)
(*       @@ A.hash_idx r_keys scope (A.filter inner_filter_pred r) key *)

(* let elim_eq_filter = *)
(*   seq' (of_func elim_eq_filter ~name:"elim-eq-filter") (try_ filter_const) *)

(* let elim_disjunct r = *)
(*   let open Option.Let_syntax in *)
(*   let%bind p, r = to_filter r in *)
(*   let clauses = Pred.disjuncts p in *)
(*   if List.length clauses > 1 then *)
(*     let%bind all_disjoint = *)
(*       Tactics_util.all_disjoint (List.map ~f:(Pred.to_static ~params) clauses) r *)
(*       |> Or_error.ok *)
(*     in *)
(*     if all_disjoint && List.length clauses > 1 then *)
(*       Some (A.tuple (List.map clauses ~f:(fun p -> A.filter p r)) Concat) *)
(*     else None *)
(*   else None *)

(* let elim_disjunct = of_func elim_disjunct ~name:"elim-disjunct" *)

(* let eq_bound n (ps : _ pred list) = *)
(*   List.find_map *)
(*     ~f:(function *)
(*       | `Binop (Eq, `Name n', p) when Name.O.(n' = n) -> Some p *)
(*       | `Binop (Eq, p, `Name n') when Name.O.(n' = n) -> Some p *)
(*       | _ -> None) *)
(*     ps *)

(* let to_lower_bound n (ps : _ pred list) = *)
(*   let cmp = *)
(*     if !enable_partition_cmp then *)
(*       List.find_map *)
(*         ~f:(function *)
(*           | `Binop (Lt, `Name n', p) when Name.O.(n' = n) -> Some p *)
(*           | `Binop (Le, `Name n', p) when Name.O.(n' = n) -> Some p *)
(*           | `Binop (Gt, p, `Name n') when Name.O.(n' = n) -> Some p *)
(*           | `Binop (Ge, p, `Name n') when Name.O.(n' = n) -> Some p *)
(*           | `Binop (Lt, `Binop (Add, `Name n', p'), p) when Name.O.(n' = n) -> *)
(*               Some (`Binop (Sub, p, p')) *)
(*           | `Binop (Le, `Binop (Add, `Name n', p'), p) when Name.O.(n' = n) -> *)
(*               Some (`Binop (Sub, p, p')) *)
(*           | `Binop (Gt, p, `Binop (Add, `Name n', p')) when Name.O.(n' = n) -> *)
(*               Some (`Binop (Sub, p, p')) *)
(*           | `Binop (Ge, p, `Binop (Add, `Name n', p')) when Name.O.(n' = n) -> *)
(*               Some (`Binop (Sub, p, p')) *)
(*           | _ -> None) *)
(*         ps *)
(*     else None *)
(*   in *)
(*   Option.first_some (eq_bound n ps) cmp *)

(* let to_upper_bound n (ps : _ pred list) = *)
(*   let cmp = *)
(*     if !enable_partition_cmp then *)
(*       List.find_map *)
(*         ~f:(function *)
(*           | `Binop (Lt, p, `Name n') when Name.O.(n' = n) -> Some p *)
(*           | `Binop (Le, p, `Name n') when Name.O.(n' = n) -> Some p *)
(*           | `Binop (Gt, `Name n', p) when Name.O.(n' = n) -> Some p *)
(*           | `Binop (Ge, `Name n', p) when Name.O.(n' = n) -> Some p *)
(*           | `Binop (Lt, p, `Binop (Add, `Name n', p')) when Name.O.(n' = n) -> *)
(*               Some (`Binop (Add, p, p')) *)
(*           | `Binop (Le, p, `Binop (Add, `Name n', p')) when Name.O.(n' = n) -> *)
(*               Some (`Binop (Add, p, p')) *)
(*           | `Binop (Gt, `Binop (Add, `Name n', p'), p) when Name.O.(n' = n) -> *)
(*               Some (`Binop (Add, p, p')) *)
(*           | `Binop (Ge, `Binop (Add, `Name n', p'), p) when Name.O.(n' = n) -> *)
(*               Some (`Binop (Add, p, p')) *)
(*           | _ -> None) *)
(*         ps *)
(*     else None *)
(*   in *)
(*   Option.first_some (eq_bound n ps) cmp *)

(* let to_range n ps = (to_lower_bound n ps, to_upper_bound n ps) *)

(* let relevant_conjuncts r n = *)
(*   let visitor = *)
(*     object *)
(*       inherit [_] V.reduce as super *)
(*       inherit [_] Util.list_monoid *)

(*       method! visit_Filter () (p, r) = *)
(*         super#visit_Filter () (p, r) *)
(*         @ (Pred.conjuncts p *)
(*           |> List.filter ~f:(fun p -> Set.mem (Pred.names p) n)) *)
(*     end *)
(*   in *)
(*   visitor#visit_t () r *)

(* let subst_no_subquery ctx = *)
(*   let v = *)
(*     object *)
(*       inherit [_] V.endo *)

(*       method! visit_Name _ this v = *)
(*         match Map.find ctx v with Some x -> x | None -> this *)

(*       method! visit_Exists _ this _ = this *)
(*       method! visit_First _ this _ = this *)
(*     end *)
(*   in *)
(*   v#visit_t () *)

(* let partition_with_bounds field aliases lo hi r n = *)
(*   let open Option.Let_syntax in *)
(*   let key_name = fresh_name "k%d" in *)
(*   let%bind keys = *)
(*     match Pred.to_type field with *)
(*     | IntT _ | DateT _ -> *)
(*         let%bind field_name = Match.Pred.to_name field in *)
(*         let%map vals = *)
(*           Tactics_util.all_values [ (field, Name.name field_name) ] r *)
(*           |> Or_error.ok *)
(*         in *)
(*         let vals = *)
(*           let select_list = *)
(*             let alias_binds = List.filter_map aliases ~f:Fun.id in *)
(*             Select_list.of_names (field_name :: alias_binds) *)
(*           in *)
(*           A.select select_list vals *)
(*         and scope = fresh_name "k%d" in *)

(*         let open P in *)
(*         A.dep_join (A.select [ (`Min lo, "lo"); (`Max hi, "hi") ] vals) scope *)
(*         @@ A.select [ (name (Name.create "range"), key_name) ] *)
(*         @@ A.range *)
(*              (name (Name.create ~scope "lo")) *)
(*              (name (Name.create ~scope "hi")) *)
(*     | StringT _ -> *)
(*         let%map keys = *)
(*           Tactics_util.all_values [ (field, key_name) ] r |> Or_error.ok *)
(*         in *)
(*         A.select (Select_list.of_names [ Name.create key_name ]) keys *)
(*     | _ -> None *)
(*   in *)
(*   let scope = fresh_name "s%d" in *)
(*   let r' = *)
(*     let ctx = *)
(*       Map.singleton *)
(*         (module Name) *)
(*         n *)
(*         (P.name @@ Name.scoped scope @@ Name.create key_name) *)
(*     in *)
(*     subst_no_subquery ctx r *)
(*   in *)
(*   if Set.mem (Abslayout.names r') n then None *)
(*   else *)
(*     return *)
(*     @@ A.select Schema.(schema r' |> to_select_list) *)
(*     @@ A.hash_idx keys scope r' [ `Name n ] *)

(* let exists_correlated_subquery r n = *)
(*   let zero = false and plus = ( || ) in *)
(*   let rec annot in_subquery r = *)
(*     V.Reduce.annot zero plus (query in_subquery) meta r *)
(*   and meta _ = zero *)
(*   and pred in_subquery = function *)
(*     | `Name n' -> in_subquery && [%compare.equal: Name.t] n n' *)
(*     | `Exists r | `First r -> annot true r *)
(*     | p -> V.Reduce.pred zero plus (annot in_subquery) (pred in_subquery) p *)
(*   and query in_subquery q = *)
(*     V.Reduce.query zero plus (annot in_subquery) (pred in_subquery) q *)
(*   in *)
(*   annot false r *)

(* (\** Try to partition a layout on values of an attribute. *\) *)
(* let partition_on r n = *)
(*   let open Option.Let_syntax in *)
(*   let preds = relevant_conjuncts r n in *)
(*   let key_range = to_range n preds in *)
(*   let%bind fields = *)
(*     List.map preds ~f:(fun p -> Set.remove (Pred.names p) n) *)
(*     |> List.reduce ~f:Set.union |> Option.map ~f:Set.to_list *)
(*   in *)
(*   let fields = *)
(*     let m = Tactics_util.alias_map r in *)
(*     List.map fields ~f:(fun n -> *)
(*         (\* `If n is an alias for a field in a base relation, find the name of *)
(*            that field. *\) *)
(*         match Map.find m n with *)
(*         | Some n' -> (n', Some n) *)
(*         | None -> (`Name n, None)) *)
(*     |> Map.of_alist_multi (module Pred) *)
(*     |> Map.to_alist *)
(*   in *)

(*   let fail msg = *)
(*     Logs.debug (fun m -> *)
(*         m "Partition: %s %a" msg (Fmt.Dump.list Pred.pp) *)
(*           (List.map ~f:Tuple.T2.get1 fields)) *)
(*   in *)
(*   let%bind f, aliases, l, h = *)
(*     match (fields, key_range) with *)
(*     | [ (f, aliases) ], (Some l, Some h) -> return (f, aliases, l, h) *)
(*     | _, (None, _ | _, None) -> *)
(*         fail "Could not find bounds for fields"; *)
(*         None *)
(*     | _ -> *)
(*         fail "Found too many fields"; *)
(*         None *)
(*   in *)
(*   (\* Don't try to partition if there's a subquery that refers to the partition attribute. *\) *)
(*   if *)
(*     Pred.names f |> Set.to_sequence *)
(*     |> Sequence.exists ~f:(exists_correlated_subquery r) *)
(*   then None *)
(*   else partition_with_bounds f aliases l h r n *)

(* let partition _ r = Set.to_sequence params |> Seq.filter_map ~f:(partition_on r) *)
(* let partition = Branching.global partition ~name:"partition" *)
(* let db_relation n = A.relation (Db.relation C.conn n) *)

(* let partition_eq n = *)
(*   let eq_preds r = *)
(*     let visitor = *)
(*       object *)
(*         inherit [_] V.reduce *)
(*         inherit [_] Util.list_monoid *)

(*         method! visit_Binop ps op arg1 arg2 = *)
(*           if [%compare.equal: Pred.Binop.t] op Eq then (arg1, arg2) :: ps *)
(*           else ps *)
(*       end *)
(*     in *)
(*     visitor#visit_t [] r *)
(*   in *)

(*   let replace_pred r p1 p2 = *)
(*     let visitor = *)
(*       object *)
(*         inherit [_] V.endo as super *)

(*         method! visit_pred () p = *)
(*           let p = super#visit_pred () p in *)
(*           if [%compare.equal: Pred.t] p p1 then p2 else p *)
(*       end *)
(*     in *)
(*     visitor#visit_t () r *)
(*   in *)

(*   let open A in *)
(*   let name = Abslayout.name_of_string_exn n in *)
(*   let fresh_name = *)
(*     Caml.Format.sprintf "%s_%s" (Name.to_var name) *)
(*       (Fresh.name Global.fresh "%d") *)
(*   in *)
(*   let rel = Name.rel_exn name in *)
(*   Branching.local ~name:"partition-eq" (fun r -> *)
(*       let eqs = eq_preds r in *)
(*       (\* Any predicate that is compared for equality with the partition *)
(*           field is a candidate for the hash table key. *\) *)
(*       let keys = *)
(*         List.filter_map eqs ~f:(fun (p1, p2) -> *)
(*             match (p1, p2) with *)
(*             | `Name n, _ when String.(Name.name n = Name.name name) -> Some p2 *)
(*             | _, `Name n when String.(Name.name n = Name.name name) -> Some p1 *)
(*             | _ -> None) *)
(*       in *)
(*       let lhs = *)
(*         dedup *)
(*           (select *)
(*              [ (`Name (Name.copy ~scope:None name), fresh_name) ] *)
(*              (db_relation rel)) *)
(*       in *)
(*       let scope = Fresh.name Global.fresh "s%d" in *)
(*       let pred = *)
(*         `Binop *)
(*           ( Eq, *)
(*             `Name (Name.copy ~scope:None name), *)
(*             `Name (Name.create fresh_name) ) *)
(*         |> Pred.scoped (Schema.schema lhs) scope *)
(*       in *)
(*       let filtered_rel = filter pred (db_relation rel) in *)
(*       List.map keys ~f:(fun k -> *)
(*           (\* The predicate that we chose as the key can be replaced by *)
(*              `fresh_name`. *\) *)
(*           A.select Schema.(schema r |> to_select_list) *)
(*           @@ hash_idx lhs scope *)
(*                (replace_pred *)
(*                   (Tactics_util.replace_rel rel filtered_rel r) *)
(*                   k *)
(*                   (`Name (Name.create ~scope fresh_name))) *)
(*                [ k ]) *)
(*       |> Seq.of_list) *)

(* let partition_domain n_subst n_domain = *)
(*   let open Abslayout in *)
(*   let n_subst, n_domain = *)
(*     (name_of_string_exn n_subst, name_of_string_exn n_domain) *)
(*   in *)
(*   let rel = Name.rel_exn n_domain in *)
(*   let lhs = *)
(*     dedup *)
(*       (select *)
(*          [ (`Name (Name.unscoped n_domain), Name.name n_domain) ] *)
(*          (db_relation rel)) *)
(*   in *)
(*   let scope = Fresh.name Global.fresh "s%d" in *)
(*   of_func ~name:"partition-domain" @@ fun r -> *)
(*   let inner_select = *)
(*     let lhs_schema = Schema.schema lhs in *)
(*     Schema.schema r *)
(*     |> List.filter ~f:(fun n -> *)
(*            not (List.mem lhs_schema ~equal:[%compare.equal: Name.t] n)) *)
(*     |> Schema.to_select_list *)
(*   in *)
(*   Option.return *)
(*   @@ A.select Schema.(schema r |> to_select_list) *)
(*   @@ hash_idx lhs scope *)
(*        (select inner_select *)
(*           (subst *)
(*              (Map.singleton *)
(*                 (module Name) *)
(*                 n_subst *)
(*                 (`Name (Name.scoped scope n_domain))) *)
(*              r)) *)
(*        [ `Name n_subst ] *)

(* (\** Hoist subqueries out of the filter predicate and make them available by a *)
(*      depjoin. Allows expensive subqueries to be computed once instead of many *)
(*      times. *\) *)
(* let elim_subquery p r = *)
(*   let open Option.Let_syntax in *)
(*   let stage = Is_serializable.stage ~params r in *)
(*   let can_hoist r = *)
(*     Free.free r *)
(*     |> Set.for_all ~f:(fun n -> *)
(*            Set.mem params n *)
(*            || match stage n with `Compile -> true | _ -> false) *)
(*   in *)
(*   let scope = fresh_name "s%d" in *)
(*   let visitor = *)
(*     object *)
(*       inherit [_] Tactics_util.extract_subquery_visitor *)
(*       method can_hoist = can_hoist *)
(*       method fresh_name () = Name.create ~scope @@ Fresh.name Global.fresh "q%d" *)
(*     end *)
(*   in *)
(*   let rhs, subqueries = visitor#visit_t () @@ Path.get_exn p r in *)
(*   let subqueries = *)
(*     List.map subqueries ~f:(fun (n, p) -> *)
(*         A.select [ (p, Name.name n) ] @@ A.scalar (`Int 0) "dummy") *)
(*   in *)
(*   let%map lhs = *)
(*     match subqueries with *)
(*     | [] -> None *)
(*     | [ x ] -> Some x *)
(*     | _ -> Some (A.tuple subqueries Cross) *)
(*   in *)
(*   Path.set_exn p r (A.dep_join lhs scope rhs) *)

(* let elim_subquery = global elim_subquery "elim-subquery" *)

(* (\** Hoist subqueries out of the filter predicate and make them available by a *)
(*      join. *\) *)
(* let elim_subquery_join p r = *)
(*   let open Option.Let_syntax in *)
(*   let stage = Is_serializable.stage ~params r in *)
(*   let can_hoist r = *)
(*     Free.free r *)
(*     |> Set.for_all ~f:(fun n -> *)
(*            Set.mem params n *)
(*            || match stage n with `Compile -> true | _ -> false) *)
(*   in *)
(*   let visitor = *)
(*     object *)
(*       inherit [_] Tactics_util.extract_subquery_visitor *)
(*       method can_hoist = can_hoist *)
(*       method fresh_name () = Name.create @@ Fresh.name Global.fresh "q%d" *)
(*     end *)
(*   in *)

(*   let%bind pred, r' = to_filter @@ Path.get_exn p r in *)
(*   let pred', subqueries = visitor#visit_pred () pred in *)
(*   let subqueries = *)
(*     List.filter_map subqueries ~f:(function *)
(*       | n, `First r -> ( *)
(*           match Schema.schema r with *)
(*           | [ n' ] -> return @@ A.select [ (`Name n', Name.name n) ] r *)
(*           | _ -> None) *)
(*       | _ -> None) *)
(*   in *)
(*   let%map rhs = *)
(*     match subqueries with *)
(*     | [] -> None *)
(*     | [ x ] -> Some x *)
(*     | _ -> Some (A.tuple subqueries Cross) *)
(*   in *)
(*   Path.set_exn p r (A.join pred' r' rhs) *)

(* let elim_subquery_join = global elim_subquery_join "elim-subquery-join" *)

(* let elim_correlated_first_subquery r = *)
(*   let open Option.Let_syntax in *)
(*   let visitor = *)
(*     object *)
(*       inherit [_] Tactics_util.extract_subquery_visitor *)
(*       val mutable is_first = true *)

(*       method can_hoist _ = *)
(*         if is_first then ( *)
(*           is_first <- false; *)
(*           true) *)
(*         else false *)

(*       method fresh_name () = Name.create @@ Fresh.name Global.fresh "q%d" *)
(*     end *)
(*   in *)
(*   let%bind p, r' = to_filter r in *)
(*   let p', subqueries = visitor#visit_pred () p in *)
(*   let%bind subquery_name, subquery = *)
(*     match subqueries with *)
(*     | [ s ] -> return s *)
(*     | [] -> None *)
(*     | _ -> failwith "expected one subquery" *)
(*   in *)
(*   let scope = Fresh.name Global.fresh "s%d" in *)
(*   let schema = Schema.schema r' in *)
(*   let ctx = *)
(*     List.map schema ~f:(fun n -> (n, `Name (Name.scoped scope n))) *)
(*     |> Map.of_alist_exn (module Name) *)
(*   in *)
(*   let schema = Schema.scoped scope schema in *)
(*   match subquery with *)
(*   | `Exists _ -> None *)
(*   | `First r -> *)
(*       return @@ A.dep_join r' scope *)
(*       @@ A.select (Schema.to_select_list schema) *)
(*       @@ Abslayout.subst ctx @@ A.filter p' *)
(*       @@ A.select *)
(*            [ *)
(*              (P.name @@ List.hd_exn @@ Schema.schema r, Name.name subquery_name); *)
(*            ] *)
(*            r *)
(*   | _ -> failwith "not a subquery" *)

(* let elim_correlated_first_subquery = *)
(*   of_func elim_correlated_first_subquery ~name:"elim-correlated-first-subquery" *)

(* let elim_correlated_exists_subquery r = *)
(*   let open Option.Let_syntax in *)
(*   let%bind p, r' = to_filter r in *)
(*   let p, subqueries = *)
(*     Pred.conjuncts p *)
(*     |> List.partition_map ~f:(function `Exists r -> Second r | p -> First p) *)
(*   in *)
(*   let%bind p, subquery = *)
(*     match subqueries with *)
(*     | [] -> None *)
(*     | s :: ss -> Some (Pred.conjoin (p @ List.map ss ~f:P.exists), s) *)
(*   in *)
(*   let scope = Fresh.name Global.fresh "s%d" in *)
(*   let schema = Schema.schema r' in *)
(*   let ctx = *)
(*     List.map schema ~f:(fun n -> (n, `Name (Name.scoped scope n))) *)
(*     |> Map.of_alist_exn (module Name) *)
(*   in *)
(*   let schema = Schema.scoped scope schema in *)
(*   let unscoped_schema = Schema.unscoped schema in *)
(*   return @@ A.dep_join r' scope @@ A.dedup @@ A.filter p *)
(*   @@ A.group_by (Schema.to_select_list unscoped_schema) unscoped_schema *)
(*   @@ A.select (Schema.to_select_list schema) *)
(*   @@ Abslayout.subst ctx subquery *)

(* let elim_correlated_exists_subquery = *)
(*   of_func elim_correlated_exists_subquery *)
(*     ~name:"elim-correlated-exists-subquery" *)

(* let elim_all_correlated_subqueries = *)
(*   for_all *)
(*     (first_success *)
(*        [ elim_correlated_first_subquery; elim_correlated_exists_subquery ]) *)
(*     Path.(all >>? is_filter >>? is_run_time) *)

(* let unnest = *)
(*   global (fun _ r -> Some (Unnest.unnest ~params r |> Ast.strip_meta)) "unnest" *)

(* let simplify_filter r = *)
(*   let open Option.Let_syntax in *)
(*   match r.node with *)
(*   | Filter (p, r) -> return @@ A.filter (Pred.simplify p) r *)
(*   | Join { pred; r1; r2 } -> return @@ A.join (Pred.simplify pred) r1 r2 *)
(*   | _ -> None *)

(* let simplify_filter = of_func ~name:"simplify-filter" simplify_filter *)

(* let precompute_filter_bv args = *)
(*   let open A in *)
(*   let values = List.map args ~f:Pred.of_string_exn in *)
(*   let exception Failed of Error.t in *)
(*   let run_exn r = *)
(*     match r.node with *)
(*     | Filter (p, r') -> *)
(*         let schema = Schema.schema r' in *)
(*         let free_vars = *)
(*           Set.diff (Free.pred_free p) (Set.of_list (module Name) schema) *)
(*           |> Set.to_list *)
(*         in *)
(*         let free_var = *)
(*           match free_vars with *)
(*           | [ v ] -> v *)
(*           | _ -> *)
(*               let err = *)
(*                 Error.of_string *)
(*                   "Unexpected number of free variables in predicate." *)
(*               in *)
(*               raise (Failed err) *)
(*         in *)
(*         let witness_name = Fresh.name Global.fresh "wit%d_" in *)
(*         let witnesses = *)
(*           List.mapi values ~f:(fun i v -> *)
(*               ( Pred.subst (Map.singleton (module Name) free_var v) p, *)
(*                 sprintf "%s_%d" witness_name i )) *)
(*         in *)
(*         let filter_pred = *)
(*           List.foldi values ~init:p ~f:(fun i else_ v -> *)
(*               `If *)
(*                 ( `Binop (Eq, `Name free_var, v), *)
(*                   `Name (Name.create (sprintf "%s_%d" witness_name i)), *)
(*                   else_ )) *)
(*         in *)
(*         let select_list = witnesses @ Select_list.of_names schema in *)
(*         Some (filter filter_pred (select select_list r')) *)
(*     | _ -> None *)
(*   in *)
(*   of_func ~name:"precompute-filter-bv" @@ fun r -> *)
(*   try run_exn r with Failed _ -> None *)

(* (\** Given a restricted parameter range, precompute a filter that depends on a *)
(*      single table field. `If the parameter is outside the range, then run the *)
(*      original filter. Otherwise, check the precomputed evidence. *\) *)
(* let precompute_filter field values = *)
(*   let open A in *)
(*   let field, values = *)
(*     (Abslayout.name_of_string_exn field, List.map values ~f:Pred.of_string_exn) *)
(*   in *)
(*   let exception Failed of Error.t in *)
(*   let run_exn r = *)
(*     match r.node with *)
(*     | Filter (p, r') -> *)
(*         let schema = Schema.schema r' in *)
(*         let free_vars = *)
(*           Set.diff (Free.pred_free p) (Set.of_list (module Name) schema) *)
(*           |> Set.to_list *)
(*         in *)
(*         let free_var = *)
(*           match free_vars with *)
(*           | [ v ] -> v *)
(*           | _ -> *)
(*               let err = *)
(*                 Error.of_string *)
(*                   "Unexpected number of free variables in predicate." *)
(*               in *)
(*               raise (Failed err) *)
(*         in *)
(*         let encoder = *)
(*           List.foldi values ~init:(`Int 0) ~f:(fun i else_ v -> *)
(*               let witness = *)
(*                 Pred.subst (Map.singleton (module Name) free_var v) p *)
(*               in *)
(*               `If (witness, `Int (i + 1), else_)) *)
(*         in *)
(*         let decoder = *)
(*           List.foldi values ~init:(`Int 0) ~f:(fun i else_ v -> *)
(*               `If (`Binop (Binop.Eq, `Name free_var, v), `Int (i + 1), else_)) *)
(*         in *)
(*         let fresh_name = Fresh.name Global.fresh "p%d_" ^ Name.name field in *)
(*         let select_list = *)
(*           (encoder, fresh_name) :: Select_list.of_names schema *)
(*         in *)
(*         Option.return *)
(*         @@ filter *)
(*              (`If *)
(*                ( `Binop (Eq, decoder, `Int 0), *)
(*                  p, *)
(*                  `Binop (Eq, decoder, `Name (Name.create fresh_name)) )) *)
(*         @@ select select_list r' *)
(*     | _ -> None *)
(*   in *)
(*   let f r = try run_exn r with Failed _ -> None in *)
(*   of_func ~name:"precompute-filter" f *)

(* let cse_filter r = *)
(*   let open Option.Let_syntax in *)
(*   let%bind p, r = to_filter r in *)
(*   let p', binds = Pred.cse p in *)
(*   if List.is_empty binds then None *)
(*   else *)
(*     return @@ A.filter p' *)
(*     @@ A.select *)
(*          (List.map binds ~f:(fun (n, p) -> (p, Name.name n)) *)
(*          @ Schema.(schema r |> to_select_list)) *)
(*     @@ r *)

(* let cse_filter = of_func ~name:"cse-filter" cse_filter *)

let () =
  Ops.register hoist_filter_orderby "hoist-filter-orderby";
  Ops.register hoist_filter_groupby "hoist-filter-groupby";
  Ops.register hoist_filter_filter "hoist-filter-filter";
  Ops.register hoist_filter_select "hoist-filter-select";
  Ops.register hoist_filter_join_both "hoist-filter-join-both";
  Ops.register hoist_filter_join_left "hoist-filter-join-left";
  Ops.register hoist_filter_join_right "hoist-filter-join-right";
  Ops.register hoist_filter_dedup "hoist-filter-dedup";
  Ops.register hoist_filter_list "hoist-filter-list";
  Ops.register hoist_filter_hashidx "hoist-filter-hashidx";
  Ops.register hoist_filter_orderedidx "hoist-filter-orderedidx";
  Ops.register hoist_filter "hoist-filter";
  Ops.register hoist_filter_agg "hoist-filter-agg";
  Ops.register split_filter "split-filter"
