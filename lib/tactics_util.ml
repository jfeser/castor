open Core
open Ast
open Abslayout
open Collections
open Schema
module V = Visitors
module A = Abslayout
module P = Pred.Infix

module Config = struct
  module type S = sig
    val conn : Db.t
    val cost_conn : Db.t
    val params : Set.M(Name).t
  end
end

module Make (Config : Config.S) = struct
  open Config

  (** Remove all references to names in params while ensuring that the resulting
     relation overapproximates the original. *)
  let over_approx params r =
    let visitor =
      object (self)
        inherit [_] V.map as super

        method! visit_Filter () (p, r) =
          if Set.is_empty (Set.inter (Pred.names p) params) then
            super#visit_Filter () (p, r)
          else (self#visit_t () r).node

        method! visit_Select () (ps, r) =
          match A.select_kind ps with
          | `Agg -> Select (ps, r)
          | `Scalar -> Select (ps, self#visit_t () r)

        method! visit_GroupBy () (ps, ks, r) = GroupBy (ps, ks, r)
      end
    in
    let r = visitor#visit_t () r in
    let remains = Set.inter (Free.free r) params in
    if Set.is_empty remains then Ok r
    else
      Or_error.error "Failed to remove all parameters." remains
        [%sexp_of: Set.M(Name).t]

  (** Precise selection of all valuations of a list of predicates from a relation.
   *)
  let all_values_precise ps r =
    if Set.is_empty (Free.free r) then Ok (A.dedup (A.select ps r))
    else Or_error.errorf "Predicate contains free variables."

  let all_values_approx_1 ps r =
    let open Or_error.Let_syntax in
    let%map r' = over_approx params r in
    A.dedup @@ A.select ps r'

  let rec closure m =
    let m' = Map.map m ~f:(Pred.subst m) in
    if [%compare.equal: Pred.t Map.M(Name).t] m m' then m else closure m'

  let group_by m ~f l =
    List.fold_left ~init:(Map.empty m)
      ~f:(fun m e -> Map.add_multi m ~key:(f e) ~data:e)
      l

  (** Collect a map from names to defining expressions from a relation. *)
  let rec aliases r =
    let plus =
      Map.merge ~f:(fun ~key:_ -> function
        | `Left r | `Right r -> Some r
        | `Both (r1, r2) ->
            if Pred.O.(r1 = r2) then Some r1
            else failwith "Multiple relations with same alias")
    and zero = Map.empty (module Name)
    and one k v = Map.singleton (module Name) k v in
    match r.node with
    | Select (ps, r) -> (
        match select_kind ps with
        | `Scalar ->
            List.fold_left ps ~init:(aliases r) ~f:(fun m (p, n) ->
                plus (one (Name.create n) p) m)
        | `Agg -> zero)
    | Filter (_, r) | Dedup r -> aliases r
    | _ -> zero

  let alias_map r = aliases r |> closure

  let all_values_attr n =
    let open Option.Let_syntax in
    let%bind rel = Db.relation_has_field cost_conn (Name.name n) in
    return @@ A.select [ (`Name n, Name.name n) ] @@ A.relation rel

  (** Approximate selection of all valuations of a list of predicates from a
   relation. Works if the relation is parameterized, but only when the
   predicates do not depend on those parameters. *)
  let all_values_approx_2 (preds : _ pred select_list) r =
    let open Or_error.Let_syntax in
    (* Otherwise, if all grouping keys are from named relations, select all
       possible grouping keys. *)
    let alias_map = aliases r in

    (* Find the definition of each key and collect all the names in that
       definition. `If they all come from base relations, then we can enumerate
       the keys. *)
    let preds = Select_list.map preds ~f:(fun p _ -> Pred.subst alias_map p) in

    (* Try to substitute names that don't come from base relations with equivalent names that do. *)
    let subst =
      Equiv.eqs r |> Set.to_list
      |> List.filter_map ~f:(fun (n, n') ->
             match
               ( Db.relation_has_field cost_conn (Name.name n),
                 Db.relation_has_field cost_conn (Name.name n') )
             with
             | None, None | Some _, Some _ -> None
             | Some _, None -> Some (n', n)
             | None, Some _ -> Some (n, n'))
      |> Map.of_alist_reduce (module Name) ~f:(fun n _ -> n)
      |> Map.map ~f:P.name
    in

    let preds = Select_list.map preds ~f:(fun p _ -> Pred.subst subst p) in

    (* Collect the relations referred to by the predicate list. *)
    let%bind rels =
      List.map preds ~f:(fun (p, _) ->
          List.map
            (Pred.names p |> Set.to_list)
            ~f:(fun n ->
              match Db.relation_has_field cost_conn (Name.name n) with
              | Some r -> Ok (r, n)
              | None ->
                  Or_error.error "`Name does not come from base relation." n
                    [%sexp_of: Name.t])
          |> Or_error.all)
      |> Or_error.all
    in

    let joined_rels =
      List.concat rels
      |> List.map ~f:(fun (r, n) -> (r.Relation.r_name, n))
      |> Map.of_alist_multi (module String)
      |> Map.to_alist
      |> List.map ~f:(fun (r, ns) ->
             dedup
             @@ select (Select_list.of_names ns)
             @@ relation (Db.relation cost_conn r))
      |> List.reduce ~f:(join (`Bool true))
    in

    match joined_rels with
    | Some r -> Ok (select preds r)
    | None -> Or_error.errorf "No relations found."

  let all_values_approx ps r =
    if List.length ps = 1 then all_values_approx_2 ps r
    else
      match all_values_approx_1 ps r with
      | Ok r' -> Ok r'
      | Error _ -> all_values_approx_2 ps r

  let all_values ps r =
    match all_values_precise ps r with
    | Ok r' -> Ok r'
    | Error _ -> all_values_approx ps r

  (** Check that a predicate is fully supported by a relation (it does not
      depend on anything in the context.) *)
  let is_supported stage bound pred =
    Set.for_all (Free.pred_free pred) ~f:(fun n ->
        Set.mem bound n
        (* TODO: We assume that compile time names that are bound in the context
           are ok, but this might not be true? *)
        || (match Map.find stage n with
           | Some `Compile -> true
           | Some `Run -> false
           | None ->
               Logs.warn (fun m -> m "Missing stage on %a" Name.pp n);
               false)
           && Option.is_some (Name.rel n))

  (** Remove names from a selection list. *)
  let select_out ns r =
    let ns = List.map ns ~f:Name.unscoped in
    select
      (schema r
      |> List.filter ~f:(fun n' ->
             not (List.mem ~equal:Name.O.( = ) ns (Name.unscoped n')))
      |> Select_list.of_names)
      r

  let select_contains names ps r =
    Set.(
      is_empty
        (diff
           (inter names (of_list (module Name) (schema r)))
           (of_list
              (module Name)
              (List.map ~f:(fun (_, n) -> Name.create n) ps))))

  let rec all_pairs = function
    | [] -> []
    | x :: xs -> List.map xs ~f:(fun x' -> (x, x')) @ all_pairs xs

  let rec disjoin =
    let open Ast in
    function
    | [] -> `Bool false
    | [ p ] -> p
    | p :: ps -> `Binop (Binop.Or, p, disjoin ps)

  (** For a set of predicates, check whether more than one predicate is true at
     any time. *)
  let all_disjoint ps r =
    let open Or_error.Let_syntax in
    if List.length ps <= 1 then return true
    else
      let%map tups =
        let sql =
          let pred =
            all_pairs ps |> List.map ~f:(fun (p, p') -> P.(p && p')) |> disjoin
          in
          filter pred @@ r
          |> Unnest.unnest ~params:(Set.empty (module Name))
          |> Sql.of_ralgebra |> Sql.to_string
        in
        Log.debug (fun m -> m "All disjoint sql: %s" sql);
        Db.run conn sql
      in
      List.length tups = 0

  let replace_rel rel new_rel r =
    let visitor =
      object
        inherit [_] Visitors.endo

        method! visit_Relation () r' { r_name = rel'; _ } =
          if String.(rel = rel') then new_rel.Ast.node else r'
      end
    in
    visitor#visit_t () r

  (** Visitor for extracting subqueries from a query, leaving behind names so
     that the subqueries can later be bound to the names.

      can_hoist should be overwritten to determine whether the subquery can be
     removed *)
  class virtual extract_subquery_visitor =
    object (self : 'self)
      inherit [_] Visitors.mapreduce
      inherit [_] Util.list_monoid
      method virtual can_hoist : Ast.t -> bool
      method virtual fresh_name : unit -> Name.t

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
        if self#can_hoist r then
          let name = self#fresh_name () in
          (`Name name, [ (name, `Exists r) ])
        else (`Exists r, [])

      method! visit_First () r =
        if self#can_hoist r then
          let name = self#fresh_name () in
          (`Name name, [ (name, `First r) ])
        else (`First r, [])
    end
end
