open! Core
open Castor
open Abslayout
open Collections

module Config = struct
  module type S = sig
    val conn : Db.t
  end
end

module Make (Config : Config.S) = struct
  open Config

  (** Precise selection of all valuations of a list of predicates from a relation.
   *)
  let all_values_precise ps r =
    if Set.is_empty (free r) then Ok (dedup (select ps r))
    else Or_error.errorf "Predicate contains free variables."

  let rec closure m =
    let m' = Map.map m ~f:(Pred.subst m) in
    if [%compare.equal: Pred.t Map.M(Name).t] m m' then m else closure m'

  let group_by m ~f l =
    List.fold_left ~init:(Map.empty m)
      ~f:(fun m e -> Map.add_multi m ~key:(f e) ~data:e)
      l

  (** Approximate selection of all valuations of a list of predicates from a
   relation. Works if the relation is parameterized, but only when the
   predicates do not depend on those parameters. *)
  let all_values_approx ps r =
    let open Or_error.Let_syntax in
    (* Otherwise, if all grouping keys are from named relations, select all
     possible grouping keys. *)
    let alias_map = aliases r |> closure in
    (* Find the definition of each key and collect all the names in that
       definition. If they all come from base relations, then we can enumerate
       the keys. *)
    let orig_names = List.map ps ~f:Pred.to_name in
    let preds = List.map ps ~f:(Pred.subst alias_map) in
    let%map rels =
      List.map preds ~f:(fun p ->
          List.map
            (Pred.names p |> Set.to_list)
            ~f:(fun n ->
              match Db.relation_has_field conn (Name.name n) with
              | Some r -> Ok (r, n)
              | None ->
                  Or_error.error "Name does not come from base relation." n
                    [%sexp_of: Name.t] )
          |> Or_error.all )
      |> Or_error.all
    in
    List.concat rels
    |> List.map ~f:(fun (r, n) -> (r.r_name, n))
    |> Map.of_alist_multi (module String)
    |> Map.to_alist
    |> List.map ~f:(fun (r, ns) ->
           dedup
             (select
                (List.map ns ~f:(fun n -> Name n))
                (relation (Db.relation conn r))) )
    |> List.fold_left1_exn ~f:(join (Bool true))
    |> select
         (List.map2_exn orig_names preds ~f:(fun n p ->
              match n with Some n -> As_pred (p, Name.name n) | None -> p ))

  let all_values ps r =
    Or_error.find_ok [all_values_precise ps r; all_values_approx ps r]

  (** Check that a predicate is fully supported by a relation (it does not
      depend on anything in the context.) *)
  let is_supported bound pred =
    Set.for_all (pred_free pred) ~f:(fun n ->
        Set.mem bound n || Poly.(Name.Meta.(find_exn n stage) = `Compile) )
end
