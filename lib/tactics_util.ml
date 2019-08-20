open! Core
open Castor
open Abslayout
open Collections

module Config = struct
  module type S = sig
    val conn : Db.t

    val cost_conn : Db.t
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

  let alias_map r = aliases r |> closure

  (** Approximate selection of all valuations of a list of predicates from a
   relation. Works if the relation is parameterized, but only when the
   predicates do not depend on those parameters. *)
  let all_values_approx ps r =
    let open Or_error.Let_syntax in
    (* Otherwise, if all grouping keys are from named relations, select all
     possible grouping keys. *)
    let alias_map = alias_map r in
    (* Find the definition of each key and collect all the names in that
       definition. If they all come from base relations, then we can enumerate
       the keys. *)
    let orig_names = List.map ps ~f:Pred.to_name in
    let preds = List.map ps ~f:(Pred.subst alias_map) in
    let%bind rels =
      List.map preds ~f:(fun p ->
          List.map
            (Pred.names p |> Set.to_list)
            ~f:(fun n ->
              match Db.relation_has_field cost_conn (Name.name n) with
              | Some r -> Ok (r, n)
              | None ->
                  Or_error.error "Name does not come from base relation." n
                    [%sexp_of: Name.t])
          |> Or_error.all)
      |> Or_error.all
    in
    let joined_rels =
      List.concat rels
      |> List.map ~f:(fun (r, n) -> (r.r_name, n))
      |> Map.of_alist_multi (module String)
      |> Map.to_alist
      |> List.map ~f:(fun (r, ns) ->
             dedup
               (select
                  (List.map ns ~f:(fun n -> Name n))
                  (relation (Db.relation cost_conn r))))
      |> List.reduce ~f:(join (Bool true))
    in
    match joined_rels with
    | Some r ->
        Ok
          (select
             (List.map2_exn orig_names preds ~f:(fun n p ->
                  match n with
                  | Some n -> Pred.as_pred (p, Name.name n)
                  | None -> p))
             r)
    | None -> Or_error.errorf "No relations found."

  let all_values ps r =
    Or_error.find_ok [all_values_precise ps r; all_values_approx ps r]

  (** Check that a predicate is fully supported by a relation (it does not
      depend on anything in the context.) *)
  let is_supported bound pred =
    Set.for_all (pred_free pred) ~f:(fun n ->
        Set.mem bound n
        (* TODO: We assume that compile time names that are bound in the context
           are ok, but this might not be true? *)
        || ( match Name.Meta.(find n stage) with
           | Some s -> s = `Compile
           | None ->
               Logs.warn (fun m -> m "Missing stage on %a" Name.pp n) ;
               false )
           && Option.is_some (Name.rel n))

  (** Remove names from a selection list. *)
  let select_out ns r =
    let ns = List.map ns ~f:Name.unscoped in
    select
      ( schema_exn r
      |> List.filter ~f:(fun n' ->
             not (List.mem ~equal:Name.O.( = ) ns (Name.unscoped n')))
      |> List.map ~f:(fun n -> Name n) )
      r

  let select_contains names ps r =
    Set.(
      is_empty
        (diff
           (inter names (of_list (module Name) (schema_exn r)))
           (of_list (module Name) (List.filter_map ~f:Pred.to_name ps))))

  let all_disjoint ps r =
    let tup =
      select [Max (Pred.sum_exn (List.map ps ~f:Pred.pseudo_bool))] r
      |> Sql.of_ralgebra |> Sql.to_string
      |> Db.exec_cursor_exn conn [Type.PrimType.int_t]
      |> Gen.get
    in
    match tup with
    | Some [|Int x|] -> x <= 1
    | _ -> failwith "Unexpected tuple."
end
