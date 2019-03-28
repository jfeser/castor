open Base
open Castor
open Abslayout
open Collections

(** Precise selection of all valuations of a list of predicates from a relation.
   *)
let all_values_precise ps r =
  if Set.is_empty (free r) then Some (dedup (select ps r)) else None

(** Approximate selection of all valuations of a list of predicates from a
   relation. Works if the relation is parameterized, but only when the
   predicates do not depend on those parameters. *)
let all_values_approx ps r =
  let exception Failed of Error.t in
  let fail err = raise (Failed err) in
  (* Otherwise, if all grouping keys are from named relations,
             select all possible grouping keys. *)
  let rels = Hashtbl.create (module Abslayout) in
  let alias_map = aliases r in
  try
    (* Find the definition of each key and collect all the names in that
               definition. If they all come from base relations, then we can
               enumerate the keys. *)
    List.iter ps ~f:(fun p ->
        Set.iter (Pred.names p) ~f:(fun n ->
            let r_name =
              match Name.rel n with
              | Some r -> r
              | None ->
                  fail
                    (Error.create "Name does not come from base relation." n
                       [%sexp_of: Name.t])
            in
            (* Look up relation in alias table. *)
            let r =
              match Map.find alias_map r_name with
              | Some r -> r
              | None ->
                  fail (Error.create "Unknown relation." n [%sexp_of: Name.t])
            in
            Hashtbl.add_multi rels ~key:r ~data:(Name n) ) ) ;
    Hashtbl.to_alist rels
    |> List.map ~f:(fun (r, ns) -> dedup (select ns r))
    |> List.fold_left1_exn ~f:(join (Bool true))
    |> Option.some
  with Failed err ->
    Logs.info (fun m -> m "%a" Error.pp err) ;
    None

let all_values ps r =
  List.find_map [all_values_precise; all_values_approx] ~f:(fun to_vs ->
      to_vs ps r )
