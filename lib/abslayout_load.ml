open! Core
open Abslayout

let annotate_relations conn =
  let visitor =
    object
      inherit [_] endo

      method! visit_Relation () _ r = Relation (Db.relation conn r.r_name)
    end
  in
  visitor#visit_t ()

let load_layout ?(params = Set.empty (module Name)) conn l =
  l |> strip_unused_as |> annotate_relations conn |> annotate_key_layouts
  |> Resolve.resolve ~params

let load_string ?params conn s = of_string_exn s |> load_layout conn ?params
