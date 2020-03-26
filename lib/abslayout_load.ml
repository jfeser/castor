open Ast
open Abslayout
module V = Visitors

let annotate_relations conn =
  let visitor =
    object
      inherit [_] V.endo

      method! visit_Relation () _ r = Relation (Db.relation conn r.r_name)
    end
  in
  visitor#visit_t ()

let annotate conn l =
  l |> strip_unused_as |> annotate_relations conn |> annotate_key_layouts

let load_layout ?(params = Set.empty (module Name)) conn l =
  Resolve.resolve ~params (annotate conn l)

let load_string ?params conn s =
  of_string_exn s |> load_layout conn ?params |> strip_meta
