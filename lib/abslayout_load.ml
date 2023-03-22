open Core
open Ast
open Abslayout
module V = Visitors

let annotate_relations db_schema =
  let visitor =
    object
      inherit [_] V.endo

      method! visit_Relation () _ r =
        Relation (Db.Schema.relation db_schema r.r_name)
    end
  in
  visitor#visit_t ()

let annotate conn l = l |> annotate_relations conn |> annotate_key_layouts

let load_layout_exn ?(params = Set.empty (module Name)) conn l =
  annotate conn l |> Resolve.resolve_exn ~params

let load_layout ?(params = Set.empty (module Name)) conn l =
  annotate conn l |> Resolve.resolve ~params

let load_string_exn ?params conn s =
  of_string_exn s |> load_layout_exn conn ?params |> strip_meta

let load_string_nostrip_exn ?params conn s =
  of_string_exn s |> load_layout_exn conn ?params

let load_stdin_nostrip_exn params conn =
  let params =
    List.map params ~f:(fun (n, t) -> Name.create ~type_:t n)
    |> Set.of_list (module Name)
  in
  of_channel_exn In_channel.stdin |> load_layout_exn conn ~params
