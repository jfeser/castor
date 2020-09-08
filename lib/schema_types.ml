open Ast
open Schema

let types (r : < resolved : Resolve.resolved ; .. > annot) =
  schema r |> List.map ~f:Name.type_exn

let types_full (r : < resolved : Resolve.resolved ; .. > annot) =
  schema_full r |> List.map ~f:Name.type_exn

let names_and_types (r : < resolved : Resolve.resolved ; .. > annot) =
  schema r |> List.map ~f:(fun n -> (Name.name n, Name.type_exn n))
