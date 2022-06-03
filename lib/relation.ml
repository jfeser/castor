open Core

module T = struct
  type t = {
    r_name : string;
    r_schema : (string * Prim_type.t) list option; [@opaque]
  }
  [@@deriving compare, equal, hash, sexp]
end

include T
include Comparator.Make (T)

let create ?schema name = { r_name = name; r_schema = schema }

let schema_exn { r_schema; r_name } =
  Option.value_exn
    ~error:Error.(create "Missing schema annotation." r_name [%sexp_of: string])
    r_schema

let types_exn r = schema_exn r |> List.map ~f:(fun (_, t) -> t)
(* let names_exn r = schema_exn r |> List.map ~f:(fun (n, _) -> n) *)

let schema r =
  List.map (schema_exn r) ~f:(fun (n, t) ->
      Name.{ type_ = Some t; name = Attr (r.r_name, n) })
