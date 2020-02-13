open! Core

module T = struct
  type t = { r_name : string; r_schema : Name.t list option [@opaque] }
  [@@deriving compare, hash, sexp]
end

include T
include Comparator.Make (T)

let schema { r_schema; _ } = Option.value_exn r_schema
