open Core

type t = { r_name : string; r_schema : (string * Prim_type.t) list option }
[@@deriving compare, equal, hash, sexp]

include Comparator.S with type t := t

val create : ?schema:(string * Prim_type.t) list -> string -> t
val schema : t -> Name.t list
val types_exn : t -> Prim_type.t list
