open Core

type t = { r_name : string; r_schema : (Name.t * Prim_type.t) list option }
[@@deriving compare, equal, hash, sexp]

include Comparator.S with type t := t

val create : ?schema:(Name.t * Prim_type.t) list -> string -> t
val schema_exn : t -> (Name.t * Prim_type.t) list
val types_exn : t -> Prim_type.t list
val names_exn : t -> Name.t list
val schema : t -> Name.t list
