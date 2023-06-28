open Core

type name = Simple of string | Bound of int * string | Attr of string * string
[@@deriving compare, equal, hash, sexp]

type t = { name : name; type_ : Prim_type.t option }
[@@deriving compare, equal, hash, sexp]

include Comparator.S with type t := t
module O : Comparable.Infix with type t := t

val create : ?type_:Prim_type.t -> string -> t
val name : t -> string
val type_ : t -> Prim_type.t option
val type_exn : t -> Prim_type.t
val shift : cutoff:int -> int -> t -> t
val incr : t -> t
val decr : t -> t option
val decr_exn : t -> t
val zero : t -> t
val set_index : t -> int -> t
val pp : Formatter.t -> t -> unit
val fresh : (int -> string, unit, string) format -> t
val of_string_exn : string -> t
val scope : t -> int option
val unscoped : t -> t
val is_bound : t -> bool
