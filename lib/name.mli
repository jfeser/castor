type t [@@deriving compare, hash, sexp]

include Comparator.S with type t := t

module O : Comparable.Infix with type t := t

val create : ?scope:string -> ?type_:Prim_type.t -> string -> t

val copy :
  ?scope:string option ->
  ?type_:Prim_type.t option ->
  ?name:string ->
  ?meta:Univ_map.t ->
  t ->
  t

val name : t -> string

val type_ : t -> Prim_type.t option

val type_exn : t -> Prim_type.t

val rel : t -> string option

val rel_exn : t -> string

val scoped : string -> t -> t

val unscoped : t -> t

val to_sql : t -> string

val to_var : t -> string

val pp : Formatter.t -> t -> unit

val fresh : (int -> string, unit, string) format -> t

val create_table : unit -> (t, 'a) Bounded_int_table.t
