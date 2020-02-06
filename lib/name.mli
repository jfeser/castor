open! Core

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

val rel : t -> string option

val meta : t -> Univ_map.t

val type_exn : t -> Prim_type.t

val rel_exn : t -> string

val scoped : string -> t -> t

val unscoped : t -> t

val to_sql : t -> string

val to_var : t -> string

val pp : Formatter.t -> t -> unit

val pp_with_stage : Formatter.t -> t -> unit

val pp_with_stage_and_refcnt : Formatter.t -> t -> unit

val pp_with_stage_and_type : Formatter.t -> t -> unit

val fresh : (int -> string, unit, string) format -> t

val create_table : unit -> (t, 'a) Bounded_int_table.t

module Meta : sig
  val type_ : Prim_type.t Univ_map.Key.t

  val stage : [ `Compile | `Run ] Univ_map.Key.t

  val refcnt : int Univ_map.Key.t

  val find : t -> 'a Univ_map.Key.t -> 'a option

  val find_exn : t -> 'a Univ_map.Key.t -> 'a

  val set : t -> 'a Univ_map.Key.t -> 'a -> t

  val change : t -> f:('a option -> 'a option) -> 'a Univ_map.Key.t -> t
end
