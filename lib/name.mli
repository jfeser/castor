open Core
open Collections

type t [@@deriving compare, hash, sexp]

include Comparator.S with type t := t

module O : Comparable.Infix with type t := t

val create : ?relation:string -> ?type_:Type.PrimType.t -> string -> t

val copy :
     ?relation:string sexp_option
  -> ?type_:Type.PrimType.t sexp_option
  -> ?name:string
  -> t
  -> t

val name : t -> string

val type_ : t -> Type.PrimType.t option

val rel : t -> string option

val type_exn : t -> Type.PrimType.t

val rel_exn : t -> string

val to_sql : t -> string

val to_var : t -> string

val pp : Formatter.t -> t -> unit

val fresh : Fresh.t -> (int -> string, unit, string) format -> t

val create_table : unit -> (t, 'a) Bounded_int_table.t

module Meta : sig
  val type_ : Type.PrimType.t Univ_map.Key.t

  val stage : [`Compile | `Run] Univ_map.Key.t

  val find : t -> 'a Univ_map.Key.t -> 'a option

  val set : t -> 'a Univ_map.Key.t -> 'a -> t
end
