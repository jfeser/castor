open Base
open Collections

type t = Abslayout0.name =
  {relation: string option; name: string; type_: Type.PrimType.t option}
[@@deriving compare, hash, sexp]

include Comparator.S with type t := t

module O : Comparable.Infix with type t := t

val create : ?relation:string -> ?type_:Type.PrimType.t -> string -> t

val type_exn : t -> Type.PrimType.t

val rel_exn : t -> string

val to_sql : t -> string

val to_var : t -> string

val pp : Formatter.t -> t -> unit

val of_string_exn : string -> t

val fresh : Fresh.t -> (int -> string, unit, string) format -> t
