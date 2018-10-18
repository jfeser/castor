open Base
open Collections

type t = Abslayout0.name =
  {relation: string option; name: string; type_: Type.PrimType.t option}
[@@deriving sexp]

module Compare : sig
  type t = Abslayout0.name [@@deriving compare, hash, sexp]

  include Comparable.S with type t := t
end

module Compare_no_type : sig
  type t = Abslayout0.name [@@deriving compare, hash, sexp]

  include Comparable.S with type t := t
end

module Compare_name_only : sig
  type t = Abslayout0.name [@@deriving compare, hash, sexp]

  include Comparable.S with type t := t
end

val create : ?relation:string -> ?type_:Type.PrimType.t -> string -> t

val type_exn : t -> Type.PrimType.t

val to_sql : t -> string

val to_var : t -> string

val pp : Formatter.t -> t -> unit

val of_string_exn : string -> t

val of_field : ?rel:string -> Db.Field.t -> t

val fresh : Fresh.t -> (int -> string, unit, string) format -> t
