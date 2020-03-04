(** Interval abstraction for integers. *)

type t = Bottom | Interval of int * int | Top [@@deriving compare, sexp]

include Container.Summable with type t := t

val pp : Format.formatter -> t -> unit

val zero : t

val byte_width : nullable:bool -> t -> int

val of_int : int -> t

val to_int : t -> int option

val top : t

val bot : t

val inf : t -> (int, Error.t) result

val sup : t -> (int, Error.t) result

val meet : t -> t -> t

val join : t -> t -> t

val ceil_pow2 : t -> t

val ( && ) : t -> t -> t

val ( || ) : t -> t -> t

val ( + ) : t -> t -> t

val ( - ) : t -> t -> t

val ( * ) : t -> t -> t

module O : sig
  val ( && ) : t -> t -> t

  val ( || ) : t -> t -> t

  val ( + ) : t -> t -> t

  val ( - ) : t -> t -> t

  val ( * ) : t -> t -> t
end
