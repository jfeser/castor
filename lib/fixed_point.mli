open Core

type t = { value : int; scale : int } [@@deriving hash, sexp]

include Comparable.S with type t := t

val convert : t -> int -> t
(** Convert to a new scale. *)

val pow10 : int -> int
val of_int : int -> t
val of_string : String.t -> t
val to_string : t -> string
val pp : Format.formatter -> t -> unit
val ( + ) : t -> t -> t
val ( ~- ) : t -> t
val ( - ) : t -> t -> t
val ( * ) : t -> t -> t
val ( / ) : t -> t -> t
val min_value : t
val max_value : t
val epsilon : t
val of_float : Float.t -> t
val to_float : t -> Float.t
