open Core

type t

include Sexpable.S with type t := t

val create : unit -> t
val reset : t -> unit
val name : t -> (int -> string, unit, string) format -> string
val int : t -> int
