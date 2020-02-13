type t

val create : unit -> t

val reset : t -> unit

val name : t -> (int -> string, unit, string) format -> string
