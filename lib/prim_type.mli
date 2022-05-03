type t =
  | NullT
  | IntT of { nullable : bool }
  | DateT of { nullable : bool }
  | FixedT of { nullable : bool }
  | StringT of { nullable : bool; padded : bool }
  | BoolT of { nullable : bool }
  | TupleT of t list
  | VoidT
[@@deriving compare, equal, hash, sexp]

val pp : Format.formatter -> t -> unit
val null_t : t
val int_t : t
val date_t : t
val fixed_t : t
val string_t : t
val bool_t : t
val to_sql : t -> string
val to_string : t -> string
val is_nullable : t -> bool
val unify : t -> t -> t
val width : t -> int
