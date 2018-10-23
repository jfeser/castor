open Base

type t = Abslayout0.meta [@@deriving sexp_of]

type 'a key

type pos = Pos of int64 | Many_pos

type lexpos = Lexing.position =
  {pos_fname: string; pos_lnum: int; pos_bol: int; pos_cnum: int}
[@@deriving sexp]

val empty : unit -> t

val schema : Name.t list key

val pos : pos key

val align : int key

val use_foreach : bool key

val start_pos : lexpos key

val end_pos : lexpos key

val needed : Set.M(Name.Compare_no_type).t key

val free : Set.M(Name.Compare_no_type).t key

val eq : (Name.t * Name.t) list key

val order : Abslayout0.pred list key

val type_ : Type.t key

val find : Abslayout0.t -> 'a key -> 'a option

val find_exn : Abslayout0.t -> 'a key -> 'a

val set : Abslayout0.t -> 'a key -> 'a -> Abslayout0.t

val set_m : Abslayout0.t -> 'a key -> 'a -> unit

val update : Abslayout0.t -> 'a key -> f:('a option -> 'a) -> unit

module Direct : sig
  val find : t -> 'a key -> 'a option

  val find_exn : t -> 'a key -> 'a

  val set : t -> 'a key -> 'a -> t

  val set_m : t -> 'a key -> 'a -> unit
end
