open Ast

type t = Univ_map.t ref [@@deriving sexp_of]

type 'a key = 'a Univ_map.Key.t

type lexpos = Lexing.position = {
  pos_fname : string;
  pos_lnum : int;
  pos_bol : int;
  pos_cnum : int;
}
[@@deriving sexp]

val empty : unit -> t

val find : t annot -> 'a key -> 'a option

val find_exn : t annot -> 'a key -> 'a

val set : t annot -> 'a key -> 'a -> t annot

val set_m : t annot -> 'a key -> 'a -> unit

val update : t annot -> 'a key -> f:('a option -> 'a) -> unit

module Direct : sig
  val find : t -> 'a key -> 'a option

  val find_exn : t -> 'a key -> 'a

  val set : t -> 'a key -> 'a -> t

  val set_m : t -> 'a key -> 'a -> unit
end
