open! Core

type t = Ast.meta [@@deriving sexp_of]

type 'a key = 'a Univ_map.Key.t

type lexpos = Lexing.position = {
  pos_fname : string;
  pos_lnum : int;
  pos_bol : int;
  pos_cnum : int;
}
[@@deriving sexp]

val empty : unit -> t

(* val defs : (Name.t option * Ast.t Ast.pred) list key
 * 
 * val align : int key
 * 
 * val start_pos : lexpos key
 * 
 * val end_pos : lexpos key
 * 
 * val free : Set.M(Name).t key
 * 
 * val eq : (Name.t * Name.t) list key
 * 
 * val order : (Ast.t Ast.pred * Ast.order) list key
 * 
 * val type_ : Type.t key
 * 
 * val refcnt : int Map.M(Name).t key *)

val find : Ast.t -> 'a key -> 'a option

val find_exn : Ast.t -> 'a key -> 'a

val set : Ast.t -> 'a key -> 'a -> Ast.t

val set_m : Ast.t -> 'a key -> 'a -> unit

val update : Ast.t -> 'a key -> f:('a option -> 'a) -> unit

module Direct : sig
  val find : t -> 'a key -> 'a option

  val find_exn : t -> 'a key -> 'a

  val set : t -> 'a key -> 'a -> t

  val set_m : t -> 'a key -> 'a -> unit
end
