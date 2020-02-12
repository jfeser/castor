open! Core
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
