open! Core
open Ast
open Implang

module Config : sig
  module type S = sig
    val debug : bool

    val code_only : bool
  end
end

type ir_module = {
  iters : func list;
  funcs : func list;
  params : Name.t list;
  buffer_len : int;
}
[@@deriving compare, sexp]

exception IRGenError of Error.t

module type S = sig
  val irgen : params:Name.t list -> len:int -> Serialize.meta annot -> ir_module

  val pp : Formatter.t -> ir_module -> unit
end

module Make (Config : Config.S) () : S
