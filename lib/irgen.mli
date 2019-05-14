open! Core
open Implang

module Config : sig
  module type S = sig
    val debug : bool

    val code_only : bool
  end
end

type ir_module =
  {iters: func list; funcs: func list; params: Name.t list; buffer_len: int}
[@@deriving compare, sexp]

exception IRGenError of Error.t

module type S = sig
  val irgen : params:Name.t list -> data_fn:string -> Abslayout.t -> ir_module

  val pp : Formatter.t -> ir_module -> unit
end

module Make
    (Config : Config.S)
    (Abslayout_db : Abslayout_db.S)
    (Serialize : Serialize.S)
    () : S
