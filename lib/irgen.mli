open Base
open Implang

module Config : sig
  module type S = sig
    val code_only : bool
  end
end

type ir_module =
  { iters: func list
  ; funcs: func list
  ; params: Abslayout.Name.t list
  ; buffer_len: int }
[@@deriving sexp]

exception IRGenError of Error.t

module type S = sig
  val irgen :
    params:Abslayout.Name.t list -> data_fn:string -> Abslayout.t -> ir_module

  val pp : Formatter.t -> ir_module -> unit
end

module Make (Config : Config.S) (Eval : Eval.S) (Serialize : Serialize.S) () : S
