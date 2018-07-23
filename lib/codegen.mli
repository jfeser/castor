open Base

module Config : sig
  module type S = sig
    val ctx : Llvm.llcontext

    val module_ : Llvm.llmodule

    val builder : Llvm.llbuilder

    val debug : bool
  end
end

module Make (Config : Config.S) () : sig
  val codegen : Implang.IRGen.ir_module -> unit

  val write_header : Stdio.Out_channel.t -> unit
end
