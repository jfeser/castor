open Base

module Config : sig
  module type S = sig
    val debug : bool
  end
end

module Make (Config : Config.S) () : sig
  val codegen : Implang.IRGen.ir_module -> Llvm.llmodule

  val write_header : Stdio.Out_channel.t -> unit

  val compile :
       ?out_dir:string
    -> gprof:bool
    -> params:(string * Type.PrimType.t) list
    -> Implang.IRGen.ir_module
    -> unit
end
