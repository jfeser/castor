module type S = sig
  val codegen : Implang.IRGen.ir_module -> Llvm.llmodule

  val write_header : Stdio.Out_channel.t -> unit

  val compile :
       ?out_dir:string
    -> gprof:bool
    -> params:Abslayout.Name.t list
    -> Abslayout.t
    -> string * string
end
