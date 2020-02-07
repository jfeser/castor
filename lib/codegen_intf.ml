open! Core

module type S = sig
  val codegen : Irgen.ir_module -> Llvm.llmodule

  val write_header : Out_channel.t -> unit

  val compile :
    ?out_dir:string ->
    ?layout_log:string ->
    gprof:bool ->
    params:Name.t list ->
    Db.t ->
    Abslayout.t ->
    string * string
end
