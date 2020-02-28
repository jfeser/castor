open Ast

module type S = sig
  val codegen : Irgen.ir_module -> Llvm.llmodule

  val write_header : Out_channel.t -> unit

  val compile :
    ?out_dir:string ->
    ?layout_log:string ->
    gprof:bool ->
    params:(Name.t * Prim_type.t) list ->
    Db.t ->
    < type_ : Type.t ; .. > annot ->
    string * string
end
