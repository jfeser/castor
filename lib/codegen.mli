open Core

module Ctx : sig
  type t
end

val codegen : Ctx.t -> Irgen.ir_module -> Llvm.llmodule
val write_header : Ctx.t -> Out_channel.t -> unit

val compile :
  ?out_dir:string ->
  ?layout_log:string ->
  ?debug:bool ->
  gprof:bool ->
  params:(string * Prim_type.t) list ->
  < fold_stream : Abslayout_fold.Data.t
  ; type_ : Type.t
  ; resolved : Resolve.resolved
  ; eq : Set.M(Equiv.Eq).t
  ; .. >
  Ast.annot ->
  string * string
