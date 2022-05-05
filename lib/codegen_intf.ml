open Core
open Ast

module type S = sig
  val codegen : Irgen.ir_module -> Llvm.llmodule
  val write_header : Out_channel.t -> unit

  val compile :
    ?out_dir:string ->
    ?layout_log:string ->
    gprof:bool ->
    params:(string * Prim_type.t) list ->
    < fold_stream : Abslayout_fold.Data.t
    ; type_ : Type.t
    ; resolved : Resolve.resolved
    ; eq : Set.M(Equiv.Eq).t
    ; .. >
    annot ->
    string * string
end
