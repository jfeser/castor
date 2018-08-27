open Abslayout

module Make (Eval : Eval.S) : sig
  val partition : part:pred -> lookup:pred -> t -> t

  val materialize : ?ctx:Ctx.t -> t -> Layout.t

  val to_type : ?ctx:Ctx.t -> t -> Type.t

  val annotate_schema : t -> t
end
