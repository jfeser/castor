open Base
open Abslayout

module Make (Eval : Eval.S) : sig
  val partition : part:pred -> lookup:pred list -> t -> t

  val to_type : ?ctx:Ctx.t -> t -> Type.t

  val annotate_schema : t -> t

  val annotate_key_layouts : t -> t

  val resolve : ?params:Set.M(Name.Compare_no_type).t -> t -> t
end
