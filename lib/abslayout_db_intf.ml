open Base
open Abslayout

module type S = sig
  val partition : part:pred -> lookup:pred list -> t -> t

  val to_type : ?ctx:Ctx.t -> t -> Type.t

  val annotate_schema : t -> t

  val to_schema : t -> Name.t list

  val annotate_key_layouts : t -> t

  val resolve : ?params:Set.M(Name.Compare_no_type).t -> t -> t
end
