module type S = sig
  include Abslayout_intf.S

  val partition : part:pred -> lookup:pred -> t -> t

  val materialize : ?ctx:Ctx.t -> t -> Layout.t

  val to_type : ?ctx:Ctx.t -> t -> Type.t

  val serialize : ?ctx:Ctx.t -> Bitstring.Writer.t -> Type.t -> t -> t * int
end
