open Stdio
open Abslayout

module type S = sig
  module Log : sig
    val render : string -> Out_channel.t -> unit
  end

  val serialize : ?ctx:Ctx.t -> Bitstring.Writer.t -> Type.t -> t -> t * int
end
