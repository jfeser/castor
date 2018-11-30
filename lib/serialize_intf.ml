open Stdio
open Abslayout

module type S = sig
  module Log : sig
    val render : string -> Out_channel.t -> unit
  end

  val serialize : Bitstring.Writer.t -> t -> t * int
end
