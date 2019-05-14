open! Core

module Config : sig
  module type S = sig
    val layout_map_channel : Out_channel.t option
  end
end

module type S = Serialize_intf.S

module Make (Config : Config.S) (M : Abslayout_db.S) : S
