open! Core

module Config : sig
  module type S = sig
    val layout_file : string option

    val conn : Db.t
  end
end

module type S = Serialize_intf.S

module Make (Config : Config.S) : S
