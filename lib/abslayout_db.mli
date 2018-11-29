module type S = Abslayout_db_intf.S

module Config : sig
  module type S = sig
    val conn : Db.t
  end
end

module Make (Config : Config.S) : S
