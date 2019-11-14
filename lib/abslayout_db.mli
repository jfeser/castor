module type S = Abslayout_db_intf.S

module Config : sig
  module type S = sig
    val conn : Db.t

    val simplify : (Abslayout.t -> Abslayout.t) option
  end
end

module Make (C : Config.S) : S
