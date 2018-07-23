open Base
open Collections

include Abslayout_intf.S

module Config : sig
  module type S_db = sig
    val layout_map : bool

    val conn : Postgresql.connection
  end

  module type S = sig
    val layout_map : bool

    val eval : Ctx.t -> t -> Ctx.t Seq.t
  end
end

module Make (Config : Config.S) () : Abslayout_db_intf.S

module Make_db (Config_db : Config.S_db) () : sig
  include Abslayout_db_intf.S

  val annotate_schema : t -> t
end
