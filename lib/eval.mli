open Base
open Db

module type S = Eval_intf.S

module Config : sig
  module type S = sig
    val conn : Db.t
  end

  module type S_mock = sig
    val rels : (string * Value.t) list list Hashtbl.M(Relation).t
  end
end

module Make (Config : Config.S) : S

module Make_mock (Config : Config.S_mock) : S
