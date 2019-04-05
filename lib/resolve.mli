open Core

module Config : sig
  module type S = sig
    val conn : Db.t
  end
end

module Make (C : Config.S) : sig
  val refcnt : int Map.M(Name).t Univ_map.Key.t

  val resolve : ?params:Set.M(Name).t -> Abslayout.t -> Abslayout.t
end
