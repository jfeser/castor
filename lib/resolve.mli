open! Core

module Config : sig
  module type S = sig end
end

module Make (C : Config.S) : sig
  val resolve : ?params:Set.M(Name).t -> Abslayout.t -> Abslayout.t
end
