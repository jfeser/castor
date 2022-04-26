open Ast

module Config : sig
  module type S = sig
    val conn : Db.t
    val cost_conn : Db.t
    val params : Set.M(Name).t
    val cost_timeout : float option
    val random : Mcmc.Random_choice.t
  end
end

module Make (Config : Config.S) : sig
  val is_serializable : 'a annot -> unit Or_error.t
end

val optimize : (module Config.S) -> Ast.t -> (Ast.t, Ast.t) Either.t
