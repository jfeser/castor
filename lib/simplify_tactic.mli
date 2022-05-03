open Core
module Config = Ops.Config

module Make (C : Config.S) : sig
  val elim_depjoin : Ops.t
  val flatten_select : Ops.t
  val unnest_and_simplify : Ops.t
end

val simplify : ?params:Set.M(Name).t -> Db.t -> Ast.t -> Ast.t
