open Core
module Config = Ops.Config

module Make (_ : Config.S) : sig
  val elim_depjoin : Ops.t
  val flatten_select : Ops.t
  val unnest_and_simplify : Ops.t
  val filter_const : Ops.t
  val simplify : Ops.t
  val project : Ops.t
end

val param : unit Command.Param.t
val simplify : ?params:Set.M(Name).t -> Db.Schema.t -> Ast.t -> Ast.t
