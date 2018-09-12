open Base
open Collections
open Db
open Abslayout

module type S = sig
  val load_relation : string -> Relation.t

  val eval_relation : Relation.t -> Tuple.t Seq.t

  val eval_pred : Ctx.t -> pred -> primvalue

  val eval : Ctx.t -> t -> Ctx.t Seq.t

  val eval_foreach : Ctx.t -> t -> t -> (Ctx.t * Ctx.t Seq.t) Seq.t
end
