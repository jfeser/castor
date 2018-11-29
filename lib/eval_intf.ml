open Base
open Collections
open Db
open Abslayout

module type S = sig
  val load_relation : string -> Relation.t

  val eval_relation : Relation.t -> (Name.t * Value.t) list Gen.t

  val eval_pred : Ctx.t -> pred -> Value.t

  val eval : Ctx.t -> t -> Ctx.t Gen.t
  (* val eval_foreach : Ctx.t -> t -> t -> (Ctx.t * Ctx.t Gen.t) Gen.t *)
end
