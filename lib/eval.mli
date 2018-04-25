open Base

open Collections
open Db
open Layout

exception EvalError of Error.t

module Config : sig
  module type S = sig
    val conn : Postgresql.connection
  end
end

module Make (Config : Config.S) : sig
  val eval_pred : PredCtx.t -> Field.t Ralgebra0.pred -> primvalue
  val eval_aggregate : Tuple.t -> Field.t Ralgebra0.agg list -> Tuple.t Seq.t -> Tuple.t
  val eval_layout : PredCtx.t -> t -> Tuple.t Seq.t
  val eval_relation : Relation.t -> Tuple.t Seq.t
  val eval_count : 'a Seq.t -> Tuple.t Seq.t
  val eval_project : Field.t list -> Tuple.t Seq.t -> Tuple.t Seq.t
  val eval_filter : PredCtx.t -> Field.t Ralgebra0.pred -> Tuple.t Seq.t -> Tuple.t Seq.t

  val eval_eqjoin : Field.t -> Field.t -> Tuple.t Seq.t -> Tuple.t Seq.t -> Tuple.t Seq.t
  val eval_concat : 'a Seq.t list -> 'a Seq.t
  val eval_agg : Field.t Ralgebra0.agg list -> Field.t List.t -> Tuple.t Seq.t -> Tuple.t Seq.t
  val eval : PredCtx.t -> Ralgebra.t -> Tuple.t Seq.t
  val eval_partial : Ralgebra.t -> Ralgebra.t
end
