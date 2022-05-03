open Core
open Ast

val unnest :
  params:Set.M(Name).t ->
  < .. > annot ->
  < cardinality_matters : bool ; why_card_matters : string > annot

module Private : sig
  val attrs : _ annot -> Set.M(String).t
  val free : _ annot -> Set.M(String).t
  val to_nice_depjoin : _ annot -> _ annot -> < was_depjoin : bool > annot
  val to_visible_depjoin : < > annot -> < > annot
end
