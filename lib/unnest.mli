open Core
open Ast

val unnest :
  < .. > annot ->
  < cardinality_matters : bool ; why_card_matters : string > annot

module Private : sig
  type ('p, 'r) visible_depjoin_query =
    | Query of ('p, 'r) query
    | Visible_depjoin of 'r depjoin
  [@@deriving sexp]

  type 'm visible_depjoin_annot = {
    node :
      ( 'm visible_depjoin_annot pred,
        'm visible_depjoin_annot )
      visible_depjoin_query;
    meta : 'm;
  }
  [@@deriving sexp]

  val attrs : _ visible_depjoin_annot -> Set.M(Name).t
  val free : _ visible_depjoin_annot -> Set.M(Name).t

  val to_nice_depjoin :
    < was_depjoin : bool > visible_depjoin_annot ->
    < was_depjoin : bool > visible_depjoin_annot ->
    (_ pred, < was_depjoin : bool > visible_depjoin_annot) visible_depjoin_query

  val to_visible_depjoin : < > annot -> < > visible_depjoin_annot
  val of_visible_depjoin : 'a visible_depjoin_annot -> 'a annot

  val map_meta :
    ('a -> 'b) -> 'a visible_depjoin_annot -> 'b visible_depjoin_annot

  val pp : _ visible_depjoin_annot Fmt.t
end
