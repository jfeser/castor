open Ast

module Map : sig
  val bound : ('a -> 'b) -> 'a * 'c -> 'b * 'c

  val ordered_idx :
    ('r1 -> 'r2) ->
    ('p1 -> 'p2) ->
    ('p1, 'r1) ordered_idx ->
    ('p2, 'r2) ordered_idx

  val hash_idx :
    ('r1 -> 'r2) -> ('p1 -> 'p2) -> ('p1, 'r1) hash_idx -> ('p2, 'r2) hash_idx

  val dep_join : ('r1 -> 'r2) -> 'r1 depjoin -> 'r2 depjoin
  val list : ('r1 -> 'r2) -> 'r1 list_ -> 'r2 list_
  val scalar : ('r1 -> 'r2) -> 'r1 scalar -> 'r2 scalar
  val select_list : ('r1 -> 'r2) -> 'r1 select_list -> 'r2 select_list

  val pred :
    ('r1 -> 'r2) -> ('p1 -> 'p2) -> ('p1, 'r1) ppred -> ('p2, 'r2) ppred

  val query :
    ('r1 -> 'r2) -> ('p1 -> 'p2) -> ('p1, 'r1) query -> ('p2, 'r2) query

  val annot :
    (('m annot pred, 'm annot) query -> ('m annot pred, 'm annot) query) ->
    'm annot ->
    'm annot
end

module Map2 : sig
  exception Mismatch

  val annot :
    (('a annot pred, 'a annot) query ->
    ('b annot pred, 'b annot) query ->
    ('c annot pred, 'c annot) query) ->
    ('a -> 'b -> 'c) ->
    'a annot ->
    'b annot ->
    'c annot

  val pred :
    ('r1 -> 'r2 -> 'r3) ->
    ('p1 -> 'p2 -> 'p3) ->
    ('p1, 'r1) ppred ->
    ('p2, 'r2) ppred ->
    ('p3, 'r3) ppred

  val query :
    ('r1 -> 'r2 -> 'r3) ->
    ('p1 -> 'p2 -> 'p3) ->
    ('p1, 'r1) query ->
    ('p2, 'r2) query ->
    ('p3, 'r3) query
end

val map_meta : ('a -> 'b) -> 'a annot -> 'b annot

val map_meta_query :
  ('a -> 'b) ->
  ('a annot pred, 'a annot) query ->
  ('b annot pred, 'b annot) query

val map_meta_pred : ('a -> 'b) -> 'a annot pred -> 'b annot pred

module Reduce : sig
  type ('a, 'b) t = 'a -> ('a -> 'a -> 'a) -> 'b
  (** ['a t] is the type of reductions *)

  val select_list : ('a, ('p -> 'a) -> 'p select_list -> 'a) t
  val pred : ('a, ('r -> 'a) -> ('p -> 'a) -> ('p, 'r) ppred -> 'a) t
  val query : ('a, ('r -> 'a) -> ('p -> 'a) -> ('p, 'r) query -> 'a) t

  val annot :
    ( 'a,
      (('m annot pred, 'm annot) query -> 'a) -> ('m -> 'a) -> 'm annot -> 'a
    )
    t
end

module Iter : sig
  val pred : ('r -> unit) -> ('p -> unit) -> ('p, 'r) ppred -> unit
  val query : ('a -> unit) -> ('b -> unit) -> ('b, 'a) query -> unit

  val annot :
    (('a annot pred, 'a annot) query -> unit) -> ('a -> 'b) -> 'a annot -> 'b
end

module Annotate_obj : sig
  val annot :
    ('a -> 'b) ->
    ('c -> 'd -> 'e) ->
    (('a annot -> 'b) -> ('e annot pred, 'e annot) query -> 'd) ->
    'c annot ->
    'e annot

  val query :
    ('a -> 'b) ->
    ('c -> 'd -> 'e) ->
    (('a annot -> 'b) -> ('e annot pred, 'e annot) query -> 'd) ->
    ('c annot pred, 'c annot) query ->
    ('e annot pred, 'e annot) query

  val pred :
    ('a -> 'b) ->
    ('c -> 'd -> 'e) ->
    (('a annot -> 'b) -> ('e annot pred, 'e annot) query -> 'd) ->
    'c annot pred ->
    'e annot pred
end

include module type of Visitors_gen
