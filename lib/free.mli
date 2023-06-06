open Core
open Ast

val pred_open : ('r -> Set.M(Name).t) -> 'r pred -> Set.M(Name).t

val query_open :
  schema:('r -> Set.M(Name).t) ->
  ('r -> Set.M(Name).t) ->
  ('r pred, 'r) query ->
  Set.M(Name).t

val annot : _ annot -> Set.M(Name).t
val pred : _ annot pred -> Set.M(Name).t
val annotate : 'a annot -> < free : Set.M(Name).t ; meta : 'a > annot
