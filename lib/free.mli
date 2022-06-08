open Core
open Ast

val free_query_open :
  schema_set:('r -> Set.M(Name).t) ->
  ('r -> Set.M(Name).t) ->
  ('r pred, 'r) query ->
  Set.M(Name).t

val free : _ annot -> Set.M(Name).t
val pred_free : _ annot pred -> Set.M(Name).t
val annotate : 'a annot -> < free : Set.M(Name).t ; meta : 'a > annot
