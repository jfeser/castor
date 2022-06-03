open Core
open Ast

val free : _ annot -> Set.M(Name).t
val pred_free : _ annot pred -> Set.M(Name).t
val annotate : 'a annot -> < free : Set.M(Name).t ; meta : 'a > annot
