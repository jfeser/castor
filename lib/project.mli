open Ast

val project_once : < refs : bool Map.M(Name).t ; .. > annot -> < > annot

val project :
  ?params:Set.M(Name).t -> ?max_iters:int -> < .. > annot -> < > annot
