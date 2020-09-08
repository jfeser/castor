open Ast

val project_once :
  params:Set.M(Name).t -> < refs : bool Map.M(Name).t ; .. > annot -> < > annot

val project :
  params:Set.M(Name).t -> ?max_iters:int -> < .. > annot -> < > annot
