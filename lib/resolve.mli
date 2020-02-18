open Ast

val resolve :
  ?params:Set.M(Name).t ->
  'a annot ->
  < refs : bool Map.M(Name).t ; stage : [ `Compile | `Run ] Map.M(Name).t >
  annot
