open! Core
open Ast

val project_once : int Map.M(Name).t annot -> unit annot

val project : ?params:Set.M(Name).t -> 'a annot -> unit annot
