open Core
open Ast

type resolved

type error =
  [ `Resolve of
    Ast.t
    * [ `Ambiguous_names of Name.t list
      | `Ambiguous_stage of Name.t
      | `Unbound of Name.t * Name.t list ] ]

type 'a meta =
  < refs : bool Map.M(Name).t
  ; stage : [ `Compile | `Run ] Map.M(Name).t
  ; resolved : resolved
  ; meta : 'a >

exception Resolve_error of error

val pp_err : ([> error ] as 'a) Fmt.t -> 'a Fmt.t
val resolve_exn : params:Set.M(Name).t -> (< .. > as 'a) annot -> 'a meta annot

val resolve :
  params:Set.M(Name).t ->
  (< .. > as 'a) annot ->
  ('a meta annot, [> error ]) result
