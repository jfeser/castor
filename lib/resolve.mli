open Ast

type error =
  [ `Ambiguous_names of Name.t list
  | `Ambiguous_stage of Name.t
  | `Unbound of Name.t * Name.t list ]

val pp_err : ([> error ] as 'a) Fmt.t -> 'a Fmt.t

val resolve_exn :
  ?params:Set.M(Name).t ->
  (< .. > as 'a) annot ->
  < refs : bool Map.M(Name).t
  ; stage : [ `Compile | `Run ] Map.M(Name).t
  ; meta : 'a >
  annot

val resolve :
  ?params:Set.M(Name).t ->
  (< .. > as 'a) annot ->
  ( < refs : bool Map.M(Name).t
    ; stage : [ `Compile | `Run ] Map.M(Name).t
    ; meta : 'a >
    annot,
    [> error ] )
  result
