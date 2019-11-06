open! Core
open Collections

type t

val create : string -> t

val conn : t -> Postgresql.connection

val param : t Command.Spec.param

val exec :
  ?max_retries:int -> ?params:string list -> t -> string -> Postgresql.result

val command_ok : Postgresql.result -> unit Or_error.t

val command_ok_exn : Postgresql.result -> unit

val result_to_strings : Postgresql.result -> string list list

val exec_cursor_exn :
  ?batch_size:int ->
  ?params:string list ->
  t ->
  Type.PrimType.t list ->
  string ->
  Value.t array Gen.t

val exec_cursor_lwt_exn :
  ?batch_size:int ->
  ?params:string list ->
  ?timeout:float ->
  t ->
  Type.PrimType.t list ->
  string ->
  (Value.t array, exn) result Lwt_stream.t

val check : t -> string -> unit Or_error.t

val relation : ?with_types:bool -> t -> string -> Relation.t

val all_relations : t -> Relation.t list

val relation_has_field : t -> string -> Relation.t option
