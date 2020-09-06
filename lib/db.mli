type t

val src : Logs.Src.t

val create : ?pool_size:int -> string -> t

val conn : t -> Postgresql.connection

val param : unit Command.Param.t

val psql_exec : ?max_retries:int -> t -> string -> Postgresql.result Or_error.t

val command_ok : Postgresql.result -> unit Or_error.t

val command_ok_exn : Postgresql.result -> unit

val run : t -> string -> string list list Or_error.t

val exec : t -> Prim_type.t list -> string -> Value.t list list Or_error.t

val exec_exn : t -> Prim_type.t list -> string -> Value.t list list

val exec_cursor_exn :
  ?count:int -> t -> Prim_type.t list -> string -> Value.t list list Sequence.t

val check : t -> string -> unit Or_error.t

val relation : t -> string -> Relation.t

val all_relations : t -> Relation.t list

val relation_has_field : t -> string -> Relation.t option

module Async : sig
  type error = {
    query : string;
    info : [ `Timeout | `Exn of Exn.t | `Msg of string ];
  }
  [@@deriving sexp_of]

  val to_error : error -> Error.t

  type 'a exec =
    ?timeout:float ->
    ?bound:int ->
    t ->
    'a ->
    (Value.t list list, error) result Lwt_stream.t

  val exec_sql : (Prim_type.t list * string) exec

  val exec : < .. > Ast.annot exec
end
