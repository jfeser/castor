type t

val create : ?pool_size:int -> string -> t

val conn : t -> Postgresql.connection

val param : t Command.Param.t

val exec :
  ?max_retries:int -> ?params:string list -> t -> string -> Postgresql.result

val exec1 : ?params:string list -> t -> string -> string list

val exec2 : ?params:string list -> t -> string -> (string * string) list

val exec3 :
  ?params:string list -> t -> string -> (string * string * string) list

val command_ok : Postgresql.result -> unit Or_error.t

val command_ok_exn : Postgresql.result -> unit

val result_to_strings : Postgresql.result -> string list list

val exec_cursor_exn :
  ?batch_size:int ->
  ?params:string list ->
  t ->
  Prim_type.t list ->
  string ->
  Value.t array Gen.t

val check : t -> string -> unit Or_error.t

val relation : ?with_types:bool -> t -> string -> Relation.t

val all_relations : t -> Relation.t list

val relation_count : t -> string -> int

val relation_has_field : t -> string -> Relation.t option

val eq_join_type :
  t -> string -> string -> [ `Left | `Right | `Both | `Neither ] Or_error.t

module Async : sig
  type error = {
    query : string;
    info : [ `Timeout | `Exn of Exn.t | `Msg of string ];
  }
  [@@deriving sexp_of]

  val to_error : error -> Error.t

  type 'a exec =
    ?timeout:float ->
    ?cancel:unit Lwt.t ->
    t ->
    'a ->
    (Value.t array, error) result Lwt_stream.t

  val exec_sql : (Prim_type.t list * string) exec

  val exec : 'a Ast.annot exec
end
