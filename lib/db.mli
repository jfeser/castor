open Base

type t

val create : string -> t

val exec :
  ?max_retries:int -> ?params:string list -> t -> string -> Postgresql.result

val result_to_tuples : Postgresql.result -> Value.t Map.M(String).t Sequence.t

val result_to_strings : Postgresql.result -> string list list

val exec_cursor :
     ?batch_size:int
  -> ?params:string list
  -> t
  -> string
  -> Value.t Map.M(String).t Sequence.t

module Field : sig
  type t = {fname: string; type_: Type.PrimType.t} [@@deriving compare, sexp]
end

module Relation : sig
  type db = t

  type t = {rname: string; fields: Field.t list} [@@deriving compare, hash, sexp]

  val from_db : db -> string -> t

  val all_from_db : db -> t list
end
