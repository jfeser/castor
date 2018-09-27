open Base

val exec :
     ?max_retries:int
  -> ?verbose:bool
  -> ?params:string list
  -> Postgresql.connection
  -> string
  -> string list list

val exec_cursor :
     ?batch_size:int
  -> ?params:string list
  -> Postgresql.connection
  -> string
  -> Value.t Map.M(String).t Sequence.t

module Field : sig
  type t = {fname: string; type_: Type.PrimType.t} [@@deriving compare, sexp]
end

module Relation : sig
  type t = {rname: string; fields: Field.t list} [@@deriving compare, hash, sexp]

  val from_db : Postgresql.connection -> string -> t

  val all_from_db : Postgresql.connection -> t list
end
