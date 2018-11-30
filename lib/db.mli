open Base
open Collections

type t

val create : string -> t

val create_url : string -> t

val copy : t -> t

val exec :
  ?max_retries:int -> ?params:string list -> t -> string -> Postgresql.result

val command_ok : Postgresql.result -> unit

val result_to_strings : Postgresql.result -> string list list

val exec_cursor :
  ?batch_size:int -> ?params:string list -> t -> string -> string list Gen.t

val load_value_exn : Type.PrimType.t -> string -> Value.t

val load_tuple_exn : Name.t list -> string list -> (Name.t * Value.t) list

val to_tuples :
  Name.t list -> string list Gen.t -> Value.t Map.M(Name.Compare_no_type).t Gen.t

module Field : sig
  type t = {fname: string; type_: Type.PrimType.t} [@@deriving compare, sexp]
end

module Relation : sig
  type db = t

  type t = {rname: string; fields: Field.t list} [@@deriving compare, hash, sexp]

  val from_db : db -> string -> t

  val all_from_db : db -> t list
end
