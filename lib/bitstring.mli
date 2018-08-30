open Base

type t

val of_int : ?null:bool -> width:int -> int -> t

val of_int64 : ?null:bool -> width:int -> int64 -> t

val of_bytes : bytes -> t

val of_string : string -> t

val label : string -> t -> t

val concat : t list -> t

val empty : t

(* val append : t -> t -> t *)

val length : t -> int

val byte_length : t -> int

val int_length : t -> int

val pp : Caml.Format.formatter -> t -> unit

module Writer : sig
  type bitstring = t

  module Pos : sig
    type t

    val ( - ) : t -> t -> t

    val to_bits : t -> int64

    val to_bytes_exn : t -> int64
  end

  type t

  val with_file : string -> t

  val with_buffer : Buffer.t -> t

  val write_bit : t -> int -> unit

  val write_char : t -> char -> unit

  val write_bytes : t -> bytes -> unit

  val write_string : t -> string -> unit

  val write_file : ?buf_len:int -> t -> string -> unit

  val write : t -> bitstring -> unit

  val flush : t -> unit

  val pos : t -> Pos.t

  val seek : t -> Pos.t -> unit

  val close : t -> unit
end

val to_string : t -> string

val tests : OUnitTest.test