open! Core
open Abslayout

module type S = sig
  val serialize : Unix.File_descr.t -> t -> t * int
  (** Serialize a layout to a binary format.

      @return The layout, annotated with the byte position of each
     single-appearing sub-layout. Also returns the number of bytes written. *)

  (** Sentinal values for use when a value is null. *)

  val string_sentinal : Type.string_ -> int

  val int_sentinal : Type.int_ -> int

  val date_sentinal : Type.date -> int

  val fixed_sentinal : Type.fixed -> int

  val bool_sentinal : int
end
