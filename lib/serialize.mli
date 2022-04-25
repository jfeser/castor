open Ast

type 'a meta =
  < type_ : Type.t
  ; pos : int option
  ; fold_stream : Abslayout_fold.Data.t
  ; meta : 'a >

val serialize :
  ?layout_file:string ->
  string ->
  (< fold_stream : Abslayout_fold.Data.t ; type_ : Type.t ; .. > as 'a) annot ->
  'a meta annot * int
(** Serialize a layout to a binary format.

      @return the layout, annotated with the byte position of each
      single-appearing sub-layout. Also returns the number of bytes written. *)

(** Sentinal values for use when a value is null. *)

val string_sentinal : Type.string_ -> int
val int_sentinal : Type.int_ -> int
val date_sentinal : Type.date -> int
val fixed_sentinal : Type.fixed -> int
val bool_sentinal : int
