open Ast

type 'a meta =
  < type_ : Type.t
  ; pos : int option
  ; fold_stream : Abslayout_fold.Data.t
  ; Equiv.meta
  ; meta : 'a >

val serialize :
  ?layout_file:string ->
  string ->
  (< fold_stream : Abslayout_fold.Data.t ; type_ : Type.t ; Equiv.meta ; .. >
   as
   'a)
  annot ->
  'a meta annot * int
(** Serialize a layout to a binary format.

      @return the layout, annotated with the byte position of each
      single-appearing sub-layout. Also returns the number of bytes written. *)

(** Sentinal values for use when a value is null. *)

val string_sentinal : string_t -> int
val int_sentinal : int_t -> int
val date_sentinal : date_t -> int
val fixed_sentinal : fixed_t -> int
val bool_sentinal : int
