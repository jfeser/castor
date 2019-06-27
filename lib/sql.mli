open! Core

type t

val src : Logs.Src.t

val of_ralgebra : Abslayout.t -> t

val to_string : t -> string

val to_string_hum : t -> string
(** Pretty print a SQL string if a sql formatter is available. *)
