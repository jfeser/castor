open! Core
open Collections

type t [@@deriving compare, sexp]

val ( @ ) : t -> t -> t

val shallowest_first : t -> t -> int

val deepest_first : t -> t -> int

val root : t

val length : t -> int

val set_exn : t -> Abslayout.t -> Abslayout.t -> Abslayout.t

val get_exn : t -> Abslayout.t -> Abslayout.t

val stage_exn : t -> Abslayout.t -> [ `Compile | `Run ]

val all : Abslayout.t -> t Seq.t

val is_run_time : Abslayout.t -> t -> bool

val is_compile_time : Abslayout.t -> t -> bool

val parent : t -> t option

val child : t -> int -> t
