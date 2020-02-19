open Ast
open Collections

type t [@@deriving compare, sexp]

val ( @ ) : t -> t -> t

val shallowest_first : t -> t -> int

val deepest_first : t -> t -> int

val root : t

val length : t -> int

val set_exn : t -> Ast.t -> Ast.t -> Ast.t

val get_exn : t -> 'a annot -> 'a annot
(** Return a subtree rooted at the end of a path. *)

val stage_exn : t -> 'a annot -> [ `Compile | `Run ]

val all : 'a annot -> t Seq.t

val is_run_time : 'a annot -> t -> bool

val is_compile_time : 'a annot -> t -> bool

val parent : t -> t option

val child : t -> int -> t
