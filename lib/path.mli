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

val deepest : ('a -> t Seq.t) -> 'a -> t option

val shallowest : ('a -> t Seq.t) -> 'a -> t option

val is_join : 'a annot -> t -> bool

val is_groupby : 'a annot -> t -> bool

val is_orderby : 'a annot -> t -> bool

val is_filter : 'a annot -> t -> bool

val is_expensive_filter : 'a annot -> t -> bool

val is_dedup : 'a annot -> t -> bool

val is_relation : 'a annot -> t -> bool

val is_select : 'a annot -> t -> bool

val is_hash_idx : 'a annot -> t -> bool

val is_ordered_idx : 'a annot -> t -> bool

val is_scalar : 'a annot -> t -> bool

val is_list : 'a annot -> t -> bool

val is_tuple : 'a annot -> t -> bool

val is_depjoin : 'a annot -> t -> bool

val has_child : ('a annot -> t -> bool) -> 'a annot -> t -> bool
