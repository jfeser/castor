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
val parent : t -> t option
val child : t -> int -> t
val is_prefix : t -> prefix:t -> bool
val deepest : ('a -> t Seq.t) -> 'a -> t option
val shallowest : ('a -> t Seq.t) -> 'a -> t option

type 'a pred = 'a annot -> t -> bool
(** Path predicate *)

val is_run_time : 'a pred
val is_compile_time : 'a pred
val is_join : 'a pred
val is_groupby : 'a pred
val is_orderby : 'a pred
val is_filter : 'a pred
val is_expensive_filter : 'a pred
val is_dedup : 'a pred
val is_relation : 'a pred
val is_select : 'a pred
val is_agg_select : 'a pred
val is_hash_idx : 'a pred
val is_ordered_idx : 'a pred
val is_scalar : 'a pred
val is_list : 'a pred
val is_tuple : 'a pred
val is_depjoin : 'a pred
val has_child : 'a pred -> 'a pred
