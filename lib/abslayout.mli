open Core
open Ast
open Collections

type error = [ `Parse_error of string * int * int ] [@@deriving sexp]

val pp_err : ([> error ] as 'a) Fmt.t -> 'a Fmt.t

include Comparator.S with type t := t
(** @closed *)

module O : Comparable.Infix with type t := t

val pp : _ annot Fmt.t
val name : t -> string

val names : t -> Set.M(Name).t
(** The set of names in a `t`. *)

(* val o_key_layout : *)
(*   (< .. > as 'a) annot * 'a annot * ('a annot pred, 'a annot, scope) ordered_idx -> *)
(*   < > annot *)

val of_string_exn : string -> t
val name_of_string_exn : string -> Name.t
val of_channel_exn : In_channel.t -> t
val of_string : string -> (t, [> error ]) result
val name_of_string : string -> (Name.t, [> error ]) result
val of_channel : In_channel.t -> (t, [> error ]) result
val select_kind : _ pred Select_list.t -> [ `Agg | `Scalar ]
val order_of : (< Equiv.meta ; .. > as 'm) annot -> ('m annot pred * order) list
val annotate_key_layouts : t -> t

(* val aliases : t -> Pred.t Map.M(Name).t *)
(* val relations : t -> Set.M(Relation).t *)

val hoist_meta : < meta : 'a ; .. > annot -> 'a annot

val hoist_meta_query :
  ((< meta : 'm ; .. > as 'a) annot pred, 'a annot) query ->
  ('m annot pred, 'm annot) query

val hoist_meta_pred : < meta : 'm ; .. > annot pred -> 'm annot pred
