open Core
open Ast
open Collections

type error = [ `Parse_error of string * int * int ] [@@deriving sexp]

val pp_err : ([> error ] as 'a) Fmt.t -> 'a Fmt.t

include Comparator.S with type t := t
module O : Comparable.Infix with type t := t

val pp : _ annot Fmt.t
val name : t -> string

val names : t -> Set.M(Name).t
(** The set of names in a `t`. *)

val range : Pred.t -> Pred.t -> t
val select : Pred.t Select_list.t -> t -> t
val dep_join : t -> string -> t -> t
val dep_join' : t depjoin -> t
val join : Pred.t -> t -> t -> t
val filter : Pred.t -> t -> t
val group_by : Pred.t Select_list.t -> Name.t list -> t -> t
val dedup : t -> t
val order_by : (Pred.t * order) list -> t -> t
val relation : Relation.t -> t
val empty : t
val scalar : Pred.t -> string -> t
val scalar' : Pred.t scalar -> t
val scalar_name : Name.t -> t
val list : t -> string -> t -> t
val list' : (Pred.t, t) list_ -> t
val tuple : t list -> tuple -> t
val hash_idx : ?key_layout:t -> t -> string -> t -> Pred.t list -> t
val hash_idx' : (Pred.t, t) hash_idx -> t
val h_key_layout : (_, < .. > annot) hash_idx -> t

val ordered_idx :
  ?key_layout:t ->
  t ->
  string ->
  t ->
  (Pred.t bound option * Pred.t bound option) list ->
  t

val ordered_idx' : (Pred.t, t) ordered_idx -> t

(* val o_key_layout : *)
(*   (< .. > as 'a) annot * 'a annot * ('a annot pred, 'a annot, scope) ordered_idx -> *)
(*   < > annot *)

val alpha_scopes : 'a annot -> 'a annot
val and_ : Pred.t list -> Pred.t
val of_string_exn : string -> t
val name_of_string_exn : string -> Name.t
val of_channel_exn : In_channel.t -> t
val of_string : string -> (t, [> error ]) result
val name_of_string : string -> (Name.t, [> error ]) result
val of_channel : In_channel.t -> (t, [> error ]) result
val subst : 'a annot pred Map.M(Name).t -> 'a annot -> 'a annot
val select_kind : _ annot pred Select_list.t -> [ `Agg | `Scalar ]
val order_of : 'a annot -> (Pred.t * order) list
val annotate_key_layouts : t -> t

(* val aliases : t -> Pred.t Map.M(Name).t *)
(* val relations : t -> Set.M(Relation).t *)

val hoist_meta : < meta : 'a ; .. > annot -> 'a annot
