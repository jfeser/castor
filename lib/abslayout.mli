open! Core
open Ast
open Collections

include Comparator.S with type t := t

module O : Comparable.Infix with type t := t

val pp_pred : Format.formatter -> Pred.t -> unit

val pp : Format.formatter -> 'a annot -> unit

val pp_small : Format.formatter -> 'a annot -> unit

val pp_small_str : unit -> 'a annot -> string

val mk_pp :
  ?pp_name:(Format.formatter -> Name.t -> unit) ->
  ?pp_meta:(Format.formatter -> 'a -> unit) ->
  unit ->
  (Format.formatter -> 'a annot -> unit)
  * (Format.formatter -> 'a annot pred -> unit)

val name : t -> string

val names : t -> Set.M(Name).t
(** The set of names in a `t`. *)

val range : Pred.t -> Pred.t -> t

val select : Pred.t list -> t -> t

val dep_join : t -> string -> t -> t

val dep_join' : t depjoin -> t

val join : Pred.t -> t -> t -> t

val filter : Pred.t -> t -> t

val group_by : Pred.t list -> Name.t list -> t -> t

val dedup : t -> t

val order_by : (Pred.t * order) list -> t -> t

val relation : Relation.t -> t

val empty : t

val scalar : Pred.t -> t

val list : t -> string -> t -> t

val list' : t * t -> t

val tuple : t list -> tuple -> t

val hash_idx : ?key_layout:t -> t -> string -> t -> Pred.t list -> t

val hash_idx' : (Pred.t, t) hash_idx -> t

val h_key_layout : ('a annot pred, 'a annot) hash_idx -> unit annot

val ordered_idx : t -> string -> t -> (Pred.t, t) ordered_idx -> t

val o_key_layout :
  'a annot * 'a annot * ('a annot pred, 'a annot) ordered_idx -> unit annot

val as_ : string -> t -> t

val strip_scope : 'a annot -> 'a annot

val scope : 'a annot -> string option

val scope_exn : 'a annot -> string

val alpha_scopes : 'a annot -> 'a annot

val and_ : Pred.t list -> Pred.t

val schema_exn : t -> Name.t list

val of_string_exn : string -> t

val name_of_string_exn : string -> Name.t

val of_channel_exn : In_channel.t -> t

val subst : 'a annot pred Map.M(Name).t -> 'a annot -> 'a annot

val select_kind : 'a annot pred list -> [ `Agg | `Scalar ]

val is_serializeable : t -> (unit, string) result

val pred_free : 'a annot pred -> Set.M(Name).t

val free : 'a annot -> Set.M(Name).t

val annotate_free : t -> unit

val annotate_eq : t -> unit

val annotate_orders : t -> unit

val order_of : t -> (Pred.t * order) list

val validate : t -> unit

val strip_meta : t -> t

class virtual ['a] iter : ['a] Abslayout_visitors.iter

class virtual ['a] map : ['a] Abslayout_visitors.map

class virtual ['a] endo : ['a] Abslayout_visitors.endo

class virtual ['a] reduce : ['a] Abslayout_visitors.reduce

class virtual ['a] mapreduce : ['a] Abslayout_visitors.mapreduce

val annotate_key_layouts : t -> t

val strip_unused_as : t -> t

val list_to_depjoin : 'a annot -> 'a annot -> 'a annot depjoin

val hash_idx_to_depjoin :
  ('a annot pred, 'a annot) hash_idx -> unit annot depjoin

val ordered_idx_to_depjoin :
  'a annot ->
  'a annot ->
  ('a annot pred, 'a annot) ordered_idx ->
  unit annot depjoin

val ensure_alias : 'a annot -> 'a annot

val aliases : t -> Pred.t Map.M(Name).t

val relations : t -> Set.M(Relation).t
