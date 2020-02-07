open! Core
open Collections

type tuple = Ast.tuple = Cross | Zip | Concat
[@@deriving compare, hash, sexp_of]

type order = Ast.order = Asc | Desc [@@deriving compare, hash, sexp_of]

type pred = Pred.t [@@deriving compare, hash, sexp_of]

and hash_idx = Ast.hash_idx = {
  hi_keys : t;
  hi_values : t;
  hi_scope : string;
  hi_key_layout : t option;
  hi_lookup : pred list;
}
[@@deriving compare, hash, sexp_of]

and bound = pred * [ `Open | `Closed ]

and ordered_idx = Ast.ordered_idx = {
  oi_key_layout : t option;
  oi_lookup : (bound option * bound option) list;
}
[@@deriving compare, hash, sexp_of]

and depjoin = Ast.depjoin = { d_lhs : t; d_alias : string; d_rhs : t }
[@@deriving compare, hash, sexp_of]

and join = Ast.join = { pred : pred; r1 : t; r2 : t }
[@@deriving compare, hash, sexp_of]

and order_by = Ast.order_by = { key : (pred * order) list; rel : t }
[@@deriving compare, hash, sexp_of]

and node = Ast.node =
  | Select of (pred list * t)
  | Filter of (pred * t)
  | Join of join
  | DepJoin of depjoin
  | GroupBy of (pred list * Name.t list * t)
  | OrderBy of order_by
  | Dedup of t
  | Relation of Relation.t
  | Range of pred * pred
  | AEmpty
  | AScalar of pred
  | AList of (t * t)
  | ATuple of (t list * tuple)
  | AHashIdx of hash_idx
  | AOrderedIdx of (t * t * ordered_idx)
  | As of string * t
[@@deriving compare, hash, sexp_of]

and t = Ast.t = { node : node; meta : Meta.t }
[@@deriving compare, hash, sexp_of]

include Comparator.S with type t := t

module O : Comparable.Infix with type t := t

val pp_pred : Format.formatter -> pred -> unit

val pp : Format.formatter -> t -> unit

val pp_small : Format.formatter -> t -> unit

val pp_small_str : unit -> t -> string

val mk_pp :
  ?pp_name:(Format.formatter -> Name.t -> unit) ->
  ?pp_meta:(Format.formatter -> Univ_map.t -> unit) ->
  unit ->
  (Format.formatter -> t -> unit) * (Format.formatter -> pred -> unit)

val name : t -> string

val names : t -> Set.M(Name).t
(** The set of names in a `t`. *)

val range : pred -> pred -> t

val select : pred list -> t -> t

val dep_join : t -> string -> t -> t

val dep_join' : depjoin -> t

val join : pred -> t -> t -> t

val filter : pred -> t -> t

val group_by : pred list -> Name.t list -> t -> t

val dedup : t -> t

val order_by : (pred * order) list -> t -> t

val relation : Relation.t -> t

val empty : t

val scalar : pred -> t

val list : t -> string -> t -> t

val list' : t * t -> t

val tuple : t list -> tuple -> t

val hash_idx : ?key_layout:t -> t -> string -> t -> pred list -> t

val hash_idx' : hash_idx -> t

val h_key_layout : hash_idx -> t

val ordered_idx : t -> string -> t -> ordered_idx -> t

val o_key_layout : t * t * ordered_idx -> t

val as_ : string -> t -> t

val strip_scope : t -> t

val scope : t -> string option

val scope_exn : t -> string

val alpha_scopes : t -> t

val and_ : pred list -> pred

val schema_exn : t -> Name.t list

val of_string_exn : string -> t

val name_of_string_exn : string -> Name.t

val of_channel_exn : In_channel.t -> t

val subst : pred Map.M(Name).t -> t -> t

val select_kind : pred list -> [ `Agg | `Scalar ]

val is_serializeable : t -> (unit, string) result

val pred_free : pred -> Set.M(Name).t

val free : t -> Set.M(Name).t

val annotate_free : t -> unit

val annotate_eq : t -> unit

val annotate_orders : t -> unit

val order_of : t -> (pred * order) list

val validate : t -> unit

val strip_meta : t -> t

class virtual ['a] iter : ['a] Abslayout_visitors.iter

class virtual ['a] map : ['a] Abslayout_visitors.map

class virtual ['a] endo : ['a] Abslayout_visitors.endo

class virtual ['a] reduce : ['a] Abslayout_visitors.reduce

class virtual ['a] mapreduce : ['a] Abslayout_visitors.mapreduce

class virtual ['a] fold : ['a] Abslayout_visitors.fold

val annotate_key_layouts : t -> t

val strip_unused_as : t -> t

val list_to_depjoin : t -> t -> t

val hash_idx_to_depjoin : hash_idx -> t

val ordered_idx_to_depjoin : t -> t -> ordered_idx -> t

val ensure_alias : t -> t

val aliases : t -> Pred.t Map.M(Name).t

val relations : t -> Set.M(Relation).t
