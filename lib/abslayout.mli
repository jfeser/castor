open Base
open Collections

type binop = Abslayout0.binop =
  | Eq
  | Lt
  | Le
  | Gt
  | Ge
  | And
  | Or
  | Add
  | Sub
  | Mul
  | Div
  | Mod
  | Strpos
[@@deriving compare, hash, sexp]

type unop = Abslayout0.unop =
  | Not
  | Day
  | Month
  | Year
  | Strlen
  | ExtractY
  | ExtractM
  | ExtractD
[@@deriving compare, hash, sexp]

type tuple = Abslayout0.tuple = Cross | Zip | Concat
[@@deriving compare, hash, sexp_of]

type order = Abslayout0.order = Asc | Desc [@@deriving compare, hash, sexp_of]

type pred = Abslayout0.pred =
  | Name of Name.t
  | Int of int
  | Fixed of Fixed_point.t
  | Date of Core.Date.t
  | Bool of bool
  | String of string
  | Null
  | Unop of (unop * pred)
  | Binop of (binop * pred * pred)
  | As_pred of (pred * string)
  | Count
  | Sum of pred
  | Avg of pred
  | Min of pred
  | Max of pred
  | If of pred * pred * pred
  | First of t
  | Exists of t
  | Substring of pred * pred * pred
[@@deriving compare, hash, sexp_of]

and hash_idx = Abslayout0.hash_idx =
  {hi_key_layout: Abslayout0.t option; lookup: pred list}
[@@deriving compare, hash, sexp_of]

and ordered_idx = Abslayout0.ordered_idx =
  { oi_key_layout: Abslayout0.t option
  ; lookup_low: pred
  ; lookup_high: pred
  ; order: [`Asc | `Desc] }
[@@deriving compare, hash, sexp_of]

and node = Abslayout0.node =
  | Select of (pred list * t)
  | Filter of (pred * t)
  | Join of {pred: pred; r1: t; r2: t}
  | GroupBy of pred list * Name.t list * t
  | OrderBy of {key: (pred * order) list; rel: t}
  | Dedup of t
  | Scan of string
  | AEmpty
  | AScalar of pred
  | AList of (t * t)
  | ATuple of (t list * tuple)
  | AHashIdx of (t * t * hash_idx)
  | AOrderedIdx of (t * t * ordered_idx)
  | As of string * t
[@@deriving compare, hash, sexp_of]

and t = Abslayout0.t = {node: node; meta: Meta.t [@compare.ignore]}
[@@deriving compare, hash, sexp_of]

include Comparator.S with type t := t

module O : Comparable.Infix with type t := t

val pp_pred : Formatter.t -> pred -> unit

val pp : Formatter.t -> t -> unit

val name : t -> string

val names : t -> Set.M(Name).t
(** The set of names in a `t`. *)

val select : pred list -> t -> t

val join : pred -> t -> t -> t

val filter : pred -> t -> t

val group_by : pred list -> Name.t list -> t -> t

val dedup : t -> t

val order_by : (pred * order) list -> t -> t

val scan : string -> t

val empty : t

val scalar : pred -> t

val list : t -> t -> t

val tuple : t list -> tuple -> t

val hash_idx : t -> t -> pred list -> t

val hash_idx' : t -> t -> hash_idx -> t

val ordered_idx : t -> t -> ordered_idx -> t

val as_ : string -> t -> t

val and_ : pred list -> pred

module Ctx : sig
  type t = Value.t Map.M(Name).t [@@deriving compare, sexp]

  include Comparable.S with type t := t

  val empty : t
end

val pred_relations : pred -> string list

val of_string_exn : string -> t

val pred_of_string_exn : string -> pred

val name_of_string_exn : string -> Name.t

val of_channel_exn : Stdio.In_channel.t -> t

val subst : pred Map.M(Name).t -> t -> t

val subst_pred : pred Map.M(Name).t -> pred -> pred

val subst_single : pred -> pred -> t -> t

val pred_to_schema : pred -> Name.t

val pred_to_name : pred -> Name.t option

val annotate_align : t -> unit

val pred_of_value : Value.t -> pred

val annotate_foreach : t -> unit

val next_inner_loop : t -> (t * t) option

val pred_kind : pred -> [`Agg | `Scalar]

val select_kind : pred list -> [`Agg | `Scalar]

val is_serializeable : t -> bool

val pred_free : pred -> Set.M(Name).t

val free : t -> Set.M(Name).t

val annotate_free : t -> unit

val annotate_needed : t -> unit

val project : t -> t

val pred_remove_as : pred -> pred

val annotate_eq : t -> unit

val annotate_orders : t -> unit

val order_of : t -> (pred * order) list

val validate : t -> unit

val pred_constants : Name.t list -> pred -> pred list

val conjuncts : pred -> pred list

val conjoin : pred list -> pred

val aliases : t -> (string * t) list

val collect_aggs : fresh:Fresh.t -> pred -> pred * (string * pred) list

class virtual ['a] map : ['a] Abslayout0.map

class virtual ['a] endo : ['a] Abslayout0.endo

class virtual ['a] reduce : ['a] Abslayout0.reduce

class virtual ['a] mapreduce : ['a] Abslayout0.mapreduce
