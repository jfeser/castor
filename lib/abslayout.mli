open! Core
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
  | Date of Date.t
  | Bool of bool
  | String of string
  | Null of Type.PrimType.t option
  | Unop of (unop * pred)
  | Binop of (binop * pred * pred)
  | As_pred of (pred * string)
  | Count
  | Row_number
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
  { hi_keys: t
  ; hi_values: t
  ; hi_scope: string
  ; hi_key_layout: t option
  ; hi_lookup: pred list }
[@@deriving compare, hash, sexp_of]

and bound = pred * ([`Open | `Closed][@opaque])

and ordered_idx = Abslayout0.ordered_idx =
  {oi_key_layout: t option; oi_lookup: (bound option * bound option) list}
[@@deriving compare, hash, sexp_of]

and relation = Abslayout0.relation = {r_name: string; r_schema: Name.t list option}
[@@deriving compare, hash, sexp_of]

and depjoin = Abslayout0.depjoin = {d_lhs: t; d_alias: string; d_rhs: t}
[@@deriving compare, hash, sexp_of]

and join = Abslayout0.join = {pred: pred; r1: t; r2: t}
[@@deriving compare, hash, sexp_of]

and order_by = Abslayout0.order_by = {key: (pred * order) list; rel: t}
[@@deriving compare, hash, sexp_of]

and node = Abslayout0.node =
  | Select of (pred list * t)
  | Filter of (pred * t)
  | Join of join
  | DepJoin of depjoin
  | GroupBy of (pred list * Name.t list * t)
  | OrderBy of order_by
  | Dedup of t
  | Relation of relation
  | AEmpty
  | AScalar of pred
  | AList of (t * t)
  | ATuple of (t list * tuple)
  | AHashIdx of hash_idx
  | AOrderedIdx of (t * t * ordered_idx)
  | As of string * t
[@@deriving compare, hash, sexp_of]

and t = Abslayout0.t = {node: node; meta: Meta.t [@compare.ignore]}
[@@deriving compare, hash, sexp_of]

include Comparator.S with type t := t

module O : Comparable.Infix with type t := t

val pp_pred : Format.formatter -> pred -> unit

val pp : Format.formatter -> t -> unit

val pp_small : Format.formatter -> t -> unit

val pp_small_str : unit -> t -> string

val mk_pp :
     ?pp_name:(Format.formatter -> Name.t -> unit)
  -> ?pp_meta:(Format.formatter -> Univ_map.t -> unit)
  -> unit
  -> (Format.formatter -> t -> unit) * (Format.formatter -> pred -> unit)

val name : t -> string

val names : t -> Set.M(Name).t
(** The set of names in a `t`. *)

val select : pred list -> t -> t

val dep_join : t -> string -> t -> t

val dep_join' : depjoin -> t

val join : pred -> t -> t -> t

val filter : pred -> t -> t

val group_by : pred list -> Name.t list -> t -> t

val dedup : t -> t

val order_by : (pred * order) list -> t -> t

val relation : relation -> t

val empty : t

val scalar : pred -> t

val list : t -> string -> t -> t

val list' : t * t -> t

val tuple : t list -> tuple -> t

val hash_idx : ?key_layout:t -> t -> string -> t -> pred list -> t

val hash_idx' : hash_idx -> t

val h_key_layout : hash_idx -> t

val ordered_idx : t -> string -> t -> ordered_idx -> t

val o_key_layout : ordered_idx -> t

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

val select_kind : pred list -> [`Agg | `Scalar]

val is_serializeable : t -> bool

val pred_free : pred -> Set.M(Name).t

val free : t -> Set.M(Name).t

val annotate_free : t -> unit

val annotate_eq : t -> unit

val annotate_orders : t -> unit

val order_of : t -> (pred * order) list

val validate : t -> unit

val strip_meta : t -> t

class virtual ['a] iter : ['a] Abslayout0.iter

class virtual ['a] map : ['a] Abslayout0.map

class virtual ['a] endo : ['a] Abslayout0.endo

class virtual ['a] reduce : ['a] Abslayout0.reduce

class virtual ['a] mapreduce : ['a] Abslayout0.mapreduce

val annotate_key_layouts : t -> t

val strip_unused_as : t -> t

val list_to_depjoin : t -> t -> t

val hash_idx_to_depjoin : hash_idx -> t

val ordered_idx_to_depjoin : t -> t -> ordered_idx -> t

val ensure_alias : t -> t

val aliases : t -> Pred.t Map.M(Name).t
