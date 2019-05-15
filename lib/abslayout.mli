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

and ordered_idx = Abslayout0.ordered_idx =
  { oi_key_layout: Abslayout0.t option
  ; lookup_low: pred
  ; lookup_high: pred
  ; order: [`Asc | `Desc] }
[@@deriving compare, hash, sexp_of]

and relation = Abslayout0.relation = {r_name: string; r_schema: Name.t list option}
[@@deriving compare, hash, sexp_of]

and depjoin = Abslayout0.depjoin = {d_lhs: t; d_alias: string; d_rhs: t}
[@@deriving compare, hash, sexp_of]

and node = Abslayout0.node =
  | Select of (pred list * t)
  | Filter of (pred * t)
  | Join of {pred: pred; r1: t; r2: t}
  | DepJoin of depjoin
  | GroupBy of pred list * Name.t list * t
  | OrderBy of {key: (pred * order) list; rel: t}
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

val pp_pred : Formatter.t -> pred -> unit

val pp : Formatter.t -> t -> unit

val pp_small : Formatter.t -> t -> unit

val pp_small_str : unit -> t -> string

val mk_pp :
     ?pp_name:(Formatter.t -> Name.t -> unit)
  -> ?pp_meta:(Formatter.t -> Univ_map.t -> unit)
  -> unit
  -> (Formatter.t -> t -> unit) * (Formatter.t -> pred -> unit)

val name : t -> string

val names : t -> Set.M(Name).t
(** The set of names in a `t`. *)

val wrap : node -> t

val select : pred list -> t -> t

val dep_join : t -> string -> t -> t

val join : pred -> t -> t -> t

val filter : pred -> t -> t

val group_by : pred list -> Name.t list -> t -> t

val dedup : t -> t

val order_by : (pred * order) list -> t -> t

val relation : relation -> t

val empty : t

val scalar : pred -> t

val list : t -> string -> t -> t

val tuple : t list -> tuple -> t

val hash_idx : ?key_layout:t -> t -> string -> t -> pred list -> t

val ordered_idx : t -> string -> t -> ordered_idx -> t

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

val aliases : t -> t Map.M(String).t

val strip_meta : t -> t

class virtual ['a] iter : ['a] Abslayout0.iter

class virtual ['a] map : ['a] Abslayout0.map

class virtual ['a] endo : ['a] Abslayout0.endo

class virtual ['a] reduce : ['a] Abslayout0.reduce

class virtual ['a] mapreduce : ['a] Abslayout0.mapreduce

module Pred : sig
  type a = t

  type t = pred =
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
    | Sum of pred
    | Avg of pred
    | Min of pred
    | Max of pred
    | If of pred * pred * pred
    | First of a
    | Exists of a
    | Substring of pred * pred * pred
  [@@deriving compare, hash, sexp_of, variants]

  include Comparator.S with type t := t

  val names : t -> (Name.t, Name.comparator_witness) Set.t

  val of_value : Value.t -> t

  val conjoin : t list -> t

  val collect_aggs : t -> t * (string * t) list

  val conjuncts : t -> t list

  val constants : Name.t list -> t -> t list

  val eqs : t -> (Name.t * Name.t) sexp_list

  val remove_as : t -> t

  val kind : t -> [`Agg | `Scalar]

  val of_lexbuf_exn : Lexing.lexbuf -> t

  val of_string_exn : string -> t

  val subst : t Map.M(Name).t -> t -> t

  val scoped : Name.t list -> string -> t -> t

  val unscoped : string -> t -> t

  val relations : t -> string list

  val to_name : t -> Name.t option

  val to_type : t -> Type.PrimType.t
end

val annotate_key_layouts : t -> t

val strip_unused_as : t -> t

val list_to_depjoin : t -> t -> t

val hash_idx_to_depjoin : hash_idx -> t

val ordered_idx_to_depjoin : t -> t -> ordered_idx -> t

val ensure_alias : t -> t
