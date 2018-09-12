open Base
open Collections

module Name : sig
  type t = Abslayout0.name =
    {relation: string option; name: string; type_: Type.PrimType.t option}
  [@@deriving sexp]

  module Compare_no_type : sig
    type t = Abslayout0.name [@@deriving compare, hash, sexp]

    include Comparable.S with type t := t
  end

  module Compare_name_only : sig
    type t = Abslayout0.name [@@deriving compare, hash, sexp]

    include Comparable.S with type t := t
  end

  val create : ?relation:string -> ?type_:Type.PrimType.t -> string -> t

  val type_exn : t -> Type.PrimType.t

  val to_sql : t -> string

  val to_var : t -> string

  val of_string_exn : string -> t

  val of_field : ?rel:string -> Db.Field.t -> t
end

type meta = Abslayout0.meta

type op = Abslayout0.op =
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
[@@deriving compare, sexp]

type pred = Abslayout0.pred =
  | Name of Name.t
  | Int of (int[@opaque])
  | Bool of (bool[@opaque])
  | String of (string[@opaque])
  | Null
  | Binop of ((op[@opaque]) * pred * pred)
  | As_pred of (pred * string)
[@@deriving sexp_of]

type agg = Abslayout0.agg =
  | Count
  | Key of Name.t
  | Sum of Name.t
  | Avg of Name.t
  | Min of Name.t
  | Max of Name.t
[@@deriving sexp_of]

type hash_idx = Abslayout0.hash_idx =
  {hi_key_layout: Abslayout0.t option; lookup: pred list}
[@@deriving sexp_of]

type ordered_idx = Abslayout0.ordered_idx =
  { oi_key_layout: Abslayout0.t option
  ; lookup_low: pred
  ; lookup_high: pred
  ; order: [`Asc | `Desc] }
[@@deriving sexp_of]

type tuple = Abslayout0.tuple = Cross | Zip [@@deriving sexp_of]

type node = Abslayout0.node =
  | Select of pred list * t
  | Filter of pred * t
  | Join of {pred: pred; r1: t; r2: t}
  | Agg of agg list * Name.t list * t
  | OrderBy of {key: pred list; order: [`Asc | `Desc]; rel: t}
  | Dedup of t
  | Scan of string
  | AEmpty
  | AScalar of pred
  | AList of (t * t)
  | ATuple of (t list * tuple)
  | AHashIdx of (t * t * hash_idx)
  | AOrderedIdx of (t * t * ordered_idx)
  | As of string * t
[@@deriving sexp_of]

and t = Abslayout0.t = {node: node; meta: meta} [@@deriving sexp_of]

val pp : Formatter.t -> t -> unit

val name : t -> string

val params : t -> Set.M(Name.Compare_no_type).t

val select : pred list -> t -> t

val join : pred -> t -> t -> t

val filter : pred -> t -> t

val agg : agg list -> Name.t list -> t -> t

val dedup : t -> t

val order_by : pred list -> [`Asc | `Desc] -> t -> t

val scan : string -> t

val empty : t

val scalar : pred -> t

val list : t -> t -> t

val tuple : t list -> tuple -> t

val hash_idx : t -> t -> hash_idx -> t

val ordered_idx : t -> t -> ordered_idx -> t

val as_ : string -> t -> t

module Meta : sig
  type 'a key

  type pos = Pos of int64 | Many_pos

  val schema : Name.t list key

  val pos : pos key

  val align : int key

  val find : t -> 'a key -> 'a option

  val find_exn : t -> 'a key -> 'a

  val set : t -> 'a key -> 'a -> t

  val update : t -> 'a key -> f:('a option -> 'a) -> unit
end

module Ctx : sig
  type t = Db.primvalue Map.M(Name.Compare_no_type).t
  [@@deriving compare, hash, sexp]

  val empty : t

  val of_tuple : Db.Tuple.t -> t
end

val pred_relations : pred -> string list

val of_string_exn : string -> t

val of_channel_exn : Stdio.In_channel.t -> t

val subst : pred Map.M(Name.Compare_no_type).t -> t -> t

val subst_pred : pred Map.M(Name.Compare_no_type).t -> pred -> pred

val pred_to_schema : pred -> Name.t

val pred_to_name : pred -> Name.t option

val annotate_align : t -> unit

val pred_of_value : Db.primvalue -> pred
