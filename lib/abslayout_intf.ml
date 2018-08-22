open Base

module type S = sig
  module Name : sig
    type t = Abslayout0.name =
      {relation: string option; name: string; type_: Type.PrimType.t option}
    [@@deriving compare, sexp]

    include Comparable.S with type t := t

    val create : ?relation:string -> ?type_:Type.PrimType.t -> string -> t

    val of_string_exn : string -> t
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
    | Varop of ((op[@opaque]) * pred list)
  [@@deriving sexp_of]

  type agg = Abslayout0.agg =
    | Count
    | Key of Name.t
    | Sum of Name.t
    | Avg of Name.t
    | Min of Name.t
    | Max of Name.t
  [@@deriving sexp_of]

  type hash_idx = Abslayout0.hash_idx = {lookup: pred} [@@deriving sexp_of]

  and ordered_idx =
    {lookup_low: pred option; lookup_high: pred option; order: [`Asc | `Desc]}

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
    | AList of t * t
    | ATuple of t list * tuple
    | AHashIdx of t * t * hash_idx
    | AOrderedIdx of t * t * ordered_idx
    | As of string * t
  [@@deriving sexp_of]

  and t = Abslayout0.t = {node: node; meta: meta} [@@deriving sexp_of]

  val pp : Formatter.t -> t -> unit

  val name : t -> string

  val params : t -> (Type.TypedName.t, Type.TypedName.comparator_witness) Base.Set.t

  val select : pred Base.list -> t -> t

  val filter : pred -> t -> t

  val agg : agg Base.list -> Name.t Base.list -> t -> t

  val dedup : t -> t

  val scan : Base.string -> t

  val empty : t

  val scalar : pred -> t

  val list : t -> t -> t

  val tuple : t Base.list -> tuple -> t

  val hash_idx : t -> t -> hash_idx -> t

  val ordered_idx : t -> t -> ordered_idx -> t

  val as_ : Base.string -> t -> t

  module Meta : sig
    type 'a key

    type pos = Pos of int64 | Many_pos

    val schema : Name.t list key

    val pos : pos key

    val find_exn : t -> 'a key -> 'a
  end

  module Ctx : sig
    type t = Db.primvalue Base.Map.M(Name).t [@@deriving sexp]

    val of_tuple : Db.Tuple.t -> t
  end

  val of_string_exn : string -> t

  val of_channel_exn : Stdio.In_channel.t -> t

  val subst : Ctx.t -> t -> t

  val ralgebra_to_sql : t -> string

  val resolve : ?params:Set.M(Name).t -> Postgresql.connection -> t -> t
end
