open Base

type meta = Core.Univ_map.t ref [@@deriving sexp_of]

type name =
  { relation: string option
  ; name: string
  ; type_: Type0.PrimType.t option [@compare.ignore] }
[@@deriving compare, sexp, hash]

(* - Visitors doesn't use the special method override syntax that warning 7 checks
   for.
   - It uses the Pervasives module directly, which Base doesn't like. *)
[@@@warning "-7"]

module Pervasives = Caml.Pervasives

type pred =
  | Name of (name[@opaque])
  | Int of (int[@opaque])
  | Bool of (bool[@opaque])
  | String of (string[@opaque])
  | Null
  | Binop of ((Ralgebra0.op[@opaque]) * pred * pred)
  | Varop of ((Ralgebra0.op[@opaque]) * pred list)

and agg =
  | Count
  | Key of (name[@opaque])
  | Sum of (name[@opaque])
  | Avg of (name[@opaque])
  | Min of (name[@opaque])
  | Max of (name[@opaque])

and hash_idx = {lookup: pred}

and ordered_idx = {lookup_low: pred; lookup_high: pred; order: pred}

and tuple = Cross | Zip

and t = {node: node; meta: meta [@opaque] [@compare.ignore]}

and node =
  | Select of pred list * t
  | Filter of pred * t
  | Join of {pred: pred; r1: t; r2: t}
  | Agg of agg list * (name[@opaque]) list * t
  | Dedup of t
  | Scan of string
  | AEmpty
  | AScalar of pred
  | AList of t * t
  | ATuple of t list * tuple
  | AHashIdx of t * t * hash_idx
  | AOrderedIdx of t * t * ordered_idx
  | As of string * t
[@@deriving
  visitors {variety= "endo"}
  , visitors {variety= "map"}
  , visitors {variety= "iter"}
  , visitors {variety= "reduce"}
  , visitors {variety= "fold"; ancestors= ["map"]}
  , sexp_of
  , compare]

[@@@warning "+7"]
