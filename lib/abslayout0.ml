open Base

type meta = Core.Univ_map.t ref [@@deriving sexp_of]

type name =
  { relation: string option
  ; name: string
  ; type_: Type0.PrimType.t option [@compare.ignore] }
[@@deriving compare, sexp, hash]

type binop = Eq | Lt | Le | Gt | Ge | And | Or | Add | Sub | Mul | Div | Mod
[@@deriving compare, sexp]

type unop = Day | Month | Year [@@deriving compare, sexp]

(* - Visitors doesn't use the special method override syntax that warning 7 checks
   for.
   - It uses the Pervasives module directly, which Base doesn't like. *)
[@@@warning "-7"]

module Pervasives = Caml.Pervasives

type pred =
  | Name of (name[@opaque])
  | Int of (int[@opaque])
  | Fixed of (Fixed_point.t[@opaque])
  | Date of (Core.Date.t[@opaque])
  | Bool of (bool[@opaque])
  | String of (string[@opaque])
  | Null
  | Unop of ((unop[@opaque]) * pred)
  | Binop of ((binop[@opaque]) * pred * pred)
  | As_pred of (pred * string)
  | Count
  | Sum of pred
  | Avg of pred
  | Min of pred
  | Max of pred
  | If of pred * pred * pred
  | First of t
  | Exists of t

and hash_idx = {hi_key_layout: t option; lookup: pred list}

and ordered_idx =
  { oi_key_layout: t option
  ; lookup_low: pred
  ; lookup_high: pred
  ; order: ([`Asc | `Desc][@opaque]) }

and tuple = Cross | Zip

and t = {node: node; meta: meta [@opaque] [@compare.ignore]}

and node =
  | Select of pred list * t
  | Filter of pred * t
  | Join of {pred: pred; r1: t; r2: t}
  | GroupBy of pred list * (name[@opaque]) list * t
  | OrderBy of {key: pred list; order: ([`Asc | `Desc][@opaque]); rel: t}
  | Dedup of t
  | Scan of string
  | AEmpty
  | AScalar of pred
  | AList of (t * t)
  | ATuple of (t list * tuple)
  | AHashIdx of (t * t * hash_idx)
  | AOrderedIdx of (t * t * ordered_idx)
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
