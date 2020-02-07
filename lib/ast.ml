open! Core

module Meta = struct
  type t = Univ_map.t ref [@@deriving sexp_of]
end

module Binop = struct
  type t =
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
end

module Unop = struct
  type t = Not | Day | Month | Year | Strlen | ExtractY | ExtractM | ExtractD
  [@@deriving compare, hash, sexp]
end

type scope = string [@@deriving compare, hash, sexp]

type tuple = Cross | Zip | Concat [@@deriving compare, hash, sexp]

type order = Asc | Desc [@@deriving compare, hash, sexp]

type pred =
  | Name of Name.t
  | Int of int
  | Fixed of Fixed_point.t
  | Date of Date.t
  | Bool of bool
  | String of string
  | Null of Prim_type.t option
  | Unop of Unop.t * pred
  | Binop of Binop.t * pred * pred
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

and hash_idx = {
  hi_keys : t;
  hi_values : t;
  hi_scope : scope;
  hi_key_layout : t option;
  hi_lookup : pred list;
}

and bound = pred * [ `Open | `Closed ]

and ordered_idx = {
  oi_key_layout : t option;
  oi_lookup : (bound option * bound option) list;
}

and depjoin = { d_lhs : t; d_alias : scope; d_rhs : t }

and join = { pred : pred; r1 : t; r2 : t }

and order_by = { key : (pred * order) list; rel : t }

and t = { node : node; meta : Meta.t [@compare.ignore] }

and node =
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
  | As of scope * t
[@@deriving sexp_of, hash, compare]

let t_of_sexp _ = assert false

module Param = struct
  type t = string * Prim_type.t * pred option
end
