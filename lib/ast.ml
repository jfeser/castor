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

(* Visitors doesn't use the special method override syntax that warning 7 checks
   for. *)
[@@@warning "-7"]

type pred =
  | Name of (Name.t[@opaque])
  | Int of (int[@opaque])
  | Fixed of (Fixed_point.t[@opaque])
  | Date of (Date.t[@opaque])
  | Bool of (bool[@opaque])
  | String of (string[@opaque])
  | Null of (Prim_type.t option[@opaque])
  | Unop of ((Unop.t[@opaque]) * pred)
  | Binop of ((Binop.t[@opaque]) * pred * pred)
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

and scope = string

and hash_idx = {
  hi_keys : t;
  hi_values : t;
  hi_scope : scope;
  hi_key_layout : t option; [@opaque]
  hi_lookup : pred list;
}

and bound = pred * ([ `Open | `Closed ][@opaque])

and ordered_idx = {
  oi_key_layout : t option;
  oi_lookup : (bound option * bound option) list;
}

and tuple = Cross | Zip | Concat

and order = Asc | Desc

and depjoin = { d_lhs : t; d_alias : scope; d_rhs : t }

and join = { pred : pred; r1 : t; r2 : t }

and order_by = { key : (pred * order) list; rel : t }

and t = { node : node; meta : Meta.t [@opaque] [@compare.ignore] }

and node =
  | Select of (pred list * t)
  | Filter of (pred * t)
  | Join of join
  | DepJoin of depjoin
  | GroupBy of (pred list * (Name.t[@opaque]) list * t)
  | OrderBy of order_by
  | Dedup of t
  | Relation of (Relation.t[@opaque])
  | Range of pred * pred
  | AEmpty
  | AScalar of pred
  | AList of (t * t)
  | ATuple of (t list * tuple)
  | AHashIdx of hash_idx
  | AOrderedIdx of (t * t * ordered_idx)
  | As of scope * t
[@@deriving
  visitors { variety = "endo" },
    visitors { variety = "map" },
    visitors { variety = "iter" },
    visitors { variety = "reduce" },
    visitors { variety = "fold"; ancestors = [ "map" ] },
    visitors { variety = "mapreduce" },
    sexp_of,
    hash,
    compare]

[@@@warning "+7"]

let t_of_sexp _ = assert false

module Param = struct
  type t = string * Prim_type.t * pred option
end

(* Visitors doesn't use the special method override syntax that warning 7 checks
   for. *)
[@@@warning "-7"]
