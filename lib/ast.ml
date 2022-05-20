open Core

type meta = < >

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
  [@@deriving compare, equal, hash, sexp]
end

module Unop = struct
  type t = Not | Day | Month | Year | Strlen | ExtractY | ExtractM | ExtractD
  [@@deriving compare, equal, hash, sexp]
end

type scope = string [@@deriving compare, equal, hash, sexp]
type tuple = Cross | Zip | Concat [@@deriving compare, equal, hash, sexp]
type order = Asc | Desc [@@deriving compare, equal, hash, sexp]

type ('p, 'r) ppred =
  [ `Name of Name.t
  | `Int of int
  | `Fixed of Fixed_point.t
  | `Date of Date.t
  | `Bool of bool
  | `String of string
  | `Null of Prim_type.t option
  | `Unop of Unop.t * 'p
  | `Binop of Binop.t * 'p * 'p
  | `Count
  | `Row_number
  | `Sum of 'p
  | `Avg of 'p
  | `Min of 'p
  | `Max of 'p
  | `If of 'p * 'p * 'p
  | `First of 'r
  | `Exists of 'r
  | `Substring of 'p * 'p * 'p ]
[@@deriving compare, equal, hash, sexp]

type 'r pred = ('r pred, 'r) ppred [@@deriving compare, equal, hash, sexp]

type ('p, 'r) hash_idx = {
  hi_keys : 'r;
  hi_values : 'r;
  hi_scope : scope;
  hi_key_layout : 'r option; [@sexp.option]
  hi_lookup : 'p list;
}
[@@deriving compare, equal, hash, sexp]

type 'p bound = 'p * [ `Open | `Closed ] [@@deriving compare, equal, hash, sexp]

type ('p, 'r) ordered_idx = {
  oi_keys : 'r;
  oi_values : 'r;
  oi_scope : scope;
  oi_key_layout : 'r option; [@sexp.option]
  oi_lookup : ('p bound option * 'p bound option) list;
}
[@@deriving compare, equal, hash, sexp]

type 'r list_ = { l_keys : 'r; l_values : 'r; l_scope : scope }
[@@deriving compare, equal, hash, sexp]

type 'r depjoin = { d_lhs : 'r; d_alias : scope; d_rhs : 'r }
[@@deriving compare, equal, hash, sexp]

type ('p, 'r) join = { pred : 'p; r1 : 'r; r2 : 'r }
[@@deriving compare, equal, hash, sexp]

type ('p, 'r) order_by = { key : ('p * order) list; rel : 'r }
[@@deriving compare, equal, hash, sexp]

type 'p scalar = { s_pred : 'p; s_name : string }
[@@deriving compare, equal, hash, sexp]

type 'p select_list = ('p * string) list [@@deriving compare, equal, hash, sexp]

type 'p scan_type = {
  select : 'p select_list;
  filter : 'p list;
  tables : Relation.t list;
}
[@@deriving compare, equal, hash, sexp]

type ('p, 'r) query =
  | Select of ('p select_list * 'r)
  | Filter of ('p * 'r)
  | Join of ('p, 'r) join
  | DepJoin of 'r depjoin
  | GroupBy of ('p select_list * Name.t list * 'r)
  | OrderBy of ('p, 'r) order_by
  | Dedup of 'r
  | Relation of Relation.t
  | Range of ('p * 'p)
  | AEmpty
  | AScalar of 'p scalar
  | AList of 'r list_
  | ATuple of ('r list * tuple)
  | AHashIdx of ('p, 'r) hash_idx
  | AOrderedIdx of ('p, 'r) ordered_idx
  | Call of string
[@@deriving compare, equal, hash, sexp, variants]

type 'm annot = { node : ('m annot pred, 'm annot) query; meta : 'm }
[@@deriving compare, equal, hash, sexp]

module T = struct
  type t = (meta[@ignore] [@sexp.opaque]) annot
  [@@deriving compare, equal, hash, sexp]
end

include T
include Comparator.Make (T)

module Param = struct
  type nonrec t = string * Prim_type.t * t pred option
end

module Query = struct
  type 'a t = {
    name : string;
    args : (string * Prim_type.t) list;
    body : 'a annot;
  }
  [@@deriving compare, hash, sexp_of]
end

let strip_meta r = (r :> t)
