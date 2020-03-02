type meta = unit [@@deriving sexp_of]

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

type 'r pred =
  | Name of Name.t
  | Int of int
  | Fixed of Fixed_point.t
  | Date of Date.t
  | Bool of bool
  | String of string
  | Null of Prim_type.t option
  | Unop of Unop.t * 'r pred
  | Binop of Binop.t * 'r pred * 'r pred
  | As_pred of ('r pred * string)
  | Count
  | Row_number
  | Sum of 'r pred
  | Avg of 'r pred
  | Min of 'r pred
  | Max of 'r pred
  | If of 'r pred * 'r pred * 'r pred
  | First of 'r
  | Exists of 'r
  | Substring of 'r pred * 'r pred * 'r pred
[@@deriving compare, hash, sexp, variants]

type ('p, 'r) hash_idx = {
  hi_keys : 'r;
  hi_values : 'r;
  hi_scope : scope;
  hi_key_layout : 'r option; [@sexp.option]
  hi_lookup : 'p list;
}
[@@deriving compare, hash, sexp]

type 'p bound = 'p * [ `Open | `Closed ] [@@deriving compare, hash, sexp]

type ('p, 'r) ordered_idx = {
  oi_key_layout : 'r option; [@sexp.option]
  oi_lookup : ('p bound option * 'p bound option) list;
}
[@@deriving compare, hash, sexp]

type 'r depjoin = { d_lhs : 'r; d_alias : scope; d_rhs : 'r }
[@@deriving compare, hash, sexp]

type ('p, 'r) join = { pred : 'p; r1 : 'r; r2 : 'r }
[@@deriving compare, hash, sexp]

type ('p, 'r) order_by = { key : ('p * order) list; rel : 'r }
[@@deriving compare, hash, sexp]

type ('p, 'r) query =
  | Select of ('p list * 'r)
  | Filter of ('p * 'r)
  | Join of ('p, 'r) join
  | DepJoin of 'r depjoin
  | GroupBy of ('p list * Name.t list * 'r)
  | OrderBy of ('p, 'r) order_by
  | Dedup of 'r
  | Relation of Relation.t
  | Range of ('p * 'p)
  | AEmpty
  | AScalar of 'p
  | AList of ('r * 'r)
  | ATuple of ('r list * tuple)
  | AHashIdx of ('p, 'r) hash_idx
  | AOrderedIdx of ('r * 'r * ('p, 'r) ordered_idx)
  | As of scope * 'r
[@@deriving compare, hash, sexp, variants]

type 'm annot = { node : ('m annot pred, 'm annot) query; meta : 'm }
[@@deriving compare, hash, sexp]

type t = (meta[@compare.ignore]) annot [@@deriving compare, hash, sexp_of]

let t_of_sexp _ = assert false

module Param = struct
  type nonrec t = string * Prim_type.t * t pred option
end
