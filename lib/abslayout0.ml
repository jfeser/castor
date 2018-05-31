open Base
module Pervasives = Caml.Pervasives

type 'n quant = (string option * 'n)

type 'f pred = 'f Ralgebra0.pred =
  | Var of (Type0.TypedName.t [@opaque])
  | Field of 'f
  | Int of int
  | Bool of bool
  | String of string
  | Null
  | Binop of ((Ralgebra0.op [@opaque]) * 'f pred * 'f pred)
  | Varop of ((Ralgebra0.op [@opaque]) * 'f pred list)

and 'f agg = 'f Ralgebra0.agg =
  | Count
  | Key of 'f
  | Sum of 'f
  | Avg of 'f
  | Min of 'f
  | Max of 'f

and ('f, 'l) ralgebra =
  | Select of 'f pred list * ('f, 'l) ralgebra
  | Filter of 'f pred * ('f, 'l) ralgebra
  | Join of {
      pred : 'f pred;
      r1_name : string;
      r1 : ('f, 'l) ralgebra;
      r2_name : string;
      r2 : ('f, 'l) ralgebra
    }
  | Scan of 'l
  | Agg of 'f agg list * 'f list * ('f, 'l) ralgebra
  | Dedup of ('f, 'l) ralgebra
[@@deriving visitors { variety = "endo"; name = "ralgebra_endo" },
            visitors { variety = "map"; name = "ralgebra_map" },
            visitors { variety = "iter"; name = "ralgebra_iter" },
            visitors { variety = "reduce"; name = "ralgebra_reduce" },
            sexp]

type 'f hash_idx = {
  lookup : 'f pred;
}

and 'f ordered_idx = {
  lookup_low : 'f pred;
  lookup_high : 'f pred;
  order : 'f pred;
}

and tuple = Cross | Zip
and ('f, 'r) layout =
  | AEmpty
  | AScalar of 'f pred
  | AList of ('f, 'r) ralgebra * string * ('f, 'r) layout
  | ATuple of ('f, 'r) layout list * tuple
  | AHashIdx of ('f, 'r) ralgebra * string * ('f, 'r) layout * 'f hash_idx
  | AOrderedIdx of ('f, 'r) ralgebra * string * ('f, 'r) layout * 'f ordered_idx
[@@deriving visitors { variety = "fold" },
            visitors { variety = "endo" },
            visitors { variety = "map" },
            visitors { variety = "reduce" },
            sexp]

type t = (Db.Field.t, (Db.Field.t, Db.Relation.t) layout) ralgebra [@@deriving sexp]
