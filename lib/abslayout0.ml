open Base
module Pervasives = Caml.Pervasives

type name =
  { relation: string option
  ; name: string
  ; type_: Type0.PrimType.t option [@compare.ignore] }
[@@deriving compare, sexp, hash]

(* Visitors doesn't use the special method override syntax that warning 7 checks
   for. *)
[@@@warning "-7"]

type 'f pred =
  | Name of 'f
  | Int of (int[@opaque])
  | Bool of (bool[@opaque])
  | String of (string[@opaque])
  | Null
  | Binop of ((Ralgebra0.op[@opaque]) * 'f pred * 'f pred)
  | Varop of ((Ralgebra0.op[@opaque]) * 'f pred list)

and 'f agg = 'f Ralgebra0.agg =
  | Count
  | Key of 'f
  | Sum of 'f
  | Avg of 'f
  | Min of 'f
  | Max of 'f

and 'f hash_idx = {lookup: 'f pred}

and 'f ordered_idx = {lookup_low: 'f pred; lookup_high: 'f pred; order: 'f pred}

and tuple = Cross | Zip

and ('f, 'm) ralgebra = {node: ('f, 'm) node; meta: 'm}

and ('f, 'm) node =
  | Select of 'f pred list * ('f, 'm) ralgebra
  | Filter of 'f pred * ('f, 'm) ralgebra
  | Join of {pred: 'f pred; r1: ('f, 'm) ralgebra; r2: ('f, 'm) ralgebra}
  | Agg of 'f agg list * 'f list * ('f, 'm) ralgebra
  | Dedup of ('f, 'm) ralgebra
  | Scan of string
  | AEmpty
  | AScalar of 'f pred
  | AList of ('f, 'm) ralgebra * ('f, 'm) ralgebra
  | ATuple of ('f, 'm) ralgebra list * tuple
  | AHashIdx of ('f, 'm) ralgebra * ('f, 'm) ralgebra * 'f hash_idx
  | AOrderedIdx of ('f, 'm) ralgebra * ('f, 'm) ralgebra * 'f ordered_idx
  | As of string * ('f, 'm) ralgebra
[@@deriving
  visitors {variety= "endo"}
  , visitors {variety= "map"}
  , visitors {variety= "iter"}
  , visitors {variety= "reduce"}
  , visitors {variety= "fold"; ancestors= ["map"]}
  , compare
  , sexp]

[@@@warning "+7"]

type 'm t = (name, 'm) ralgebra [@@deriving sexp]
