open Base
module Pervasives = Caml.Pervasives

type 'a name = {relation: string option; name: string; type_: 'a [@compare.ignore]}
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

and 'f ralgebra =
  | Select of string * 'f pred list * 'f ralgebra
  | Filter of string * 'f pred * 'f ralgebra
  | Join of
      { pred: 'f pred
      ; r1_name: string
      ; r1: 'f ralgebra
      ; r2_name: string
      ; r2: 'f ralgebra }
  | Agg of string * 'f agg list * 'f list * 'f ralgebra
  | Dedup of 'f ralgebra
  | Scan of string
  | AEmpty
  | AScalar of 'f pred
  | AList of 'f ralgebra * string * 'f ralgebra
  | ATuple of 'f ralgebra list * tuple
  | AHashIdx of 'f ralgebra * string * 'f ralgebra * 'f hash_idx
  | AOrderedIdx of 'f ralgebra * string * 'f ralgebra * 'f ordered_idx
[@@deriving
  visitors {variety= "endo"}
  , visitors {variety= "map"}
  , visitors {variety= "iter"}
  , visitors {variety= "reduce"}
  , visitors {variety= "fold"; ancestors= ["map"]}
  , compare
  , sexp]

[@@@warning "+7"]

type t = Type0.PrimType.t option name ralgebra
