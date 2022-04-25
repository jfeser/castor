open Core

type unop =
  [ `Int2Fl
  | `Int2Date
  | `Date2Int
  | `ExtractY
  | `ExtractM
  | `ExtractD
  | `Not
  | `StrLen
  | `LoadBool ]
[@@deriving compare, sexp]

type binop =
  [ `IntAdd
  | `IntSub
  | `IntMul
  | `IntDiv
  | `Lsr
  | `Mod
  | `FlAdd
  | `FlSub
  | `FlMul
  | `FlDiv
  | `IntLt
  | `FlLt
  | `FlLe
  | `FlEq
  | `IntEq
  | `StrEq
  | `And
  | `Or
  | `IntHash
  | `StrHash
  | `UnivHash
  | `LoadStr
  | `StrPos
  | `AddY
  | `AddM
  | `AddD ]
[@@deriving compare, sexp]

[@@@warning "-deprecated"]

type expr =
  | Null
  | Int of (int[@opaque])
  | Date of (Date.t[@opaque])
  | Fixed of (Fixed_point.t[@opaque])
  | Bool of (bool[@opaque])
  | String of (string[@opaque])
  | Var of (string[@opaque])
  | Tuple of expr list
  | Slice of expr * int
  | Index of expr * int
  | Binop of { op : (binop[@opaque]); arg1 : expr; arg2 : expr }
  | Unop of { op : (unop[@opaque]); arg : expr }
  | Done of string
  | Ternary of expr * expr * expr
  | TupleHash of (Prim_type.t[@opaque]) list * expr * expr
  | Substr of expr * expr * expr

and stmt =
  | Print of (Prim_type.t[@opaque]) * expr
  | Consume of (Prim_type.t[@opaque]) * expr
  | Loop of { cond : expr; body : prog }
  | If of { cond : expr; tcase : prog; fcase : prog }
  | Iter of { var : string; func : string; args : expr list }
  | Step of { var : string; iter : string }
  | Assign of { lhs : string; rhs : expr }
  | Yield of expr
  | Return of expr

and prog = stmt list

and local = {
  lname : string;
  type_ : (Prim_type.t[@opaque]);
  persistent : bool;
}

and func = {
  name : string;
  args : (string * (Prim_type.t[@opaque])) list;
  body : prog;
  ret_type : (Prim_type.t[@opaque]);
  locals : local list;
}
[@@deriving
  compare,
    sexp,
    visitors { variety = "endo" },
    visitors { variety = "map" },
    visitors { variety = "mapreduce" },
    visitors { variety = "iter" },
    visitors { variety = "reduce" }]

[@@@warning "+deprecated"]

let rec conjuncts = function
  | Binop { op = `And; arg1 = p1; arg2 = p2 } -> conjuncts p1 @ conjuncts p2
  | p -> [ p ]
