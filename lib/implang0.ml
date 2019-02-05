open Base
module Pervasives = Caml.Pervasives

type op =
  | Int2Fl
  | Int2Date
  | Date2Int
  | IntAdd
  | IntSub
  | IntMul
  | IntDiv
  | Mod
  | FlAdd
  | FlSub
  | FlMul
  | FlDiv
  | IntLt
  | FlLt
  | FlLe
  | FlEq
  | IntEq
  | StrEq
  | And
  | Or
  | Not
  | IntHash
  | StrHash
  | LoadStr
  | LoadBool
  | StrLen
  | StrPos
  | ExtractY
  | ExtractM
  | ExtractD
  | AddY
  | AddM
[@@deriving compare, sexp]

type expr =
  | Null
  | Int of (int[@opaque])
  | Date of (Core.Date.t[@opaque])
  | Fixed of (Fixed_point.t[@opaque])
  | Bool of (bool[@opaque])
  | String of (string[@opaque])
  | Var of (string[@opaque])
  | Tuple of expr list
  | Slice of expr * int
  | Index of expr * int
  | Binop of {op: (op[@opaque]); arg1: expr; arg2: expr}
  | Unop of {op: (op[@opaque]); arg: expr}
  | Done of string
  | Ternary of expr * expr * expr
  | TupleHash of (Type.PrimType.t[@opaque]) list * expr * expr
  | Substr of expr * expr * expr

and stmt =
  | Print of (Type.PrimType.t[@opaque]) * expr
  | Consume of (Type.PrimType.t[@opaque]) * expr
  | Loop of {cond: expr; body: prog}
  | If of {cond: expr; tcase: prog; fcase: prog}
  | Iter of {var: string; func: string; args: expr list}
  | Step of {var: string; iter: string}
  | Assign of {lhs: string; rhs: expr}
  | Yield of expr
  | Return of expr

and prog = stmt list

and local = {lname: string; type_: (Type.PrimType.t[@opaque]); persistent: bool}

and func =
  { name: string
  ; args: (string * (Type.PrimType.t[@opaque])) list
  ; body: prog
  ; ret_type: (Type.PrimType.t[@opaque])
  ; locals: local list }
[@@deriving
  compare
  , sexp
  , visitors {variety= "endo"}
  , visitors {variety= "map"}
  , visitors {variety= "iter"}
  , visitors {variety= "reduce"}]
