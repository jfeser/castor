open Base
open Collections

type op = Implang0.op =
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

type expr = Implang0.expr =
  | Null
  | Int of int
  | Date of Core.Date.t
  | Fixed of Fixed_point.t
  | Bool of bool
  | String of string
  | Var of string
  | Tuple of expr list
  | Slice of expr * int
  | Index of expr * int
  | Binop of {op: op; arg1: expr; arg2: expr}
  | Unop of {op: op; arg: expr}
  | Done of string
  | Ternary of expr * expr * expr
  | TupleHash of Type.PrimType.t list * expr * expr
  | Substr of expr * expr * expr
[@@deriving compare, sexp]

type local = Implang0.local =
  {lname: string; type_: (Type.PrimType.t[@opaque]); persistent: bool}
[@@deriving compare, sexp]

type stmt = Implang0.stmt =
  | Print of Type.PrimType.t * expr
  | Consume of Type.PrimType.t * expr
  | Loop of {cond: expr; body: prog}
  | If of {cond: expr; tcase: prog; fcase: prog}
  | Iter of {var: string; func: string; args: expr list}
  | Step of {var: string; iter: string}
  | Assign of {lhs: string; rhs: expr}
  | Yield of expr
  | Return of expr

and prog = stmt list [@@deriving compare, sexp]

type func = Implang0.func =
  { name: string
  ; args: (string * Type0.PrimType.t) list
  ; body: prog
  ; ret_type: Type0.PrimType.t
  ; locals: local list }
[@@deriving compare, sexp]

val pp_stmt : Formatter.t -> stmt -> unit

val pp_func : Formatter.t -> func -> unit

val yield_count : func -> int

val name_of_var : expr -> string

val int2fl : expr -> expr

module Infix : sig
  val int : int -> expr

  val ( + ) : expr -> expr -> expr

  val ( - ) : expr -> expr -> expr

  val ( * ) : expr -> expr -> expr

  val ( / ) : expr -> expr -> expr

  val ( % ) : expr -> expr -> expr

  val ( < ) : expr -> expr -> expr

  val ( > ) : expr -> expr -> expr

  val ( <= ) : expr -> expr -> expr

  val ( >= ) : expr -> expr -> expr

  val ( && ) : expr -> expr -> expr

  val ( || ) : expr -> expr -> expr

  val not : expr -> expr

  val index : expr -> int -> expr
end

type _var = Global of expr | Arg of int | Field of expr

type _ctx = _var Map.M(Name).t

val type_of : Type.PrimType.t Hashtbl.M(String).t -> expr -> Type.PrimType.t

module Builder : sig
  type t

  val type_of : expr -> t -> Type.PrimType.t

  val create : ctx:_ctx -> name:string -> ret:Type.PrimType.t -> fresh:Fresh.t -> t

  val new_scope : t -> t

  val build_arg : int -> t -> expr

  val build_yield : expr -> t -> unit

  val build_func : t -> func

  val build_assign : expr -> expr -> t -> unit

  val build_unchecked_assign : expr -> expr -> t -> unit

  val build_print : expr -> t -> unit

  val build_consume : expr -> t -> unit

  val build_return : expr -> t -> unit

  val build_loop : expr -> (t -> unit) -> t -> unit

  val build_iter : func -> expr list -> t -> unit

  val build_step : expr -> func -> t -> unit

  val build_if : cond:expr -> then_:(t -> unit) -> else_:(t -> unit) -> t -> unit

  val build_var : ?persistent:bool -> string -> Type.PrimType.t -> t -> expr

  val build_defn : ?persistent:bool -> string -> expr -> t -> expr

  val build_count_loop : expr -> (t -> unit) -> t -> unit

  val build_foreach :
       ?count:Type.AbsInt.t
    -> ?header:(expr -> t -> unit)
    -> ?footer:(expr -> t -> unit)
    -> ?persistent:bool
    -> func
    -> expr list
    -> (expr -> t -> unit)
    -> t
    -> unit

  val build_eq : expr -> expr -> t -> expr

  val build_lt : expr -> expr -> t -> expr

  val build_le : expr -> expr -> t -> expr

  val build_gt : expr -> expr -> t -> expr

  val build_ge : expr -> expr -> t -> expr

  val build_add : expr -> expr -> t -> expr

  val build_sub : expr -> expr -> t -> expr

  val build_mul : expr -> expr -> t -> expr

  val build_div : expr -> expr -> t -> expr

  val build_concat : expr list -> t -> expr

  val build_printstr : string -> t -> unit

  val build_hash : expr -> expr -> t -> expr

  val build_to_int : expr -> t -> expr

  val const_int : Type.PrimType.t -> int -> expr
end

module Ctx : sig
  type var = _var = Global of expr | Arg of int | Field of expr
  [@@deriving compare, sexp]

  type t = _ctx [@@deriving compare, sexp]

  val empty : t

  val of_schema : Name.t list -> expr list -> t

  val make_caller_args : t -> (string * Type0.PrimType.t) list

  val bind : t -> string -> Type0.PrimType.t -> expr -> t

  val var_to_expr : var -> Builder.t -> expr

  val make_callee_context : t -> Builder.t -> t * expr list

  val find : t -> Name.t -> Builder.t -> expr option

  val find_exn : t -> Name.t -> Builder.t -> expr

  val bind_ctx : t -> t -> t
end
