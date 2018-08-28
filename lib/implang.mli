open Base

type type_ =
  | NullT
  | IntT of {nullable: bool}
  | StringT of {nullable: bool}
  | BoolT of {nullable: bool}
  | TupleT of type_ list
  | VoidT
[@@deriving compare, sexp]

type op = Add | Sub | Mul | Div | Mod | Lt | Eq | And | Or | Not | Hash | LoadStr

type expr =
  | Null
  | Int of int
  | Bool of bool
  | String of string
  | Var of string
  | Tuple of expr list
  | Slice of expr * int
  | Index of expr * int
  | Binop of {op: op; arg1: expr; arg2: expr}
  | Unop of {op: op; arg: expr}
  | Done of string

and stmt =
  | Print of type_ * expr
  | Loop of {cond: expr; body: prog}
  | If of {cond: expr; tcase: prog; fcase: prog}
  | Iter of {var: string; func: string; args: expr list}
  | Step of {var: string; iter: string}
  | Assign of {lhs: string; rhs: expr}
  | Yield of expr
  | Return of expr

and prog = stmt list

and func =
  { name: string
  ; args: (string * type_) list
  ; body: prog
  ; ret_type: type_
  ; locals: (string * type_) list }
[@@deriving compare, sexp]

val pp_args : Formatter.t -> (string * 'a) list -> unit

val pp_tuple : (Formatter.t -> 'a -> unit) -> Formatter.t -> 'a list -> unit

val pp_type : Formatter.t -> type_ -> unit

val pp_bytes : Formatter.t -> bytes -> unit

val pp_int : Formatter.t -> int -> unit

val pp_bool : Formatter.t -> bool -> unit

val pp_expr : Formatter.t -> expr -> unit

val pp_stmt : Formatter.t -> stmt -> unit

val pp_prog : Formatter.t -> prog -> unit

val pp_func : Formatter.t -> func -> unit

val is_nullable : type_ -> bool

val infer_type : type_ Hashtbl.M(String).t -> expr -> type_

val yield_count : func -> int

module Config : sig
  module type S = sig
    val code_only : bool
  end
end

module IRGen : sig
  type ir_module =
    { iters: func list
    ; funcs: func list
    ; params: (string * type_) list
    ; buffer_len: int }
  [@@deriving sexp]

  exception IRGenError of Error.t

  module Make (Config : Config.S) (Eval : Eval.S) () : sig
    val irgen_abstract : data_fn:string -> Abslayout.t -> ir_module

    val pp : Formatter.t -> ir_module -> unit
  end
end
