open Collections

type t [@@deriving compare, sexp]

val ( @ ) : t -> t -> t

val shallowest_first : t -> t -> int

val deepest_first : t -> t -> int

val root : t

val length : t -> int

val set_exn : t -> Ast.t -> Ast.t -> Ast.t

val get_exn : t -> Ast.t -> Ast.t

val stage_exn : t -> Ast.t -> [ `Compile | `Run ]

val all : Ast.t -> t Seq.t

val is_run_time : Ast.t -> t -> bool

val is_compile_time : Ast.t -> t -> bool

val parent : t -> t option

val child : t -> int -> t
