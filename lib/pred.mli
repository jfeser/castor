open! Core
module Binop = Ast.Binop
module Unop = Ast.Unop

type t = Ast.pred =
  | Name of Name.t
  | Int of int
  | Fixed of Fixed_point.t
  | Date of Date.t
  | Bool of bool
  | String of string
  | Null of Prim_type.t option
  | Unop of Unop.t * t
  | Binop of Binop.t * t * t
  | As_pred of (t * string)
  | Count
  | Row_number
  | Sum of t
  | Avg of t
  | Min of t
  | Max of t
  | If of t * t * t
  | First of Ast.t
  | Exists of Ast.t
  | Substring of t * t * t
[@@deriving compare, hash, sexp_of, variants]

include Comparator.S with type t := t

module O : Comparable.Infix with type t := t

module Infix : sig
  val name : Name.t -> t

  val int : int -> t

  val fixed : Fixed_point.t -> t

  val date : Date.t -> t

  val bool : bool -> t

  val string : string -> t

  val null : Prim_type.t option -> t

  val not : t -> t

  val day : t -> t

  val month : t -> t

  val year : t -> t

  val strlen : t -> t

  val extract_y : t -> t

  val extract_m : t -> t

  val extract_d : t -> t

  val ( + ) : t -> t -> t

  val ( - ) : t -> t -> t

  val ( / ) : t -> t -> t

  val ( * ) : t -> t -> t

  val ( = ) : t -> t -> t

  val ( < ) : t -> t -> t

  val ( <= ) : t -> t -> t

  val ( > ) : t -> t -> t

  val ( >= ) : t -> t -> t

  val ( && ) : t -> t -> t

  val ( || ) : t -> t -> t

  val ( mod ) : t -> t -> t

  val strpos : t -> t -> t

  val as_ : t -> string -> t
end

val pp : Format.formatter -> t -> unit

val of_lexbuf_exn : Lexing.lexbuf -> t

val of_string_exn : string -> t

val kind : t -> [ `Agg | `Scalar | `Window ]

val to_type : t -> Prim_type.t

val to_name : t -> Name.t option

val eqs : t -> (Name.t * Name.t) List.t

val names : t -> Set.M(Name).t

val collect_aggs : t -> t * (string * t) List.t

val simplify : t -> t

val scoped : Name.t List.t -> string -> t -> t
(** Apply a scope to each occurrence of a name in the list. Does not descend into
   subqueries. *)

val unscoped : String.t -> t -> t

val remove_as : t -> t

val ensure_alias : t -> t
(** Ensure that a predicate is decorated with an alias. If the predicate is
   nameless, then the alias will be fresh. *)

val subst : t Map.M(Name).t -> t -> t

val subst_tree : (t, t, 'a) Map.t -> t -> t

val conjoin : t list -> t

val conjuncts : t -> t List.t

val disjuncts : t -> t List.t

val sum_exn : t List.t -> t

val pseudo_bool : t -> t

val min_of : t -> t -> t

val max_of : t -> t -> t

val to_nnf : t -> t

val to_static : params:Set.M(Name).t -> t -> t
