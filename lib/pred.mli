open! Core

type t = Abslayout0.pred =
  | Name of Name.t
  | Int of int
  | Fixed of Fixed_point.t
  | Date of Core.Date.t
  | Bool of bool
  | String of string
  | Null of Type.PrimType.t option
  | Unop of (Abslayout0.unop * t)
  | Binop of (Abslayout0.binop * t * t)
  | As_pred of (t * string)
  | Count
  | Row_number
  | Sum of t
  | Avg of t
  | Min of t
  | Max of t
  | If of t * t * t
  | First of Abslayout0.t
  | Exists of Abslayout0.t
  | Substring of t * t * t
[@@deriving compare, hash, variants, sexp_of]

include Comparator.S with type t := t

module O : Comparable.Infix with type t := t

val pp : Format.formatter -> t -> unit

val of_lexbuf_exn : Lexing.lexbuf -> t

val of_string_exn : string -> t

val kind : t -> [ `Agg | `Scalar | `Window ]

val to_type : t -> Type.PrimType.t

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
