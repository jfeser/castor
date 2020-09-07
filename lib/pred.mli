open Ast
module Binop = Ast.Binop
module Unop = Ast.Unop

type t = Ast.t Ast.pred [@@deriving compare, hash, sexp]

include Comparator.S with type t := t

module O : Comparable.Infix with type t := t

module Infix : sig
  val name : Name.t -> 'a annot pred

  val int : int -> 'a annot pred

  val fixed : Fixed_point.t -> 'a annot pred

  val date : Date.t -> 'a annot pred

  val bool : bool -> 'a annot pred

  val string : string -> 'a annot pred

  val null : Prim_type.t option -> 'a annot pred

  val not : 'a annot pred -> 'a annot pred

  val day : 'a annot pred -> 'a annot pred

  val month : 'a annot pred -> 'a annot pred

  val year : 'a annot pred -> 'a annot pred

  val strlen : 'a annot pred -> 'a annot pred

  val extract_y : 'a annot pred -> 'a annot pred

  val extract_m : 'a annot pred -> 'a annot pred

  val extract_d : 'a annot pred -> 'a annot pred

  val ( + ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val ( - ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val ( / ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val ( * ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val ( = ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val ( < ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val ( <= ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val ( > ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val ( >= ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val ( && ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val ( || ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val ( mod ) : 'a annot pred -> 'a annot pred -> 'a annot pred

  val strpos : 'a annot pred -> 'a annot pred -> 'a annot pred

  val as_ : 'a annot pred -> string -> 'a annot pred

  val exists : 'a annot -> 'a annot pred

  val count : 'a annot pred
end

val pp : Format.formatter -> 'a annot pred -> unit

val of_lexbuf_exn : Lexing.lexbuf -> t

val of_string_exn : string -> t

val kind : 'a annot pred -> [ `Agg | `Scalar | `Window ]

val to_type : 'a annot pred -> Prim_type.t

val to_name : 'a annot pred -> Name.t option

val eqs : 'a annot pred -> (Name.t * Name.t) List.t

val names : 'a annot pred -> Set.M(Name).t

val collect_aggs :
  'a annot pred -> 'a annot pred * (string * 'a annot pred) List.t

val simplify : t -> t

val scoped : Name.t List.t -> string -> 'a annot pred -> 'a annot pred
(** Apply a scope to each occurrence of a name in the list. Does not descend into
   subqueries. *)

val unscoped : String.t -> 'a annot pred -> 'a annot pred

val remove_as : 'a annot pred -> 'a annot pred

val ensure_alias : 'a annot pred -> 'a annot pred
(** Ensure that a predicate is decorated with an alias. If the predicate is
   nameless, then the alias will be fresh. *)

val subst : 'a annot pred Map.M(Name).t -> 'a annot pred -> 'a annot pred

val subst_tree :
  ('a annot pred, 'a annot pred, _) Map.t -> 'a annot pred -> 'a annot pred

val conjoin : 'a annot pred list -> 'a annot pred

val conjuncts : 'a annot pred -> 'a annot pred List.t

val disjuncts : 'a annot pred -> 'a annot pred List.t

val sum_exn : 'a annot pred List.t -> 'a annot pred

val pseudo_bool : 'a annot pred -> 'a annot pred

val min_of : 'a annot pred -> 'a annot pred -> 'a annot pred

val max_of : 'a annot pred -> 'a annot pred -> 'a annot pred

val to_nnf : 'a annot pred -> 'a annot pred

val to_static : params:Set.M(Name).t -> t -> t

val strip_meta : < .. > annot pred -> t

val is_expensive : _ pred -> bool

val cse : ?min_uses:int -> t -> t * (Name.t * t) list
