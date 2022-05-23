open Core
open Ast
module Binop = Ast.Binop
module Unop = Ast.Unop

type t = Ast.t Ast.pred [@@deriving compare, hash, sexp]

include Comparator.S with type t := t
module O : Comparable.Infix with type t := t

module Infix : sig
  val name : Name.t -> 'a annot pred
  val name_s : string -> 'a annot pred
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
  val exists : 'a annot -> 'a annot pred
  val count : 'a annot pred
end

val pp : 'a annot pred Fmt.t
val of_lexbuf_exn : Lexing.lexbuf -> t
val of_string_exn : string -> t
val kind : _ annot pred -> [ `Agg | `Scalar | `Window ]
val to_type : 'a annot pred -> Prim_type.t
val eqs : 'a annot pred -> (Name.t * Name.t) list

val equiv : 'a annot pred -> Name.t Union_find.t Map.M(Name).t
(** [equiv p] returns the equivalence class for each name mentioned in [p]. *)

val names : 'a annot pred -> Set.M(Name).t

val collect_aggs :
  'a annot pred -> 'a annot pred * ('a annot pred * string) list

val simplify : t -> t

val scoped : Name.t list -> string -> 'a annot pred -> 'a annot pred
(** Apply a scope to each occurrence of a name in the list. Does not descend into
   subqueries. *)

val unscoped : String.t -> 'a annot pred -> 'a annot pred
val subst : 'a annot pred Map.M(Name).t -> 'a annot pred -> 'a annot pred

val subst_tree :
  ('a annot pred, 'a annot pred, _) Map.t -> 'a annot pred -> 'a annot pred

val conjoin : 'a annot pred list -> 'a annot pred
val conjuncts : 'a annot pred -> 'a annot pred list
val disjuncts : 'a annot pred -> 'a annot pred list
val sum_exn : 'a annot pred list -> 'a annot pred
val pseudo_bool : 'a annot pred -> 'a annot pred
val min_of : 'a annot pred -> 'a annot pred -> 'a annot pred
val max_of : 'a annot pred -> 'a annot pred -> 'a annot pred
val to_nnf : 'a annot pred -> 'a annot pred
val to_static : params:Set.M(Name).t -> t -> t
val strip_meta : < .. > annot pred -> t
val is_expensive : _ pred -> bool
val cse : ?min_uses:int -> t -> t * (Name.t * t) list
val map_names : f:(Name.t -> Name.t) -> 'a annot pred -> 'a annot pred
