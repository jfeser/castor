open Core
open Ast
module Binop = Ast.Binop
module Unop = Ast.Unop

type t = Ast.t Ast.pred [@@deriving compare, hash, sexp]

include Comparator.S with type t := t
module O : Comparable.Infix with type t := t

module Infix : sig
  val name : Name.t -> _ pred
  val name_s : string -> _ pred
  val int : int -> _ pred
  val fixed : Fixed_point.t -> _ pred
  val date : Date.t -> _ pred
  val bool : bool -> _ pred
  val string : string -> _ pred
  val null : Prim_type.t option -> _ pred
  val not : 'a pred -> 'a pred
  val day : 'a pred -> 'a pred
  val month : 'a pred -> 'a pred
  val year : 'a pred -> 'a pred
  val strlen : 'a pred -> 'a pred
  val extract_y : 'a pred -> 'a pred
  val extract_m : 'a pred -> 'a pred
  val extract_d : 'a pred -> 'a pred
  val ( + ) : 'a pred -> 'a pred -> 'a pred
  val ( - ) : 'a pred -> 'a pred -> 'a pred
  val ( / ) : 'a pred -> 'a pred -> 'a pred
  val ( * ) : 'a pred -> 'a pred -> 'a pred
  val ( = ) : 'a pred -> 'a pred -> 'a pred
  val ( < ) : 'a pred -> 'a pred -> 'a pred
  val ( <= ) : 'a pred -> 'a pred -> 'a pred
  val ( > ) : 'a pred -> 'a pred -> 'a pred
  val ( >= ) : 'a pred -> 'a pred -> 'a pred
  val ( && ) : 'a pred -> 'a pred -> 'a pred
  val ( || ) : 'a pred -> 'a pred -> 'a pred
  val ( mod ) : 'a pred -> 'a pred -> 'a pred
  val and_ : 'a pred list -> 'a pred
  val strpos : 'a pred -> 'a pred -> 'a pred
  val exists : 'a -> 'a pred
  val count : _ pred
end

val pp : 'a annot pred Fmt.t
val of_lexbuf_exn : Lexing.lexbuf -> t
val of_string_exn : string -> t
val kind : _ pred -> [ `Agg | `Scalar | `Window ]
val to_type : 'a annot pred -> Prim_type.t
val eqs : 'a annot pred -> (Name.t * Name.t) list

val equiv : 'a annot pred -> Name.t Union_find.t Map.M(Name).t
(** [equiv p] returns the equivalence class for each name mentioned in [p]. *)

val names : 'a annot pred -> Set.M(Name).t

val collect_aggs :
  'a annot pred -> 'a annot pred * ('a annot pred * string) list

val simplify : t -> t

val scoped : Name.t list -> string -> 'a pred -> 'a pred
(** Apply a scope to each occurrence of a name in the list. Does not descend into
   subqueries. *)

val unscoped : String.t -> 'a pred -> 'a pred
val subst : 'a pred Map.M(Name).t -> 'a pred -> 'a pred

val subst_tree :
  ('a annot pred, 'a annot pred, _) Map.t -> 'a annot pred -> 'a annot pred

val conjoin : 'a pred list -> 'a pred
val conjuncts : 'a pred -> 'a pred list
val disjuncts : 'a pred -> 'a pred list
val sum_exn : 'a pred list -> 'a pred
val pseudo_bool : 'a pred -> 'a pred
val min_of : 'a pred -> 'a pred -> 'a pred
val max_of : 'a pred -> 'a pred -> 'a pred
val to_nnf : 'a annot pred -> 'a annot pred
val to_static : params:Set.M(Name).t -> t -> t
val strip_meta : < .. > annot pred -> t
val is_expensive : _ pred -> bool
val cse : ?min_uses:int -> t -> t * (Name.t * t) list
val map_names : f:(Name.t -> Name.t) -> 'a annot pred -> 'a annot pred
