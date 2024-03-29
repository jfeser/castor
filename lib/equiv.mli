open Core
open Ast

module Eq : sig
  type t = Name.t * Name.t [@@deriving compare, sexp]

  include Comparator.S with type t := t
end

type t = Set.M(Eq).t [@@deriving compare, sexp]
type meta = < eq : t >

val pp_meta : < meta ; .. > Fmt.t
val annotate : 'a annot -> < meta ; meta : 'a > annot

val eqs_query_open :
  schema:('r -> Schema.t) -> ('r -> t) -> ('r pred, 'r) query -> t

val eqs_pred : _ pred -> t
val eqs : _ annot -> t

(** Two attributes are equivalent in a context if they can be substituted
   without changing the final relation. *)
module Context : sig
  type meta = < eqs : t >

  val annotate : 'a annot -> < meta ; meta : 'a > annot
end
