open Ast

type t = Name.t list [@@deriving compare, equal, sexp]

val pp : t Fmt.t

val to_type_open :
  ('a -> t) -> ('a pred -> Prim_type.t) -> 'a pred -> Prim_type.t

val schema_query_open : ('r -> t) -> ('r pred, 'r) query -> t
val schema_open : ('a annot -> t) -> 'a annot -> t
val schema : _ annot -> t
val annotate_schema : 'm annot -> < schema : t ; meta : 'm > annot
val names : _ annot -> string list
val to_type : _ annot pred -> Prim_type.t
val to_type_opt : _ annot pred -> Prim_type.t Core.Or_error.t
val to_select_list : t -> _ pred Select_list.t
val zero : t -> t
