open Ast

type t = Name.t list [@@deriving compare, equal, sexp]

type opt_t = (string option * Prim_type.t option) list
[@@deriving compare, sexp]

val pp : t Fmt.t
val scoped : string -> t -> t
val unscoped : t -> t

val to_type_open :
  ('a -> t) -> ('a pred -> Prim_type.t) -> 'a pred -> Prim_type.t

val schema_open_opt : ('a annot -> opt_t) -> 'a annot -> opt_t
val schema_opt : _ annot -> opt_t
val schema_open : ('a annot -> t) -> 'a annot -> t
val schema_open_full : ('a annot -> t) -> 'a annot -> t
val schema : _ annot -> t
val schema_full : _ annot -> t
val names : _ annot -> string list
val to_type : _ annot pred -> Prim_type.t
val to_type_opt : _ annot pred -> Prim_type.t Core.Or_error.t
val to_select_list : t -> _ pred Select_list.t
