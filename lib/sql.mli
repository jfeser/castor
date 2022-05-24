open Ast

type select_entry = { pred : Pred.t; alias : string; cast : Prim_type.t option }
[@@deriving compare, sexp_of]

type 'r spj = {
  select : select_entry list;
  distinct : bool;
  conds : Pred.t list;
  relations : 'r list;
  order : (Pred.t * order) list;
  group : Pred.t list;
  limit : int option;
}
[@@deriving compare, sexp_of]

type 'q compound_relation =
  [ `Subquery of 'q | `Table of Relation.t | `Series of Pred.t * Pred.t ]
  * string
  * [ `Left | `Lateral ]
[@@deriving compare, sexp_of]

type t =
  [ `Query of t compound_relation spj
  | `Union_all of t compound_relation spj list ]
[@@deriving compare, sexp_of]

val create_spj :
  ?distinct:bool ->
  ?conds:Pred.t list ->
  ?relations:'a list ->
  ?order:(Pred.t * order) list ->
  ?group:Pred.t list ->
  ?limit:int ->
  select_entry list ->
  'a spj

val create_entry : ?alias:string -> ?cast:Prim_type.t -> Pred.t -> select_entry

val create_entry_s :
  ?alias:string -> ?cast:Prim_type.t -> string -> select_entry

val create_entries_s : string list -> select_entry list
val src : Logs.Src.t
val of_ralgebra : < .. > annot -> t
val has_aggregates : t -> bool
val to_schema : t -> string list
val sample : int -> string -> string
val trash_sample : int -> string -> string
val to_string : t -> string
val format : string -> string

val to_string_hum : t -> string
(** Pretty print a SQL string if a sql formatter is available. *)
