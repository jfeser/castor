open Ast

type sql_pred = unit annot pred [@@deriving compare, sexp_of]

type select_entry = {
  pred : sql_pred;
  alias : string;
  cast : Prim_type.t option;
}
[@@deriving compare, sexp_of]

type spj = {
  select : select_entry list;
  distinct : bool;
  conds : sql_pred list;
  relations :
    ( [ `Subquery of t * string
      | `Table of Relation.t * string
      | `Series of sql_pred * sql_pred * string ]
    * [ `Left | `Lateral ] )
    list;
  order : (sql_pred * order) list;
  group : sql_pred list;
  limit : int option;
}

and t = Query of spj | Union_all of spj list [@@deriving compare, sexp_of]

val src : Logs.Src.t

val of_ralgebra : 'a annot -> t

val has_aggregates : t -> bool

val to_schema : t -> string list

val sample : int -> string -> string

val trash_sample : int -> string -> string

val to_string : t -> string

val to_string_hum : t -> string
(** Pretty print a SQL string if a sql formatter is available. *)
