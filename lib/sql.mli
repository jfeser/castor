open! Core

type select_entry = {
  pred : Pred.t;
  alias : string;
  cast : Type.PrimType.t option;
}
[@@deriving compare, sexp_of]

type spj = {
  select : select_entry list;
  distinct : bool;
  conds : Pred.t list;
  relations :
    ( [ `Subquery of t * string
      | `Table of Relation.t * string
      | `Series of Pred.t * Pred.t * string ]
    * [ `Left | `Lateral ] )
    list;
  order : (Pred.t * Abslayout.order) list;
  group : Pred.t list;
  limit : int option;
}

and t = Query of spj | Union_all of spj list [@@deriving compare, sexp_of]

val src : Logs.Src.t

val of_ralgebra : Abslayout.t -> t

val has_aggregates : t -> bool

val to_schema : t -> string list

val to_string : t -> string

val to_string_hum : t -> string
(** Pretty print a SQL string if a sql formatter is available. *)
