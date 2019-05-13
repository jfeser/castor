open! Core
open Abslayout

type select_entry = {pred: pred; alias: string; cast: Type.PrimType.t option}
[@@deriving compare, sexp_of]

type spj =
  { select: select_entry list
  ; distinct: bool
  ; conds: pred list
  ; relations:
      ([`Subquery of t * string | `Table of string * string] * [`Left | `Lateral])
      list
  ; order: (pred * order) list
  ; group: pred list
  ; limit: int option }

and t = Query of spj | Union_all of spj list [@@deriving compare, sexp_of]

val of_ralgebra : Abslayout.t -> t

val create_query :
     ?distinct:bool
  -> ?conds:pred list
  -> ?relations:( [`Subquery of t * string | `Table of string * string]
                * [`Lateral | `Left] )
                list
  -> ?order:(pred * order) list
  -> ?group:pred list
  -> ?limit:int
  -> select_entry list
  -> spj

val create_entry : ?alias:string -> ?cast:Type.PrimType.t -> pred -> select_entry

val to_spj : t -> spj

val to_schema : t -> string list

val to_order : t -> (pred * order) list Or_error.t

val join : Name.t list -> Name.t list -> t -> t -> pred -> t

val order_by : (Abslayout.t -> t) -> (pred * order) list -> Abslayout.t -> t

val select : ?groupby:pred list -> Name.t list -> t -> pred list -> t

val pred_to_sql : pred -> string

val to_string : t -> string

val to_string_hum : t -> string
(** Pretty print a SQL string if a sql formatter is available. *)
