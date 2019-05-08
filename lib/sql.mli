open! Core
open Abslayout

type ctx

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

val create_ctx : unit -> ctx

val of_ralgebra : ctx -> Abslayout.t -> t

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

val create_entry :
  ctx:ctx -> ?alias:string -> ?cast:Type.PrimType.t -> pred -> select_entry

val to_spj : ctx -> t -> spj

val to_schema : t -> string list

val to_order : t -> (pred * order) list Or_error.t

val join : ctx -> Name.t list -> Name.t list -> t -> t -> pred -> t

val order_by : ctx -> Name.t list -> t -> (pred * order) list -> t

val select : ?groupby:pred list -> ctx -> Name.t list -> t -> pred list -> t

val pred_to_sql : ctx -> pred -> string

val to_string : ctx -> t -> string

val to_string_hum : ctx -> t -> string
(** Pretty print a SQL string if a sql formatter is available. *)
