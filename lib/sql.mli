open Base
open Collections
open Abslayout

type ctx

type order = (pred * [`Asc | `Desc]) list

type spj =
  { select: (pred * string * Type.PrimType.t option) list
  ; distinct: bool
  ; conds: pred list
  ; relations:
      ([`Subquery of t * string | `Table of string] * [`Left | `Lateral]) list
  ; order: order
  ; group: pred list
  ; limit: int option }

and t = Query of spj | Union_all of spj list [@@deriving compare, sexp_of]

val create_ctx : ?fresh:Fresh.t -> unit -> ctx

val of_ralgebra : ctx -> Abslayout.t -> t

val create_query :
     ?distinct:bool
  -> ?conds:pred list
  -> ?relations:([`Subquery of t * string | `Table of string] * [`Lateral | `Left])
                list
  -> ?order:(pred * [`Asc | `Desc]) list
  -> ?group:pred list
  -> ?limit:int
  -> (pred * string * Type0.PrimType.t option) list
  -> spj

val to_spj : ctx -> t -> spj

val to_schema : t -> string list

val to_order : t -> order Or_error.t

val join : ctx -> Name.t list -> Name.t list -> t -> t -> pred -> t

val order_by : ctx -> Name.t list -> t -> pred list -> [`Asc | `Desc] -> t

val select : ctx -> Name.t list -> t -> pred list -> t

val pred_to_sql : ctx -> pred -> string

val to_string : ctx -> t -> string

val to_string_hum : ctx -> t -> string

val add_pred_alias : ctx -> pred -> pred * string * _ option
