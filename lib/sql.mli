open Base
open Collections
open Abslayout

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

val of_ralgebra : ?fresh:Fresh.t -> Abslayout.t -> t

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

val to_spj : ?fresh:Fresh.t -> t -> spj

val to_schema : t -> string list

val to_order : t -> order Or_error.t

val join : ?fresh:Fresh.t -> Name.t list -> Name.t list -> t -> t -> pred -> t

val order_by : Name.t list -> t -> pred list -> [`Asc | `Desc] -> t

val select : ?fresh:Fresh.t -> Name.t list -> t -> pred list -> t

val pred_to_sql : pred -> string

val to_sql : t -> string

val add_pred_alias : ?fresh:Fresh.t -> pred -> pred * string * _ option
