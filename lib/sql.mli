open Abslayout

type spj =
  { select: (pred * string * Type.PrimType.t option) list
  ; distinct: bool
  ; conds: pred list
  ; relations:
      ([`Subquery of t * string | `Table of string] * [`Left | `Lateral]) list
  ; order: (pred * [`Asc | `Desc]) list
  ; group: pred list
  ; limit: int option }

and t = Query of spj | Union_all of spj list [@@deriving compare, sexp_of]

val of_ralgebra : Abslayout.t -> t

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

val to_spj : t -> spj

val to_schema : t -> string list

val join : Name.t list -> Name.t list -> t -> t -> pred -> t

val order_by : Name.t list -> t -> pred list -> [`Asc | `Desc] -> t

val select : Name.t list -> t -> pred list -> t

val pred_to_sql : pred -> string

val to_sql : t -> string

val to_subquery : t -> string * Name.t list

val add_pred_alias : pred -> pred * string * _ option
