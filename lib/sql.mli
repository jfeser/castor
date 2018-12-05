type t = {schema: Name.t list; sql: [`Subquery of string | `Scan of string]}

val to_subquery : ?alias:string -> t -> string * Name.t list

val to_query : t -> string

val of_ralgebra : Abslayout.t -> t

val pred_to_sql : Abslayout.pred -> string

val ralgebra_to_sql : Abslayout.t -> string
