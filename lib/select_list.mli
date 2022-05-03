type 'p t = ('p * string) list [@@deriving compare, hash, sexp]

val of_list : ('p * string) list -> 'p t Core.Or_error.t
val of_list_exn : ('p * string) list -> 'p t
val to_list : 'p t -> ('p * string) list
val map : 'a t -> f:('a -> string -> 'b) -> 'b t
val fold_map : 'a t -> f:('c -> 'a -> string -> 'c * 'b) -> init:'c -> 'c * 'b t
val filter : 'p t -> f:('p -> string -> bool) -> 'p t
val preds : 'p t -> 'p Iter.t
val names : _ t -> string Iter.t
