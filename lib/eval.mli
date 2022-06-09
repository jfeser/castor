module Tuple : sig
  type t = (Name.t * Value.t) list [@@deriving compare, sexp]
end

val eval :
  (Relation.t -> Tuple.t list) ->
  (Name.t * Value.t) list ->
  < .. > Ast.annot ->
  Tuple.t list
