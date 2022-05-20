open Ast

val list : 'r list_ -> 'r depjoin
val hash_idx : (< .. > annot pred, < .. > annot) hash_idx -> t depjoin
val ordered_idx : (< .. > annot pred, < .. > annot) ordered_idx -> t depjoin
val cross_tuple : meta annot list -> (meta annot pred, meta annot) query
val annot : meta annot -> meta annot

val query :
  (meta annot pred, meta annot) query -> (meta annot pred, meta annot) query

val pred : meta annot pred -> meta annot pred
