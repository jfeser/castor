open Ast

val list : ('a, 'b, 'c) list_ -> ('b, 'c) depjoin

val hash_idx :
  (< .. > annot pred, < .. > annot, string) hash_idx -> (t, string) depjoin

val ordered_idx :
  (< .. > annot pred, < .. > annot, 'a) ordered_idx -> (t, 'a) depjoin

val cross_tuple : meta annot list -> (meta annot pred, meta annot, 'a) query
val annot : meta annot -> meta annot

val query :
  (meta annot pred, meta annot, string) query ->
  (meta annot pred, meta annot, string) query

val pred : meta annot pred -> meta annot pred
