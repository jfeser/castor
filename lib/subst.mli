open Core
open Ast

val incr : 'm annot -> 'm annot
val incr_pred : 'm annot pred -> 'm annot pred
val incr_ctx : 'm annot pred Map.M(Name).t -> 'm annot pred Map.M(Name).t
val decr_pred : 'a pred -> 'a pred option
val decr_pred_exn : 'a pred -> 'a pred
val subst : 'm annot pred Map.M(Name).t -> 'm annot -> 'm annot
