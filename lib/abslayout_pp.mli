open Ast

val pp_query_open : 'r Fmt.t -> 'p Fmt.t -> ('p, 'r) query Fmt.t
val pp_pred_open : 'r Fmt.t -> 'p Fmt.t -> ('p, 'r) ppred Fmt.t
val pp_pred_with_meta : 'a Fmt.t -> 'a annot pred Fmt.t
val pp_with_meta : 'a Fmt.t -> 'a annot Fmt.t
val pp_pred : _ annot pred Fmt.t
val pp : _ annot Fmt.t
