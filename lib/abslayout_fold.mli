(* open Core *)
(* open Ast *)

(* val src : Logs.Src.t *)

(* module Data : sig *)
(*   type t *)

(*   val of_ralgebra : ?dir:string -> Db.t -> < Equiv.meta ; .. > annot -> t *)

(*   val annotate : *)
(*     ?dir:string -> *)
(*     Db.t -> *)
(*     (< Equiv.meta ; .. > as 'm) annot -> *)
(*     < Equiv.meta ; fold_stream : t ; set_fold_stream : t -> unit ; meta : 'm > *)
(*     annot *)
(* end *)

(* module Fold : sig *)
(*   type ('a, 'b, 'c) fold = { *)
(*     init : 'b; *)
(*     fold : 'b -> 'a -> 'b; *)
(*     extract : 'b -> 'c; *)
(*   } *)

(*   type ('a, 'c) t = Fold : ('a, 'b, 'c) fold -> ('a, 'c) t *)

(*   val run : ('a, 'b) t -> 'a list -> 'b *)
(*   val run_seq : ('a, 'b) t -> 'a Sequence.t -> 'b *)
(* end *)

(* class virtual ['a, 'm] abslayout_fold : *)
(*   object *)
(*     constraint 'm = < Equiv.meta ; .. > *)
(*     (\** metadata *\) *)

(*     constraint 'r = 'm annot *)
(*     (\** relations *\) *)

(*     constraint 'p = 'r pred *)
(*     (\** predicates *\) *)

(*     method virtual hash_idx : *)
(*       'm -> ('p, 'r) hash_idx -> (Value.t list * 'a * 'a, 'a) Fold.t *)

(*     method virtual ordered_idx : *)
(*       'm -> ('p, 'r) ordered_idx -> (Value.t list * 'a * 'a, 'a) Fold.t *)

(*     method virtual list : 'm -> 'r list_ -> (Value.t list * 'a, 'a) Fold.t *)
(*     method virtual scalar : 'm -> 'p scalar -> Value.t -> 'a *)
(*     method virtual tuple : 'm -> 'r list * tuple -> ('a, 'a) Fold.t *)
(*     method virtual empty : 'm -> 'a *)
(*     method virtual join : 'm -> ('p, 'r) join -> 'a -> 'a -> 'a *)
(*     method virtual depjoin : 'm -> 'r depjoin -> 'a -> 'a -> 'a *)
(*     method dedup : 'm -> 'a -> 'a *)
(*     method select : 'm -> 'p Select_list.t * 'r -> 'a -> 'a *)
(*     method filter : 'm -> 'p * 'r -> 'a -> 'a *)
(*     method group_by : 'm -> 'p Select_list.t * Name.t list * 'r -> 'a -> 'a *)
(*     method order_by : 'm -> ('p, 'r) order_by -> 'a -> 'a *)
(*     method run : Data.t -> 'r -> 'a *)
(*   end *)

(* class ['m] print_fold : *)
(*   object *)
(*     constraint 'm = < Equiv.meta ; .. > *)
(*     (\** metadata *\) *)

(*     constraint 'r = 'm annot *)
(*     (\** relations *\) *)

(*     constraint 'p = 'r pred *)
(*     (\** predicates *\) *)

(*     constraint 's = string list *)
(*     (\** results *\) *)

(*     method hash_idx : *)
(*       'm -> ('p, 'r) hash_idx -> (Value.t list * 's * 's, 's) Fold.t *)

(*     method ordered_idx : *)
(*       'm -> ('p, 'r) ordered_idx -> (Value.t list * 's * 's, 's) Fold.t *)

(*     method list : 'm -> 'r list_ -> (Value.t list * 's, 's) Fold.t *)
(*     method collection : string -> (Value.t list * 's * 's, 's) Fold.t *)
(*     method scalar : 'm -> 'p scalar -> Value.t -> 's *)
(*     method tuple : 'm -> 'r list * tuple -> ('s, 's) Fold.t *)
(*     method empty : 'm -> 's *)
(*     method join : 'm -> ('p, 'r) join -> 's -> 's -> 's *)
(*     method depjoin : 'm -> 'r depjoin -> 's -> 's -> 's *)
(*     method dedup : 'm -> 's -> 's *)
(*     method select : 'm -> 'p Select_list.t * 'r -> 's -> 's *)
(*     method filter : 'm -> 'p * 'r -> 's -> 's *)
(*     method group_by : 'm -> 'p Select_list.t * Name.t list * 'r -> 's -> 's *)
(*     method order_by : 'm -> ('p, 'r) order_by -> 's -> 's *)
(*     method run : Data.t -> 'r -> 's *)
(*   end *)
