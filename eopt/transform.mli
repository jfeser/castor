(* open Core *)
(* open Castor.Ast *)

(* module Config : sig *)
(*   module type S = sig *)
(*     val conn : Castor.Db.t *)
(*     val cost_conn : Castor.Db.t *)
(*     val params : Set.M(Castor.Name).t *)
(*     val cost_timeout : float option *)
(*     val random : Castor.Mcmc.Random_choice.t *)
(*   end *)
(* end *)

(* module Make (Config : Config.S) : sig *)
(*   val is_serializable : 'a annot -> unit Or_error.t *)
(* end *)

(* val optimize : *)
(*   (module Config.S) -> Castor.Ast.t -> (Castor.Ast.t, Castor.Ast.t) Either.t *)
