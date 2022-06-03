(* open Core *)
(* open Ast *)
(* open Implang *)

(* type ir_module = { *)
(*   funcs : func list; *)
(*   params : (string * Prim_type.t) list; *)
(*   buffer_len : int; *)
(* } *)
(* [@@deriving compare, equal, sexp] *)

(* exception IRGenError of Error.t *)

(* val irgen : *)
(*   ?debug:bool -> *)
(*   params:(string * Prim_type.t) list -> *)
(*   len:int -> *)
(*   < resolved : Resolve.resolved ; type_ : Type.t ; pos : int option > annot -> *)
(*   ir_module *)

(* val pp : Formatter.t -> ir_module -> unit *)
