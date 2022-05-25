open Core

module Id : sig
  type t [@@deriving compare, equal, hash, sexp_of]
end

module type LANG = sig
  type 'a t [@@deriving compare, hash, sexp_of]

  val match_func : 'a t -> 'b t -> bool
  val args : 'a t -> 'a list
  val map_args : ('a -> 'b) -> 'a t -> 'b t
end

module type ANALYSIS = sig
  type 'a lang
  type t [@@deriving equal, sexp_of]

  val of_enode : 'a lang -> t
  val merge : t -> t -> t
end

module type EGRAPH = sig
  type 'a lang

  module ENode : sig
    type t = Id.t lang [@@deriving compare, hash, sexp_of]
  end

  module EClass : sig
    type t
  end

  type t [@@deriving sexp_of]

  val create : unit -> t
  val eclass_id_equiv : Id.t -> Id.t -> bool
  val enode_equiv : t -> ENode.t -> ENode.t -> bool
  val add : t -> Id.t lang -> Id.t
  val merge : t -> Id.t -> Id.t -> Id.t
  val rebuild : t -> unit
  val classes : t -> Id.t Iter.t

  type pat = [ `Apply of pat lang | `Var of int ]

  val search : t -> pat -> (Id.t * Id.t Map.M(Int).t list) list
  val rewrite : t -> pat -> pat -> unit
end

module Make (L : LANG) (A : ANALYSIS with type 'a lang = 'a L.t) :
  EGRAPH with type 'a lang := 'a L.t

module SymbolLang (S : sig
  type t [@@deriving compare, equal, hash, sexp_of]
end) : sig
  type 'a t = { func : S.t; args : 'a list }

  include LANG with type 'a t := 'a t
end

module AstLang : sig
  type 'a t = Query of ('a, 'a) Ast.query | Pred of ('a, 'a) Ast.ppred

  include LANG with type 'a t := 'a t
end

module UnitAnalysis : ANALYSIS with type t = unit and type 'a lang := 'a

module AstEGraph : sig
  open Ast
  include EGRAPH with type 'a lang := 'a AstLang.t

  val add_query : t -> ('a annot pred, 'a annot) query -> Id.t
  val add_pred : t -> 'a annot pred -> Id.t
  val add_annot : t -> 'a annot -> Id.t
  val choose_exn : t -> Id.t -> [ `Annot of < > annot | `Pred of < > annot pred ]
end
