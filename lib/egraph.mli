open Core

exception Merge_error of Sexp.t

module Id : sig
  type t [@@deriving compare, equal, hash, sexp_of]
end

module type LANG = sig
  type 'a t [@@deriving compare, hash, sexp_of]

  val pp : 'a Fmt.t -> 'a t Fmt.t
  val match_func : 'a t -> 'b t -> bool
  val map_args : ('a -> 'b) -> 'a t -> 'b t
end

(** [ANALYSIS] is a module type for analyses on egraphs. *)
module type ANALYSIS = sig
  type 'a lang
  (** ['a lang] is the type of enode data. *)

  type t [@@deriving equal, sexp_of]

  val of_enode : ('a -> t) -> 'a lang -> t
  (** [of_enode f e] computes the analysis result for the enode [e] using [f] to
      analyze child eclasses. *)

  val merge : t -> t -> (t, Sexp.t) result
  (** [merge a1 a2] merges the analysis results [a1] and [a2]. If the results are
      incompatible, [Error msg] is returned, where [msg] is a description of the
      error. *)
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
  (** [create ()] creates a new empty egraph. *)

  val add : t -> Id.t lang -> Id.t
  (** [add g e] adds the enode [e] to [g]. *)

  val merge : t -> Id.t -> Id.t -> Id.t
  (** [merge g e1 e2] merges the eclasses of [e1] and [e2] in [g]. *)

  val rebuild : t -> unit
  val classes : t -> Id.t Iter.t
  val enodes : t -> Id.t -> ENode.t Iter.t
  val n_enodes : t -> int
  val n_classes : t -> int

  val pp_dot : t Fmt.t
  (** [pp_dot] is a pretty-printer for egraphs in the dot format. *)

  val pp : t Fmt.t
  (** [pp] is a pretty-printer for egraphs. *)

  type pat = [ `Apply of pat lang | `Var of int ]

  val search : t -> pat -> (Id.t * Id.t Map.M(Int).t list) list
  val rewrite : t -> pat -> pat -> unit
end

module Make (L : LANG) (_ : ANALYSIS with type 'a lang = 'a L.t) :
  EGRAPH with type 'a lang := 'a L.t

module SymbolLang (S : sig
  type t [@@deriving compare, equal, hash, sexp_of]

  val pp : t Fmt.t
end) : sig
  type 'a t = { func : S.t; args : 'a list }

  include LANG with type 'a t := 'a t
end

module AstLang : sig
  type 'a t = ('a Ast.pred, 'a) Ast.query

  include LANG with type 'a t := 'a t
end

module UnitAnalysis (L : LANG) :
  ANALYSIS with type t = unit and type 'a lang = 'a L.t

module AstEGraph : sig
  open Ast
  include EGRAPH with type 'a lang := 'a AstLang.t

  val add_query : t -> ('a annot pred, 'a annot) query -> Id.t
  val add_annot : t -> 'a annot -> Id.t
  val choose : t -> Id.t -> < > annot option
  val choose_exn : t -> Id.t -> < > annot
  val schema : t -> Id.t -> Schema.t
  val max_debruijn_index : t -> Id.t -> int
end
