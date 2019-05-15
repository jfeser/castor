open! Core
open Abslayout

module type S = sig
  module Ctx : sig
    type t
  end

  val type_of : t -> Type.t

  val annotate_type : t -> unit

  val load_string : ?params:Set.M(Name).t -> string -> t

  val to_ctx : Value.t list -> Ctx.t

  class virtual ['ctx, 'a] unsafe_material_fold :
    object
      method virtual private build_AList :
        'ctx -> Meta.t -> t * t -> (Value.t list * Ctx.t) Gen.t -> 'a

      method virtual private build_ATuple :
        'ctx -> Meta.t -> t list * tuple -> Ctx.t list -> 'a

      method virtual private build_AHashIdx :
        'ctx -> Meta.t -> hash_idx -> (Value.t list * Ctx.t) Gen.t -> 'a

      method virtual private build_AOrderedIdx :
        'ctx -> Meta.t -> t * t * ordered_idx -> (Value.t list * Ctx.t) Gen.t -> 'a

      method virtual private build_AEmpty : 'ctx -> Meta.t -> 'a

      method virtual private build_AScalar :
        'ctx -> Meta.t -> pred -> Value.t Lazy.t -> 'a

      method virtual private build_DepJoin :
        'ctx -> Meta.t -> depjoin -> Ctx.t * Ctx.t -> 'a

      method virtual private build_Select :
        'ctx -> Meta.t -> pred list * t -> Ctx.t -> 'a

      method virtual private build_Filter :
        'ctx -> Meta.t -> pred * t -> Ctx.t -> 'a

      method private visit_t : 'ctx -> Ctx.t -> t -> 'a

      method run : 'ctx -> t -> 'a
    end

  type ('i, 'a, 'v, 'o) fold = {pre: 'i -> 'a; body: 'a -> 'v -> 'a; post: 'a -> 'o}

  class virtual ['out, 'l, 'h, 'o] material_fold :
    object
      method virtual list :
        Meta.t -> t * t -> (unit, 'l, Value.t list * 'out, 'out) fold

      method virtual hash_idx :
        Meta.t -> hash_idx -> (unit, 'h, 'out * 'out, 'out) fold

      method virtual ordered_idx :
        Meta.t -> t * t * ordered_idx -> (unit, 'o, 'out * 'out, 'out) fold

      method virtual tuple : Meta.t -> t list * tuple -> 'out list -> 'out

      method virtual empty : Meta.t -> 'out

      method virtual scalar : Meta.t -> pred -> Value.t -> 'out

      method virtual depjoin : Meta.t -> depjoin -> 'out -> 'out -> 'out

      method virtual select : Meta.t -> pred list * t -> 'out -> 'out

      method virtual filter : Meta.t -> pred * t -> 'out -> 'out

      method run : unit -> t -> 'out
    end

  val annotate_relations : t -> t
end
