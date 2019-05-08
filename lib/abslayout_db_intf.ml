open Core
open Abslayout

module type S = sig
  val type_of : t -> Type.t

  val annotate_type : t -> unit

  val resolve : ?params:Set.M(Name).t -> t -> t

  val load_string : ?params:Set.M(Name).t -> string -> t

  val refcnt : int Map.M(Name).t Univ_map.Key.t

  type eval_ctx

  val to_ctx : Value.t list -> eval_ctx

  class virtual ['ctx, 'a] unsafe_material_fold :
    object
      method virtual private build_AList :
        'ctx -> Meta.t -> t * t -> (Value.t list * eval_ctx) Gen.t -> 'a

      method virtual private build_ATuple :
        'ctx -> Meta.t -> t list * tuple -> eval_ctx list -> 'a

      method virtual private build_AHashIdx :
           'ctx
        -> Meta.t
        -> t * t * hash_idx
        -> Value.t list Gen.t
        -> (Value.t list * eval_ctx) Gen.t
        -> 'a

      method virtual private build_AOrderedIdx :
           'ctx
        -> Meta.t
        -> t * t * ordered_idx
        -> Value.t list Gen.t
        -> (Value.t list * eval_ctx) Gen.t
        -> 'a

      method virtual private build_AEmpty : 'ctx -> Meta.t -> 'a

      method virtual private build_AScalar :
        'ctx -> Meta.t -> pred -> Value.t Lazy.t -> 'a

      method virtual private build_DepJoin :
        'ctx -> Meta.t -> depjoin -> eval_ctx * eval_ctx -> 'a

      method virtual private build_Select :
        'ctx -> Meta.t -> pred list * t -> eval_ctx -> 'a

      method virtual private build_Filter :
        'ctx -> Meta.t -> pred * t -> eval_ctx -> 'a

      method private visit_t : 'ctx -> eval_ctx -> t -> 'a

      method run : 'ctx -> t -> 'a
    end

  type ('i, 'a, 'v, 'o) fold = {pre: 'i -> 'a; body: 'a -> 'v -> 'a; post: 'a -> 'o}

  class virtual ['out, 'l, 'h1, 'h2, 'h3, 'o1, 'o2, 'o3] material_fold :
    object
      method virtual list :
        Meta.t -> t * t -> (unit, 'l, Value.t list * 'out, 'out) fold

      method virtual hash_idx :
           Meta.t
        -> t * t * hash_idx
        -> (unit, 'h1, Value.t list, 'h2) fold * ('h2, 'h3, 'out * 'out, 'out) fold

      method virtual ordered_idx :
           Meta.t
        -> t * t * ordered_idx
        -> (unit, 'o1, Value.t list, 'o2) fold * ('o2, 'o3, 'out * 'out, 'out) fold

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
