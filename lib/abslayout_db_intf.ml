open Base
open Abslayout

module type S = sig
  val to_type : t -> Type.t

  val annotate_schema : t -> unit

  val to_schema : t -> Name.t list

  val annotate_key_layouts : t -> t

  val annotate_subquery_types : t -> unit

  val resolve : ?params:Set.M(Name.Compare_no_type).t -> t -> t

  type eval_ctx =
    [ `Concat of eval_ctx Gen.t
    | `Empty
    | `For of (Value.t list * eval_ctx) Gen.t
    | `Scalar of Value.t
    | `Query of Value.t list Gen.t ]

  val to_ctx : Value.t list -> eval_ctx

  class virtual ['ctx, 'a] material_fold :
    object
      method virtual build_AList :
        'ctx -> Meta.t -> t * t -> (Value.t list * eval_ctx) Gen.t -> 'a

      method virtual build_ATuple :
        'ctx -> Meta.t -> t list * tuple -> eval_ctx Gen.t -> 'a

      method virtual build_AHashIdx :
           'ctx
        -> Meta.t
        -> t * t * hash_idx
        -> Value.t list Gen.t
        -> (Value.t list * eval_ctx) Gen.t
        -> 'a

      method virtual build_AOrderedIdx :
           'ctx
        -> Meta.t
        -> t * t * ordered_idx
        -> Value.t list Gen.t
        -> (Value.t list * eval_ctx) Gen.t
        -> 'a

      method virtual build_AEmpty : 'ctx -> Meta.t -> 'a

      method virtual build_AScalar : 'ctx -> Meta.t -> pred -> Value.t -> 'a

      method virtual build_Select :
        'ctx -> Meta.t -> pred list * t -> eval_ctx -> 'a

      method virtual build_Filter : 'ctx -> Meta.t -> pred * t -> eval_ctx -> 'a

      method visit_t : 'ctx -> eval_ctx -> t -> 'a

      method run : 'ctx -> t -> 'a
    end
end
