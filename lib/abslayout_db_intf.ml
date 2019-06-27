open! Core
open Abslayout

module type S = sig
  val type_of : ?timeout:float -> t -> Type.t

  val annotate_type : t -> unit

  val load_layout : ?params:Set.M(Name).t -> t -> t

  val load_string : ?params:Set.M(Name).t -> string -> t

  module Fold : sig
    type ('a, 'b, 'c) fold = {init: 'b; fold: 'b -> 'a -> 'b; extract: 'b -> 'c}

    type ('a, 'c) t = Fold : ('a, 'b, 'c) fold -> ('a, 'c) t

    val run : ('a, 'c) t -> 'a list -> 'c

    val run_gen : ('a, 'c) t -> 'a Gen.t -> 'c
  end

  class virtual ['a] abslayout_fold :
    object
      method virtual empty : Meta.t -> 'a

      method virtual scalar : Meta.t -> pred -> Value.t -> 'a

      method virtual join : Meta.t -> join -> 'a -> 'a -> 'a

      method virtual depjoin : Meta.t -> depjoin -> 'a -> 'a -> 'a

      method virtual tuple : Meta.t -> t list * tuple -> ('a, 'a) Fold.t

      method virtual list : Meta.t -> t * t -> (Value.t list * 'a, 'a) Fold.t

      method virtual hash_idx :
        Meta.t -> hash_idx -> (Value.t list * 'a * 'a, 'a) Fold.t

      method virtual ordered_idx :
        Meta.t -> t * t * ordered_idx -> (Value.t list * 'a * 'a, 'a) Fold.t

      method dedup : Meta.t -> 'a -> 'a

      method filter : Meta.t -> pred * t -> 'a -> 'a

      method group_by : Meta.t -> pred list * Name.t list * t -> 'a -> 'a

      method order_by : Meta.t -> order_by -> 'a -> 'a

      method select : Meta.t -> pred list * t -> 'a -> 'a

      method run : ?timeout:float -> t -> 'a
    end

  val annotate_relations : t -> t
end
