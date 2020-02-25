open Ast

exception TypeError of Core.Error.t

(** Range abstraction for integers. *)
module AbsInt : sig
  type t = Bottom | Interval of int * int | Top [@@deriving compare, sexp]

  include Container.Summable with type t := t

  val pp : Format.formatter -> t -> unit

  val zero : t

  val byte_width : nullable:bool -> t -> int

  val of_int : int -> t

  val to_int : t -> int option

  val inf : t -> (int, Error.t) result

  val sup : t -> (int, Error.t) result

  val meet : t -> t -> t

  val join : t -> t -> t

  val ( && ) : t -> t -> t

  val ( || ) : t -> t -> t

  val ( + ) : t -> t -> t

  val ( - ) : t -> t -> t

  val ( * ) : t -> t -> t

  module O : sig
    val ( && ) : t -> t -> t

    val ( || ) : t -> t -> t

    val ( + ) : t -> t -> t

    val ( - ) : t -> t -> t

    val ( * ) : t -> t -> t
  end
end

module AbsFixed : sig
  type t = { range : AbsInt.t; scale : int } [@@deriving compare, sexp]

  val of_fixed : Fixed_point.t -> t

  val zero : t

  val bot : t

  val top : t

  val unify : (AbsInt.t -> AbsInt.t -> AbsInt.t) -> t -> t -> t

  val meet : t -> t -> t

  val join : t -> t -> t
end

type int_ = { range : AbsInt.t; nullable : bool }

type date = int_

type bool_ = { nullable : bool }

type string_ = { nchars : AbsInt.t; nullable : bool }

type list_ = { count : AbsInt.t }

type tuple = { kind : [ `Concat | `Cross ] }

type hash_idx = { key_count : AbsInt.t }

type ordered_idx = { key_count : AbsInt.t }

type fixed = { value : AbsFixed.t; nullable : bool }

type t =
  | NullT
  | IntT of int_
  | DateT of date
  | FixedT of fixed
  | BoolT of bool_
  | StringT of string_
  | TupleT of (t list * tuple)
  | ListT of (t * list_)
  | HashIdxT of (t * t * hash_idx)
  | OrderedIdxT of (t * t * ordered_idx)
  | FuncT of (t list * [ `Child_sum | `Width of int ])
  | EmptyT
[@@deriving compare, sexp]

val unify_exn : t -> t -> t

val count : t -> AbsInt.t

val len : t -> AbsInt.t

val width : t -> int
(** Returns the width of the tuples produced by reading a layout with this type.
   *)

val hash_kind_exn : t -> [ `Cmph | `Universal | `Direct ]
(** Use the type of a hash index to decide what hash method to use. *)

val hi_hash_len : ?bytes_per_key:AbsInt.t -> t -> hash_idx -> AbsInt.t
(** Range of hash index hash data lengths. *)

val hi_map_len : t -> t -> hash_idx -> AbsInt.t
(** Range of hash index map lengths. *)

val hi_ptr_size : t -> t -> hash_idx -> int
(** Size of pointers (in bytes) in hash indexes. *)

val oi_map_len : t -> t -> ordered_idx -> AbsInt.t
(** Range of ordered index map lengths. *)

val oi_ptr_size : t -> ordered_idx -> int
(** Size of pointers (in bytes) in ordered indexes. *)

val least_general_of_primtype : Prim_type.t -> t

val type_of : ?timeout:float -> Db.t -> 'a annot -> t

val annotate : Db.t -> 'a annot -> < type_ : t > annot

module Parallel : sig
  val type_of : Db.t -> unit annot -> t Lwt.t
end
