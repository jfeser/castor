open Base

(* Implementations of combinatorics routines. *)

val m_partition : int -> int -> int Array.t Sequence.t
(** Compute the partitions of an integer {i n} into {i m} parts. See
    (Knuth 3b, pg. 2). *)

val m_partition_with_zeros : int -> int -> int Array.t Sequence.t
(** Compute the partitions of an integer {i n} into {i m} parts,
    including partitions where some elements are zero. *)

val permutations : int Array.t -> int Array.t Sequence.t
(** Compute the unique permutations of an array. See (Knuth 2b, pg. 1). *)

val permutations_poly : 'a Array.t -> 'a Array.t Sequence.t

val combinations_with_replacement : int -> 'a list -> 'a list list
