open Core
open Llvm

module TypeDesc : sig
  type t =
    | Root of string option
    | Scalar of { name : string; parent : t }
    | Struct of { name : string; fields : (t * int) list }
  [@@deriving compare, sexp]

  include Comparator.S with type t := t
end

val to_meta : llcontext -> TypeDesc.t list -> llvalue Map.M(TypeDesc).t

val tag :
  ?offset:int ->
  ?constant:bool ->
  ?access:TypeDesc.t ->
  llcontext ->
  llvalue Map.M(TypeDesc).t ->
  TypeDesc.t ->
  llvalue

val kind : llcontext -> llmdkind
