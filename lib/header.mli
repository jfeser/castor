open! Core

module Field : sig
  type t = {
    name : string;
    size :
      [ `DescribedBy of string | `Empty of int | `Fixed of int | `Variable ];
    align : int;
  }
end

type t = Field.t list

val size : t -> string -> int Or_error.t

val make_access : t -> string -> Implang.expr -> Implang.expr

val make_position : t -> string -> Implang.expr -> Implang.expr

val make_header : Type.t -> t
