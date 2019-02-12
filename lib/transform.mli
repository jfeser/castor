open Base

module Config : sig
  module type S = sig
    val check_transforms : bool

    val params : Set.M(Name.Compare_no_type).t
  end
end

module Make (Config : Config.S) (M : Abslayout_db_intf.S) () : sig
  type t = private {name: string; f: Abslayout.t -> Abslayout.t list}
  [@@deriving sexp]

  val of_string_exn : string -> t list

  val run : t -> Abslayout.t -> Abslayout.t list

  val transforms : (string * (string list -> t)) list
end
