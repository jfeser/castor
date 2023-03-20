exception Error of [ `Empty | `Hash_new_failed of string | `Parameter_range | `Freed ]
[@@deriving sexp]

module Util : sig
  val with_output : (unit -> 'a) -> ('a, exn) result * string
end

module KeySet : sig
  type t

  val create : string list -> t
  (** Create a KeySet.t from a list of string keys *)
end

module Config : sig
  type chd_config = { keys_per_bucket : int; keys_per_bin : int }

  type algo =
    [ `Bmz
    | `Bmz8
    | `Chm
    | `Bdz
    | `Bdz_ph
    | `Chd_ph of chd_config
    | `Chd of chd_config ]

  type 'a args = ?verbose:bool -> ?algo:algo -> ?seed:int -> KeySet.t -> 'a

  type t

  val default_chd : algo

  val default_chd_ph : algo

  val string_of_algo : algo -> string

  val create : t args
  (** Create a hash configuration from a KeySet.t *)

  val with_config : ((t -> 'a) -> 'a) args
  (** Create a hash configuration from a KeySet.t, destroying it after use *)

  val destroy : t -> unit
  (** Destroy a Config.t

      This function is idempotent.
  *)
end

module Hash : sig
  type t

  val of_config : Config.t -> t

  val of_packed : string -> t

  val with_hash : Config.t -> (t -> 'a) -> 'a

  val to_packed : t -> string

  val hash : t -> string -> int

  val destroy : t -> unit
  (** Destroy a Hash.t

      This function is idempotent.
  *)
end
