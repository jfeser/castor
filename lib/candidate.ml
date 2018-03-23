open Base
open Printf
open Bin_prot.Std

module T = struct
  type t = {
    ralgebra : Ralgebra.t;
    transforms : (string * int) list;
  }

  let transforms_to_string : _ -> string = fun tfs ->
    List.map tfs ~f:(fun (tf, i) -> sprintf "%s:%d" tf i)
    |> String.concat ~sep:","

  module Binable = struct
    module Pervasives = Caml.Pervasives

    type cand = t
    type t = {
      ralgebra : Ralgebra.Binable.t;
      transforms : (string * int) list;
    } [@@deriving bin_io]

    let of_candidate : cand -> t = fun { ralgebra; transforms } ->
      { ralgebra = Ralgebra.Binable.of_ralgebra ralgebra; transforms }

    let to_candidate : t -> cand = fun { ralgebra; transforms } ->
      { ralgebra = Ralgebra.Binable.to_ralgebra ralgebra; transforms }
  end
end
include T

module Config = struct
  module type S = Transform.Config.S
end

module Make (Config : Config.S) = struct
  module Transform = Transform.Make(Config)

  include T

  let run : Transform.t -> t -> t list =
    fun tf { ralgebra = r; transforms = tfs } ->
      Transform.run tf r
      |> List.mapi ~f:(fun i r' -> { ralgebra = r'; transforms = tfs @ [tf.name, i]})
end
