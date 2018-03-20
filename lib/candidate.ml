open Base
open Bin_prot.Std

module Config = struct
  module type S = Transform.Config.S
end

module Binable = struct
  module Pervasives = Caml.Pervasives

  type t = {
    ralgebra : Ralgebra.Binable.t;
    transforms : (string * int) list;
  } [@@deriving bin_io]
end

module Make (Config : Config.S) = struct
  module Transform = Transform.Make(Config)

  type t = {
    ralgebra : Ralgebra.t;
    transforms : (Transform.t * int) list;
  }

  let run : Transform.t -> t -> t list =
    fun tf { ralgebra = r; transforms = tfs } ->
      Transform.run tf r
      |> List.mapi ~f:(fun i r' -> { ralgebra = r'; transforms = tfs @ [tf, i]})

  module Binable = struct
    type cand = t

    include Binable

    let of_candidate : cand -> t = fun { ralgebra; transforms } ->
      { ralgebra = Ralgebra.Binable.of_ralgebra ralgebra;
        transforms = List.map transforms ~f:(fun (tf, i) -> tf.name, i) }

    let to_candidate : t -> cand = fun { ralgebra; transforms } ->
      { ralgebra = Ralgebra.Binable.to_ralgebra ralgebra;
        transforms = List.map transforms ~f:(fun (tf, i) -> Transform.of_name_exn tf, i) }
  end
end
