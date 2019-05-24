open! Core
open Castor

module Config = struct
  module type S = sig
    val params : Set.M(Name).t

    include Abslayout_db.Config.S
  end
end

module Make (Config : Config.S) = struct
  open Config
  module M = Abslayout_db.Make (Config)

  let len r =
    Logs.debug (fun m -> m "Computing cost of %a." Abslayout.pp r) ;
    let cost =
      match M.load_layout ~params r |> M.type_of |> Type.len with
      | Bottom -> Float.max_value
      | Interval (_, h) -> Float.of_int h
      | Top -> Float.max_value
    in
    Logs.debug (fun m -> m "Found cost %f" cost) ;
    cost
end
