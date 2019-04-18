open Base
open Castor
open Abslayout

module Config = struct
  module type S = sig
    include Abslayout_db.Config.S

    include Ops.Config.S
  end
end

module Make (Config : Config.S) = struct
  open Config
  module M = Abslayout_db.Make (Config)
  module Ops = Ops.Make (Config)
  open Ops

  let no_params r = Set.is_empty (Set.inter (names r) params)

  let row_store r =
    if no_params r then
      let s = schema_exn r in
      let scalars = List.map s ~f:(fun n -> scalar (Name n)) in
      Some (list r (tuple scalars Cross))
    else None

  let row_store = of_func row_store ~name:"to-row-store"
end
