open Base
open Castor
open Abslayout

module Config = struct
  module type S = sig
    val conn : Db.t

    val params : Set.M(Name).t

    val param_ctx : Value.t Map.M(Name).t

    val validate : bool
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
      let s = M.to_schema r in
      let scalars = List.map s ~f:(fun n -> scalar (Name n)) in
      Some (list r (tuple scalars Cross))
    else None

  let row_store = of_func row_store ~name:"to-row-store"
end
