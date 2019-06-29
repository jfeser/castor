open! Core
open Castor
open Abslayout
open Collections

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
    if
      (* Relation has no free variables that are bound at runtime. *)
      Set.for_all (free r) ~f:(fun n ->
          match Name.Meta.(find n stage) with
          | Some `Compile -> true
          | Some `Run -> false
          | None ->
              Logs.warn (fun m -> m "Missing stage on %a." Name.pp n) ;
              false)
    then
      let scope = Fresh.name Global.fresh "s%d" in
      let scalars =
        Schema.scoped scope (schema_exn r)
        |> List.map ~f:(fun n -> scalar (Name n))
      in
      Some (list r scope (tuple scalars Cross))
    else None

  let row_store = of_func row_store ~name:"to-row-store"
end
