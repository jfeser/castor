open! Core
open Castor
open Abslayout
open Collections

module Config = struct
  module type S = sig
    include Ops.Config.S
  end
end

module Make (Config : Config.S) = struct
  open Config

  open Ops.Make (Config)

  let no_params r = Set.is_empty (Set.inter (names r) params)

  let row_store r =
    if
      (* Relation has no free variables that are bound at runtime. *)
      Set.for_all (free r) ~f:(fun n ->
          match Map.find r.meta#stage n with
          | Some `Compile -> true
          | Some `Run -> false
          | None ->
              Logs.warn (fun m -> m "Missing stage on %a." Name.pp n);
              false)
    then
      let scope = Fresh.name Global.fresh "s%d" in
      let scalars =
        Schema.schema r |> Schema.scoped scope
        |> List.map ~f:(fun n -> scalar (Name n))
      in
      Some (list (strip_meta r) scope (tuple scalars Cross))
    else None

  let row_store =
    of_func_pre row_store ~pre:(Resolve.resolve ~params) ~name:"to-row-store"
end
