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

  let row_store r =
    let has_no_runtime_free =
      Free.free r
      |> Set.for_all ~f:(fun n ->
             match Map.find r.meta#stage n with
             | Some `Compile -> true
             | Some `Run -> false
             | None ->
                 failwith
                   (Fmt.str "Missing stage on %a. %a %a" Name.pp n
                      Fmt.Dump.(list Name.pp)
                      (Map.keys r.meta#stage) pp r))
    in
    (* Relation has no free variables that are bound at runtime. *)
    if has_no_runtime_free then
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
