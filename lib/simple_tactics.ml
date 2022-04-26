open Ast
open Collections
module A = Abslayout

module Config = struct
  module type S = sig
    include Ops.Config.S
  end
end

module Make (Config : Config.S) = struct
  open Config
  open Ops.Make (Config)

  let row_store r =
    (* Relation has no free variables that are bound at runtime. *)
    if Is_serializable.is_static ~params r then
      let scope = Fresh.name Global.fresh "s%d" in
      let scalars =
        Schema.schema r |> Schema.scoped scope
        |> List.map ~f:(fun n -> A.scalar (Name n))
      in
      Some (A.list (strip_meta r) scope (A.tuple scalars Cross))
    else None

  let row_store =
    of_func_pre row_store ~pre:Is_serializable.annotate_stage
      ~name:"to-row-store"
end
