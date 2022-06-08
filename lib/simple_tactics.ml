open Ast
open Collections
module A = Constructors.Annot

module Config = struct
  module type S = sig
    include Ops.Config.S
  end
end

module Make (Config : Config.S) = struct
  open Ops.Make (Config)

  let row_store r =
    (* Relation has no free variables that are bound at runtime. *)
    if Is_serializable.is_static r then
      let scalars = Schema.schema r |> Schema.zero |> List.map ~f:A.scalar_n in
      Some (A.list (strip_meta r) (A.tuple scalars Cross))
    else None

  let row_store =
    of_func_pre row_store
      ~pre:(fun r -> Is_serializable.annotate_stage @@ Schema.annotate_schema r)
      ~name:"to-row-store"
end
