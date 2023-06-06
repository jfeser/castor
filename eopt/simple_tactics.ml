open Castor.Ast
open Castor.Collections
module Schema = Castor.Schema
open Egraph_matcher

let row_store g _ =
  (* Relation has no free variables that are bound at runtime. *)
  let%map root, _ = M.any g in
  let scalars =
    G.schema g root |> Schema.zero |> List.map ~f:(C.scalar_name g)
  in
  (root, C.list g { l_keys = root; l_values = C.tuple g scalars Cross })

let () = Ops.register row_store "row-store"
