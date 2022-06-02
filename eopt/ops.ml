open Core
open Castor

type t = Egraph.AstEGraph.t -> Univ_map.t -> (Egraph.Id.t * Egraph.Id.t) Iter.t

let ops : t Hashtbl.M(String).t = Hashtbl.create (module String)

let params : Set.M(Name).t Univ_map.Key.t =
  Univ_map.Key.create ~name:"params" [%sexp_of: Set.M(Name).t]

let register f name = Hashtbl.set ops ~key:name ~data:f

let apply g ctx op =
  (op g ctx) (fun (id, id') -> ignore (Egraph.AstEGraph.merge g id id'))

let of_string_exn = Hashtbl.find_exn ops
