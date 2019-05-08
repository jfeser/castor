open! Core
open Abslayout0

type t = Univ_map.t ref [@@deriving sexp_of]

type 'a key = 'a Univ_map.Key.t

type pos = Pos of int64 | Many_pos [@@deriving sexp]

type lexpos = Lexing.position =
  {pos_fname: string; pos_lnum: int; pos_bol: int; pos_cnum: int}
[@@deriving sexp]

let empty () = ref Univ_map.empty

let defs = Univ_map.Key.create ~name:"defs" [%sexp_of: (Name.t option * pred) list]

let pos = Univ_map.Key.create ~name:"pos" [%sexp_of: pos]

let start_pos = Univ_map.Key.create ~name:"start_pos" [%sexp_of: lexpos]

let end_pos = Univ_map.Key.create ~name:"end_pos" [%sexp_of: lexpos]

let align = Univ_map.Key.create ~name:"align" [%sexp_of: int]

let eq = Univ_map.Key.create ~name:"eq" [%sexp_of: (Name.t * Name.t) list]

let order = Univ_map.Key.create ~name:"order" [%sexp_of: (pred * order) list]

let type_ = Univ_map.Key.create ~name:"type" [%sexp_of: Type.t]

let free = Univ_map.Key.create ~name:"free" [%sexp_of: Set.M(Name).t]

let update r key ~f = r.meta := Univ_map.update !(r.meta) key ~f

let find ralgebra key = Univ_map.find !(ralgebra.meta) key

let find_exn ralgebra key =
  match find ralgebra key with
  | Some x -> x
  | None ->
      Error.create "Missing metadata."
        (Univ_map.Key.name key, ralgebra)
        [%sexp_of: string * Abslayout0.t]
      |> Error.raise

let set ralgebra k v = {ralgebra with meta= ref (Univ_map.set !(ralgebra.meta) k v)}

let set_m {meta; _} k v = meta := Univ_map.set !meta k v

module Direct = struct
  let find m = Univ_map.find !m

  let find_exn m = Univ_map.find_exn !m

  let set m k v = ref (Univ_map.set !m k v)

  let set_m m k v = m := Univ_map.set !m k v
end
