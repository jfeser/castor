open Ast

type t = Univ_map.t ref [@@deriving sexp_of]

type 'a key = 'a Univ_map.Key.t

type lexpos = Lexing.position = {
  pos_fname : string;
  pos_lnum : int;
  pos_bol : int;
  pos_cnum : int;
}
[@@deriving sexp]

let empty () = ref Univ_map.empty

let update r key ~f = r.meta := Univ_map.update !(r.meta) key ~f

let find ralgebra key = Univ_map.find !(ralgebra.meta) key

let find_exn ralgebra key =
  match find ralgebra key with
  | Some x -> x
  | None ->
      Error.create "Missing metadata."
        (Univ_map.Key.name key, ralgebra)
        [%sexp_of: string * _ annot]
      |> Error.raise

let set ralgebra k v =
  { ralgebra with meta = ref (Univ_map.set !(ralgebra.meta) k v) }

let set_m { meta; _ } k v = meta := Univ_map.set !meta k v

module Direct = struct
  let find m = Univ_map.find !m

  let find_exn m = Univ_map.find_exn !m

  let set m k v = ref (Univ_map.set !m k v)

  let set_m m k v = m := Univ_map.set !m k v
end
