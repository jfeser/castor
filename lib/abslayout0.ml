open Base

type meta = Core.Univ_map.t ref [@@deriving sexp_of]

type name =
  { relation: string option
  ; name: string
  ; type_: Type0.PrimType.t option [@compare.ignore] }
[@@deriving compare, sexp, hash]

type op = Eq | Lt | Le | Gt | Ge | And | Or | Add | Sub | Mul | Div | Mod
[@@deriving compare, sexp]

(* - Visitors doesn't use the special method override syntax that warning 7 checks
   for.
   - It uses the Pervasives module directly, which Base doesn't like. *)
[@@@warning "-7"]

module Pervasives = Caml.Pervasives

type pred =
  | Name of (name[@opaque])
  | Int of (int[@opaque])
  | Bool of (bool[@opaque])
  | String of (string[@opaque])
  | Null
  | Binop of ((op[@opaque]) * pred * pred)
  | As_pred of (pred * string)

and agg =
  | Count
  | Key of (name[@opaque])
  | Sum of (name[@opaque])
  | Avg of (name[@opaque])
  | Min of (name[@opaque])
  | Max of (name[@opaque])

and hash_idx = {hi_key_layout: t option; lookup: pred list}

and ordered_idx =
  { oi_key_layout: t option
  ; lookup_low: pred
  ; lookup_high: pred
  ; order: ([`Asc | `Desc][@opaque]) }

and tuple = Cross | Zip

and t = {node: node; meta: meta [@opaque] [@compare.ignore]}

and node =
  | Select of pred list * t
  | Filter of pred * t
  | Join of {pred: pred; r1: t; r2: t}
  | Agg of agg list * (name[@opaque]) list * t
  | OrderBy of {key: pred list; order: ([`Asc | `Desc][@opaque]); rel: t}
  | Dedup of t
  | Scan of string
  | AEmpty
  | AScalar of pred
  | AList of (t * t)
  | ATuple of (t list * tuple)
  | AHashIdx of (t * t * hash_idx)
  | AOrderedIdx of (t * t * ordered_idx)
  | As of string * t
[@@deriving
  visitors {variety= "endo"}
  , visitors {variety= "map"}
  , visitors {variety= "iter"}
  , visitors {variety= "reduce"}
  , visitors {variety= "fold"; ancestors= ["map"]}
  , sexp_of
  , compare]

[@@@warning "+7"]

module Meta = struct
  open Core

  type 'a key = 'a Univ_map.Key.t

  type pos = Pos of int64 | Many_pos [@@deriving sexp]

  type lexpos = Lexing.position =
    {pos_fname: string; pos_lnum: int; pos_bol: int; pos_cnum: int}
  [@@deriving sexp]

  let empty () = ref Univ_map.empty

  let schema = Univ_map.Key.create ~name:"schema" [%sexp_of: name list]

  let pos = Univ_map.Key.create ~name:"pos" [%sexp_of: pos]

  let start_pos = Univ_map.Key.create ~name:"start_pos" [%sexp_of: lexpos]

  let end_pos = Univ_map.Key.create ~name:"end_pos" [%sexp_of: lexpos]

  let align = Univ_map.Key.create ~name:"align" [%sexp_of: int]

  let use_foreach = Univ_map.Key.create ~name:"use_foreach" [%sexp_of: bool]

  let update r key ~f = r.meta := Univ_map.update !(r.meta) key ~f

  let find ralgebra key = Univ_map.find !(ralgebra.meta) key

  let find_exn ralgebra key =
    match find ralgebra key with
    | Some x -> x
    | None ->
        Error.create "Missing metadata."
          (Univ_map.Key.name key, ralgebra)
          [%sexp_of: string * t]
        |> Error.raise

  let set ralgebra k v =
    {ralgebra with meta= ref (Univ_map.set !(ralgebra.meta) k v)}

  let set_m {meta; _} k v = meta := Univ_map.set !meta k v
end
