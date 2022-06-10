open Core

module T = struct
  type name =
    | Simple of string
    | Bound of int * string
    | Attr of string * string
  [@@deriving compare, equal, hash, sexp]

  type t = { name : name; type_ : Prim_type.t option [@sexp.option] [@ignore] }
  [@@deriving compare, equal, hash, sexp]
end

include T
include Comparator.Make (T)
module C = Comparable.Make (T)

module O : Comparable.Infix with type t := t = struct
  include C

  let ( = ) = equal
  let ( <> ) x y = not (equal x y)
end

let type_ n = n.type_
let name n = match n.name with Simple x | Bound (_, x) | Attr (_, x) -> x
let create ?type_ name = { name = Simple name; type_ }

let scope n =
  match n.name with Simple _ | Attr _ -> None | Bound (i, _) -> Some i

let unscoped n =
  match n.name with
  | Simple _ | Attr _ -> n
  | Bound (_, x) -> { n with name = Simple x }

let of_string_exn s =
  match String.split s ~on:'.' with
  | [ n ] -> create n
  | [ s; n ] -> (
      try { type_ = None; name = Bound (Int.of_string s, n) }
      with _ -> { type_ = None; name = Attr (s, n) })
  | _ -> raise_s [%message "unexpected scope" s]

let shift ~cutoff d n =
  assert (d >= 0);
  match n.name with
  | Bound (i, x) when i >= cutoff -> { n with name = Bound (i + d, x) }
  | _ -> n

let incr = shift ~cutoff:0 1

let decr n =
  match n.name with
  | Bound (0, _) -> None
  | Bound (i, x) -> Some { n with name = Bound (i - 1, x) }
  | _ -> Some n

let decr_exn n =
  Option.value_exn ~message:"trying to decrease zero index" (decr n)

let set_index n i =
  match n.name with
  | Attr (_, x) | Simple x -> { n with name = Bound (i, x) }
  | Bound _ -> failwith "name already has index"

let zero n = set_index n 0

let type_exn n =
  match type_ n with
  | Some t -> t
  | None -> Error.create "Missing type." n [%sexp_of: t] |> Error.raise

let pp fmt n =
  match n.name with
  | Simple x -> Fmt.pf fmt "%s" x
  | Attr (r, x) -> Fmt.pf fmt "%s.%s" r x
  | Bound (i, x) -> Fmt.pf fmt "%d.%s" i x

let fresh fmt = create (Fresh.name Global.fresh fmt)
