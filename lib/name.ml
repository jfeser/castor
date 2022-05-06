open Core

module T = struct
  type t = {
    scope : string option; [@sexp.option]
    name : string;
    type_ : Prim_type.t option; [@sexp.option] [@ignore]
  }
  [@@deriving compare, equal, hash, sexp]
end

include T

let name n = n.name
let scope n = n.scope
let rel = scope

include Comparator.Make (T)
module C = Comparable.Make (T)

module O : Comparable.Infix with type t := t = struct
  include Comparable.Make (T)

  let ( = ) = equal
  let ( <> ) x y = not (equal x y)
end

let create ?scope ?type_ name = { scope; name; type_ }

let of_string_exn s =
  match String.split s ~on:'.' with
  | [ n ] -> create n
  | [ s; n ] -> create ~scope:s n
  | _ -> raise_s [%message "unexpected name" s]

let type_ n = n.type_

let copy ?scope:s ?type_:t ?name:n nm =
  let s = Option.value s ~default:(scope nm) in
  let t = Option.value t ~default:(type_ nm) in
  let n = Option.value n ~default:(name nm) in
  create ?scope:s ?type_:t n

let type_exn n =
  match type_ n with
  | Some t -> t
  | None -> Error.create "Missing type." n [%sexp_of: t] |> Error.raise

let scope_exn n =
  match rel n with
  | Some t -> t
  | None -> Error.create "Missing scope." n [%sexp_of: t] |> Error.raise

let rel_exn = scope_exn

let to_var n =
  let name = name n in
  match rel n with Some r -> sprintf "%s_%s" r name | None -> name

let to_sql n =
  match rel n with
  | Some r -> sprintf "%s.\"%s\"" r (name n)
  | None -> sprintf "\"%s\"" (name n)

let scoped s n = copy ~scope:(Some s) n
let unscoped n = copy ~scope:None n

let pp fmt n =
  let open Format in
  let name = name n in
  match rel n with
  | Some r -> fprintf fmt "%s.%s" r name
  | None -> fprintf fmt "%s" name

let fresh fmt = create (Fresh.name Global.fresh fmt)
