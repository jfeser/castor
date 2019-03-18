open Core
open Printf
open Collections
open Hashcons

module Key = struct
  type t = {relation: string option; name: string} [@@deriving compare, hash, sexp]

  let equal = [%compare.equal: t]
end

open Key

module Table = struct
  include Hashcons.Make (Key)

  let table = create 32

  let max_tag = ref 0

  let hashcons t k =
    let k' = hashcons t k in
    max_tag := Int.max !max_tag k'.tag ;
    k'
end

module Sexp_T = struct
  type t = {relation: string option; name: string; type_: Type0.PrimType.t option}
  [@@deriving sexp]
end

module T = struct
  type t = {name: Key.t Hashcons.hash_consed; type_: Type0.PrimType.t option}

  let sexp_of_t x =
    [%sexp_of: Sexp_T.t]
      {name= x.name.node.name; relation= x.name.node.relation; type_= x.type_}

  let t_of_sexp x =
    let Sexp_T.{name; relation; type_} = [%of_sexp: Sexp_T.t] x in
    {name= Table.(hashcons table {name; relation}); type_}

  let compare x y = [%compare: int] x.name.tag y.name.tag

  let hash x = x.name.hkey

  let hash_fold_t s x = Hash.fold_int s x.name.hkey
end

include T
include Comparator.Make (T)
module C = Comparable.Make (T)

module O : Comparable.Infix with type t := t = C

let create_consed r t n =
  {name= Table.(hashcons table Key.{relation= r; name= n}); type_= t}

let create ?relation ?type_ name = create_consed relation type_ name

let copy ?relation ?type_ ?name n =
  let r = Option.value relation ~default:n.name.node.relation in
  let t = Option.value type_ ~default:n.type_ in
  let n = Option.value name ~default:n.name.node.name in
  create_consed r t n

let name n = n.name.node.name

let rel n = n.name.node.relation

let type_ n = n.type_

let type_exn n =
  match n.type_ with
  | Some t -> t
  | None -> Error.create "Missing type." n [%sexp_of: t] |> Error.raise

let rel_exn n =
  match n.name.node.relation with
  | Some t -> t
  | None -> Error.create "Missing relation." n [%sexp_of: t] |> Error.raise

let to_var n =
  let name = n.name.node.name in
  match n.name.node.relation with Some r -> sprintf "%s_%s" r name | None -> name

let to_sql n =
  match n.name.node.relation with
  | Some r -> sprintf "%s.\"%s\"" r n.name.node.name
  | None -> sprintf "\"%s\"" n.name.node.name

let pp fmt n =
  let open Caml.Format in
  let name = n.name.node.name in
  match n.name.node.relation with
  | Some r -> fprintf fmt "%s.%s" r name
  | None -> fprintf fmt "%s" name

let fresh f fmt = create (Fresh.name f fmt)

let create_table () =
  Bounded_int_table.create ~sexp_of_key:[%sexp_of: t] ~num_keys:!Table.max_tag
    ~key_to_int:(fun k -> k.name.tag)
    ()
