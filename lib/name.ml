open Core
open Printf
open Collections
open Hashcons

module Key = struct
  type t =
    { relation: string option
    ; name: string
    ; type_: Type0.PrimType.t option [@compare.ignore] }
  [@@deriving compare, hash, sexp]

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

module T = struct
  type t = Key.t Hashcons.hash_consed

  let sexp_of_t x = [%sexp_of: Key.t] x.node

  let t_of_sexp x = Table.(hashcons table ([%of_sexp: Key.t] x))

  let compare x y = [%compare: int] x.tag y.tag

  let hash x = x.hkey

  let hash_fold_t s x = Hash.fold_int s x.hkey
end

include T
include Comparator.Make (T)
module C = Comparable.Make (T)

module O : Comparable.Infix with type t := t = C

let create_consed r t n =
  Table.(hashcons table Key.{relation= r; type_= t; name= n})

let create ?relation ?type_ name = create_consed relation type_ name

let copy ?relation ?type_ ?name n =
  let open Key in
  let r = Option.value relation ~default:n.node.relation in
  let t = Option.value type_ ~default:n.node.type_ in
  let n = Option.value name ~default:n.node.name in
  create_consed r t n

let name n = n.node.name

let rel n = n.node.relation

let type_ n = n.node.type_

let type_exn n =
  match n.node.type_ with
  | Some t -> t
  | None -> Error.create "Missing type." n [%sexp_of: t] |> Error.raise

let rel_exn n =
  match n.node.relation with
  | Some t -> t
  | None -> Error.create "Missing relation." n [%sexp_of: t] |> Error.raise

let to_var n =
  let name = n.node.name in
  match n.node.relation with Some r -> sprintf "%s_%s" r name | None -> name

let to_sql n =
  match n.node.relation with
  | Some r -> sprintf "%s.\"%s\"" r n.node.name
  | None -> sprintf "\"%s\"" n.node.name

let pp fmt n =
  let open Caml.Format in
  let name = n.node.name in
  match n.node.relation with
  | Some r -> fprintf fmt "%s.%s" r name
  | None -> fprintf fmt "%s" name

let fresh f fmt = create (Fresh.name f fmt)

let create_table () =
  Bounded_int_table.create ~sexp_of_key:[%sexp_of: t] ~num_keys:!Table.max_tag
    ~key_to_int:(fun k -> k.tag)
    ()
