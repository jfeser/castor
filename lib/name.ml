open Core
open Printf
open Collections
open Hashcons

module Key = struct
  type t = {relation: string option; name: string} [@@deriving compare, hash, sexp]

  let equal = [%compare.equal: t]
end

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
  type t = {name: Key.t Hashcons.hash_consed; meta: Univ_map.t}

  let sexp_of_t x =
    [%sexp_of: Key.t] {name= x.name.node.name; relation= x.name.node.relation}

  let create_consed r n m =
    {name= Table.(hashcons table Key.{relation= r; name= n}); meta= m}

  let t_of_sexp x =
    let Key.{name; relation} = [%of_sexp: Key.t] x in
    create_consed relation name Univ_map.empty

  let equal = phys_equal

  let compare x y =
    if equal x y then 0
    else
      [%compare: Key.t]
        {relation= x.name.node.relation; name= x.name.node.name}
        {relation= y.name.node.relation; name= y.name.node.name}

  let hash x = x.name.hkey

  let hash_fold_t s x = Hash.fold_int s x.name.hkey
end

include T
include Comparator.Make (T)
module C = Comparable.Make (T)

module O : Comparable.Infix with type t := t = struct
  include C

  let ( = ) = equal

  let ( <> ) x y = not (equal x y)
end

module Meta = struct
  let type_ = Univ_map.Key.create ~name:"type" [%sexp_of: Type.PrimType.t]

  let stage = Univ_map.Key.create ~name:"stage" [%sexp_of: [`Compile | `Run]]

  let find {meta; _} = Univ_map.find meta

  let find_exn {meta; _} = Univ_map.find_exn meta

  let set ({meta; _} as n) k v = {n with meta= Univ_map.set meta k v}
end

let create ?relation ?type_ name =
  let meta = Univ_map.empty in
  let meta =
    match type_ with Some t -> Univ_map.set meta Meta.type_ t | None -> meta
  in
  create_consed relation name meta

let type_ n = Univ_map.find n.meta Meta.type_

let copy ?relation ?type_:t ?name:n name =
  let r = Option.value relation ~default:name.name.node.relation in
  let t = Option.value t ~default:(type_ name) in
  let n = Option.value n ~default:name.name.node.name in
  let meta =
    match t with Some t -> Univ_map.set name.meta Meta.type_ t | None -> name.meta
  in
  create_consed r n meta

let name n = n.name.node.name

let rel n = n.name.node.relation

let type_exn n =
  match type_ n with
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
  let open Format in
  let name = n.name.node.name in
  match n.name.node.relation with
  | Some r -> fprintf fmt "%s.%s" r name
  | None -> fprintf fmt "%s" name

let pp_with_stage fmt n =
  let open Format in
  match Meta.(find n stage) with
  | Some `Compile -> fprintf fmt "%a@@comp" pp n
  | Some `Run -> fprintf fmt "%a@@run" pp n
  | None -> fprintf fmt "%a@@unk" pp n

let fresh f fmt = create (Fresh.name f fmt)

let create_table () =
  Bounded_int_table.create ~sexp_of_key:[%sexp_of: t] ~num_keys:!Table.max_tag
    ~key_to_int:(fun k -> k.name.tag)
    ()
