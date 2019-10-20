open! Core
open Hashcons

module Key = struct
  type t = { scope : string option; name : string }
  [@@deriving compare, hash, sexp]

  let equal = [%compare.equal: t]
end

module Table = struct
  include Hashcons.Make (Key)

  let table = create 32

  let max_tag = ref 0

  let hashcons t k =
    let k' = hashcons t k in
    max_tag := Int.max !max_tag k'.tag;
    k'
end

module T = struct
  type t = { name : Key.t Hashcons.hash_consed; meta : Univ_map.t }

  let sexp_of_t x =
    [%sexp_of: Key.t] { name = x.name.node.name; scope = x.name.node.scope }

  let create_consed r n m =
    { name = Table.(hashcons table Key.{ scope = r; name = n }); meta = m }

  let t_of_sexp x =
    let Key.{ name; scope } = [%of_sexp: Key.t] x in
    create_consed scope name Univ_map.empty

  let equal = phys_equal

  let compare x y =
    if equal x y then 0
    else
      [%compare: Key.t]
        { scope = x.name.node.scope; name = x.name.node.name }
        { scope = y.name.node.scope; name = y.name.node.name }

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

  let stage = Univ_map.Key.create ~name:"stage" [%sexp_of: [ `Compile | `Run ]]

  let refcnt = Univ_map.Key.create ~name:"refcnt" [%sexp_of: int]

  let find { meta; _ } = Univ_map.find meta

  let find_exn ({ meta; _ } as n) k =
    match Univ_map.find meta k with
    | Some x -> x
    | None ->
        Error.create "Missing metadata."
          (n, Univ_map.Key.name k, meta)
          [%sexp_of: t * string * Univ_map.t]
        |> Error.raise

  let set ({ meta; _ } as n) k v = { n with meta = Univ_map.set meta k v }

  let change ({ meta; _ } as n) ~f k =
    { n with meta = Univ_map.change meta ~f k }
end

let valid_regex = Str.regexp "^[a-zA-Z_][a-zA-Z0-9_]*$"

let check_name n =
  if not (Str.string_match valid_regex n 0) then
    Error.(create "Invalid name." n [%sexp_of: string] |> raise)

let create ?scope ?type_ name =
  check_name name;
  Option.iter ~f:check_name scope;
  let meta = Univ_map.empty in
  let meta =
    match type_ with Some t -> Univ_map.set meta Meta.type_ t | None -> meta
  in
  create_consed scope name meta

let type_ n = Univ_map.find n.meta Meta.type_

let copy ?scope ?type_:t ?name:n ?meta name =
  let r = Option.value scope ~default:name.name.node.scope in
  let t = Option.value t ~default:(type_ name) in
  let n = Option.value n ~default:name.name.node.name in
  let m = Option.value meta ~default:name.meta in
  let meta =
    match t with Some t -> Univ_map.set m Meta.type_ t | None -> m
  in
  create_consed r n meta

let name n = n.name.node.name

let rel n = n.name.node.scope

let meta n = n.meta

let type_exn n =
  match type_ n with
  | Some t -> t
  | None -> Error.create "Missing type." n [%sexp_of: t] |> Error.raise

let rel_exn n =
  match n.name.node.scope with
  | Some t -> t
  | None -> Error.create "Missing scope." n [%sexp_of: t] |> Error.raise

let to_var n =
  let name = n.name.node.name in
  match n.name.node.scope with
  | Some r -> sprintf "%s_%s" r name
  | None -> name

let to_sql n =
  match n.name.node.scope with
  | Some r -> sprintf "%s.\"%s\"" r n.name.node.name
  | None -> sprintf "\"%s\"" n.name.node.name

let scoped s n = copy ~scope:(Some s) n

let unscoped n = copy ~scope:None n

let pp fmt n =
  let open Format in
  let name = n.name.node.name in
  match n.name.node.scope with
  | Some r -> fprintf fmt "%s.%s" r name
  | None -> fprintf fmt "%s" name

let pp_with_stage fmt n =
  let open Format in
  match Meta.(find n stage) with
  | Some `Compile -> fprintf fmt "%a@@comp" pp n
  | Some `Run -> fprintf fmt "%a@@run" pp n
  | None -> fprintf fmt "%a@@unk" pp n

let pp_with_stage_and_refcnt fmt n =
  let open Format in
  let stage =
    match Meta.(find n stage) with
    | Some `Compile -> "comp"
    | Some `Run -> "run"
    | None -> "unk"
  in
  let refcnt =
    match Meta.(find n refcnt) with Some x -> Int.to_string x | None -> "?"
  in
  fprintf fmt "%a@@%s#%s" pp n stage refcnt

let pp_with_stage_and_type fmt n =
  let open Format in
  let stage =
    match Meta.(find n stage) with
    | Some `Compile -> "comp"
    | Some `Run -> "run"
    | None -> "unk"
  in
  let type_ =
    match Meta.(find n type_) with
    | Some t -> Type.PrimType.to_string t
    | None -> "unk"
  in
  fprintf fmt "%a@@%s:%s" pp n stage type_

let fresh fmt = create (Fresh.name Global.fresh fmt)

let create_table () =
  Bounded_int_table.create ~sexp_of_key:[%sexp_of: t] ~num_keys:!Table.max_tag
    ~key_to_int:(fun k -> k.name.tag)
    ()
