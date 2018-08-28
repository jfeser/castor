open Base
open Printf
open Collections
open Db
include Abslayout0

module Name = struct
  module T = struct
    type t = Abslayout0.name =
      { relation: string option
      ; name: string
      ; type_: Type0.PrimType.t option [@compare.ignore] }
    [@@deriving compare, hash, sexp]
  end

  include T
  include Comparable.Make (T)

  let create ?relation ?type_ name = {relation; name; type_}

  let type_exn {type_; _} =
    match type_ with Some t -> t | None -> failwith "missing type"

  let to_typed_name ({name; _} as n) = (name, type_exn n)

  let to_sql {relation; name; _} =
    match relation with
    | Some r -> sprintf "%s.\"%s\"" r name
    | None -> sprintf "\"%s\"" name

  let of_lexbuf_exn lexbuf =
    try Ralgebra_parser.name_eof Ralgebra_lexer.token lexbuf
    with Ralgebra0.ParseError (msg, line, col) as e ->
      Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
      raise e

  let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

  let of_field ?rel f =
    {relation= rel; name= f.fname; type_= Some (Type.PrimType.of_dtype f.dtype)}
end

module Meta = struct
  open Core

  type 'a key = 'a Univ_map.Key.t

  type pos = Pos of int64 | Many_pos [@@deriving sexp]

  let empty () = ref Univ_map.empty

  let schema = Univ_map.Key.create ~name:"schema" [%sexp_of : Name.t list]

  let pos = Univ_map.Key.create ~name:"pos" [%sexp_of : pos]

  let update r key ~f = r.meta := Univ_map.update !(r.meta) key ~f

  let find ralgebra key = Univ_map.find !(ralgebra.meta) key

  let find_exn ralgebra key =
    match find ralgebra key with
    | Some x -> x
    | None ->
        Error.create "Missing metadata."
          (Univ_map.Key.name key, ralgebra)
          [%sexp_of : string * t]
        |> Error.raise

  let set ralgebra k v =
    {ralgebra with meta= ref (Univ_map.set !(ralgebra.meta) k v)}
end

let select a b = {node= Select (a, b); meta= Meta.empty ()}

let filter a b = {node= Filter (a, b); meta= Meta.empty ()}

let agg a b c = {node= Agg (a, b, c); meta= Meta.empty ()}

let dedup a = {node= Dedup a; meta= Meta.empty ()}

let order_by a b c = {node= OrderBy {key= a; order= b; rel= c}; meta= Meta.empty ()}

let scan a = {node= Scan a; meta= Meta.empty ()}

let empty = {node= AEmpty; meta= Meta.empty ()}

let scalar a = {node= AScalar a; meta= Meta.empty ()}

let list a b = {node= AList (a, b); meta= Meta.empty ()}

let tuple a b = {node= ATuple (a, b); meta= Meta.empty ()}

let hash_idx a b c = {node= AHashIdx (a, b, c); meta= Meta.empty ()}

let ordered_idx a b c = {node= AOrderedIdx (a, b, c); meta= Meta.empty ()}

let as_ a b = {node= As (a, b); meta= Meta.empty ()}

let name r =
  match r.node with
  | Select _ -> "select"
  | Filter _ -> "filter"
  | Join _ -> "join"
  | Agg _ -> "agg"
  | Dedup _ -> "dedup"
  | OrderBy _ -> "order_by"
  | Scan _ -> "scan"
  | AEmpty -> "empty"
  | AScalar _ -> "scalar"
  | AList _ -> "list"
  | ATuple _ -> "tuple"
  | AHashIdx _ -> "hash_idx"
  | AOrderedIdx _ -> "ordered_idx"
  | As _ -> "as"

let pp_list pp fmt ls =
  let open Caml.Format in
  pp_open_hvbox fmt 1 ;
  fprintf fmt "[" ;
  let rec loop = function
    | [] -> ()
    | [x] -> fprintf fmt "%a" pp x
    | x :: xs -> fprintf fmt "%a,@ " pp x ; loop xs
  in
  loop ls ; close_box () ; fprintf fmt "]"

let op_to_str = function
  | Eq -> "="
  | Lt -> "<"
  | Le -> "<="
  | Gt -> ">"
  | Ge -> ">="
  | And -> "&&"
  | Or -> "||"
  | Add -> "+"
  | Sub -> "-"
  | Mul -> "*"
  | Div -> "/"
  | Mod -> "%"

let pp_name fmt =
  let open Caml.Format in
  function
  | {relation= Some r; name; _} -> fprintf fmt "%s.%s" r name
  | {relation= None; name; _} -> fprintf fmt "%s" name

let pp_kind fmt =
  Caml.Format.(function Cross -> fprintf fmt "cross" | Zip -> fprintf fmt "zip")

let rec pp_pred fmt =
  let open Caml.Format in
  function
  | Null -> fprintf fmt "null"
  | Int x -> fprintf fmt "%d" x
  | Bool x -> fprintf fmt "%B" x
  | String x -> fprintf fmt "%S" x
  | Name n -> pp_name fmt n
  | Binop (op, p1, p2) ->
      fprintf fmt "@[<h>%a@ %s@ %a@]" pp_pred p1 (op_to_str op) pp_pred p2
  | Varop _ -> failwith ""

let pp_agg fmt =
  let open Caml.Format in
  function
  | Count -> fprintf fmt "count"
  | Key n -> pp_name fmt n
  | Sum n -> fprintf fmt "sum(%a)" pp_name n
  | Avg n -> fprintf fmt "avg(%a)" pp_name n
  | Min n -> fprintf fmt "min(%a)" pp_name n
  | Max n -> fprintf fmt "max(%a)" pp_name n

let rec pp fmt {node; _} =
  let open Caml.Format in
  match node with
  | Select (ps, r) ->
      fprintf fmt "@[<hv 2>select(%a,@ %a)@]" (pp_list pp_pred) ps pp r
  | Filter (p, r) -> fprintf fmt "@[<hv 2>filter(%a,@ %a)@]" pp_pred p pp r
  | Join {pred; r1; r2} ->
      fprintf fmt "@[<hv 2>join(%a,@ %a,@ %a)@]" pp_pred pred pp r1 pp r2
  | Agg (a, k, r) ->
      fprintf fmt "@[<hv 2>agg(%a,@ %a,@ %a)@]" (pp_list pp_agg) a (pp_list pp_name)
        k pp r
  | Dedup r -> fprintf fmt "@[<hv 2>dedup(@,%a)@]" pp r
  | Scan n -> fprintf fmt "%s" n
  | AEmpty -> fprintf fmt "aempty"
  | AScalar p -> fprintf fmt "@[<hv 2>ascalar(%a)@]" pp_pred p
  | AList (r1, r2) -> fprintf fmt "@[<hv 2>alist(%a,@ %a)@]" pp r1 pp r2
  | ATuple (rs, kind) ->
      fprintf fmt "@[<hv 2>atuple(%a,@ %a)@]" (pp_list pp) rs pp_kind kind
  | AHashIdx (r1, r2, {lookup; _}) ->
      fprintf fmt "@[<hv 2>ahashidx(%a,@ %a,@ %a)@]" pp r1 pp r2 pp_pred lookup
  (* |AOrderedIdx (_, _, _) ->
      *    fprintf fmt "@[<h>%a@ %s@]" pp r n *)
  | As (n, r) ->
      fprintf fmt "@[<h>%a@ as@ %s@]" pp r n
  | _ -> failwith "unsupported"

module Ctx = struct
  type t = primvalue Map.M(Name).t [@@deriving compare, hash, sexp]

  let of_tuple : Tuple.t -> t =
   fun t ->
    List.fold_left t
      ~init:(Map.empty (module Name))
      ~f:(fun m v ->
        let n =
          { relation= Some v.rel.rname
          ; name= v.field.fname
          ; type_= Some (Type.PrimType.of_primvalue v.value) }
        in
        Map.set m ~key:n ~data:v.value )
end

let of_lexbuf_exn lexbuf =
  try Ralgebra_parser.abs_ralgebra_eof Ralgebra_lexer.token lexbuf
  with Ralgebra0.ParseError (msg, line, col) as e ->
    Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
    raise e

let of_channel_exn ch = of_lexbuf_exn (Lexing.from_channel ch)

let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

let params r =
  let ralgebra_params =
    object (self)
      inherit [_] reduce
      method zero = Set.empty (module Type.TypedName)
      method plus = Set.union
      method! visit_Name () n =
        if Option.is_none n.relation then
          Set.singleton (module Type.TypedName) (Name.to_typed_name n)
        else self#zero
      method visit_name _ _ = self#zero
    end
  in
  ralgebra_params#visit_t () r

let pred_of_value = function
  | `Bool x -> Bool x
  | `String x -> String x
  | `Int x -> Int x
  | `Null -> Null
  | `Unknown x -> String x

let rec eval_pred =
  let raise = Error.raise in
  fun ctx -> function
    | Null -> `Null
    | Int x -> `Int x
    | String x -> `String x
    | Bool x -> `Bool x
    | Name n -> (
      match Map.find ctx n with
      | Some v -> v
      | None ->
          Error.create "Unbound variable." (n, ctx) [%sexp_of : Name.t * Ctx.t]
          |> raise )
    | Binop (op, p1, p2) -> (
        let v1 = eval_pred ctx p1 in
        let v2 = eval_pred ctx p2 in
        match (op, v1, v2) with
        | Eq, `Null, _ | Eq, _, `Null -> `Bool false
        | Eq, `Bool x1, `Bool x2 -> `Bool Bool.(x1 = x2)
        | Eq, `Int x1, `Int x2 -> `Bool Int.(x1 = x2)
        | Eq, `String x1, `String x2 -> `Bool String.(x1 = x2)
        | Lt, `Int x1, `Int x2 -> `Bool (x1 < x2)
        | Le, `Int x1, `Int x2 -> `Bool (x1 <= x2)
        | Gt, `Int x1, `Int x2 -> `Bool (x1 > x2)
        | Ge, `Int x1, `Int x2 -> `Bool (x1 >= x2)
        | Add, `Int x1, `Int x2 -> `Int (x1 + x2)
        | Sub, `Int x1, `Int x2 -> `Int (x1 - x2)
        | Mul, `Int x1, `Int x2 -> `Int (x1 * x2)
        | Div, `Int x1, `Int x2 -> `Int (x1 / x2)
        | Mod, `Int x1, `Int x2 -> `Int (x1 % x2)
        | And, `Bool x1, `Bool x2 -> `Bool (x1 && x2)
        | Or, `Bool x1, `Bool x2 -> `Bool (x1 || x2)
        | _ ->
            Error.create "Unexpected argument types." (op, v1, v2)
              [%sexp_of : op * primvalue * primvalue]
            |> raise )
    | Varop (op, ps) ->
        let vs = List.map ps ~f:(eval_pred ctx) in
        match op with
        | And ->
            List.for_all vs ~f:(function
              | `Bool x -> x
              | _ -> failwith "Unexpected argument type." )
            |> fun x -> `Bool x
        | Or ->
            List.exists vs ~f:(function
              | `Bool x -> x
              | _ -> failwith "Unexpected argument type." )
            |> fun x -> `Bool x
        | _ ->
            Error.create "Unexpected argument types." (op, vs)
              [%sexp_of : op * primvalue list]
            |> raise

let subst ctx =
  let v =
    object
      inherit [_] endo
      method! visit_Name _ this v =
        match Map.find ctx v with Some x -> pred_of_value x | None -> this
      method visit_name _ x = x
    end
  in
  v#visit_t ()

let pred_relations p =
  let rels = ref [] in
  let f =
    object
      inherit [_] iter
      method! visit_Name () =
        function
        | {relation= Some r; _} -> rels := r :: !rels | {relation= None; _} -> ()
      method visit_name () _ = ()
      method visit_'m () _ = ()
    end
  in
  f#visit_pred () p ; !rels

let rec pred_to_sql = function
  | Name n -> sprintf "%s" (Name.to_sql n)
  | Int x -> Int.to_string x
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> sprintf "'%s'" s
  | Null -> "null"
  | Binop (op, p1, p2) -> (
      let s1 = sprintf "(%s)" (pred_to_sql p1) in
      let s2 = sprintf "(%s)" (pred_to_sql p2) in
      match op with
      | Eq -> sprintf "%s = %s" s1 s2
      | Lt -> sprintf "%s < %s" s1 s2
      | Le -> sprintf "%s <= %s" s1 s2
      | Gt -> sprintf "%s > %s" s1 s2
      | Ge -> sprintf "%s >= %s" s1 s2
      | And -> sprintf "%s and %s" s1 s2
      | Or -> sprintf "%s or %s" s1 s2
      | Add -> sprintf "%s + %s" s1 s2
      | Sub -> sprintf "%s - %s" s1 s2
      | Mul -> sprintf "%s * %s" s1 s2
      | Div -> sprintf "%s / %s" s1 s2
      | Mod -> sprintf "%s %% %s" s1 s2 )
  | Varop (op, ps) ->
      let ss = List.map ps ~f:(fun p -> sprintf "(%s)" (pred_to_sql p)) in
      match op with
      | And -> String.concat ss ~sep:" and "
      | Or -> String.concat ss ~sep:" or "
      | _ -> failwith "Unsupported op."

(** Return the set of relations which have fields in the tuple produced by
     this expression. *)
let relation r =
  let reducer =
    object
      inherit [_] reduce
      method zero = Set.empty (module String)
      method plus = Set.union
      method! visit_As _ n _ = Set.singleton (module String) n
      method! visit_Scan _ n = Set.singleton (module String) n
    end
  in
  reducer#visit_t () r

let ralgebra_to_sql r =
  let rec f {node; _} =
    let relation_name r =
      let rs = relation r in
      if Set.length rs > 1 then
        Error.create
          "More than one relation name. Use AS to give this expression a name." r
          [%sexp_of : t]
        |> Error.raise
      else Set.choose_exn rs
    in
    match node with
    | Select ([], r) ->
        sprintf "select top 0 from (%s) as %s" (f r) (relation_name r)
    | Select (fs, r) ->
        let fields = List.map fs ~f:pred_to_sql |> String.concat ~sep:"," in
        sprintf "select %s from (%s) as %s" fields (f r) (relation_name r)
    | Scan r -> sprintf "select * from %s" r
    | Filter (pred, r) ->
        sprintf "select * from (%s) as %s where %s" (f r) (relation_name r)
          (pred_to_sql pred)
    | Join {pred; r1; r2} ->
        let r1_name = relation_name r1 in
        let r2_name = relation_name r2 in
        sprintf "select * from (%s) as %s, (%s) as %s where %s" (f r1) r1_name
          (f r2) r2_name (pred_to_sql pred)
    | Agg (aggs, key, r) ->
        let rel = relation_name r in
        let aggs =
          List.map aggs ~f:(function
            | Count -> "count(*)"
            | Key f -> sprintf "%s.\"%s\"" rel f.name
            | Sum f -> sprintf "sum(%s.\"%s\")" rel f.name
            | Avg f -> sprintf "avg(%s.\"%s\")" rel f.name
            | Min f -> sprintf "min(%s.\"%s\")" rel f.name
            | Max f -> sprintf "max(%s.\"%s\")" rel f.name )
          |> String.concat ~sep:", "
        in
        let key =
          List.map key ~f:(fun f -> sprintf "%s.\"%s\"" rel f.name)
          |> String.concat ~sep:", "
        in
        sprintf "select %s from (%s) as %s group by (%s)" aggs (f r) rel key
    | Dedup r -> sprintf "select distinct * from (%s) as t" (f r)
    | As (_, r) -> f r
    | _ ->
        Error.of_string "Only relational algebra constructs allowed." |> Error.raise
  in
  f r

let unnamed t = {name= ""; relation= None; type_= Some t}

let pred_to_schema_exn =
  let open Type0.PrimType in
  function
  | Name ({type_= None; _} as n) ->
      Error.create "Missing type." n [%sexp_of : Name.t] |> Error.raise
  | Name ({type_= Some _; _} as n) -> n
  | Int _ -> unnamed IntT
  | Bool _ -> unnamed BoolT
  | String _ -> unnamed StringT
  | Null -> failwith ""
  | Binop (op, _, _) | Varop (op, _) ->
    match op with
    | Eq | Lt | Le | Gt | Ge | And | Or -> unnamed BoolT
    | Add | Sub | Mul | Div | Mod -> unnamed IntT

let pred_to_name = function Name n -> Some n | _ -> None

let annotate_key_layouts =
  let key_layout schema =
    match List.map schema ~f:(fun n -> scalar (Name n)) with
    | [] -> failwith "empty schema"
    | [x] -> x
    | xs -> tuple xs Cross
  in
  let annotator =
    object
      inherit [_] endo
      method! visit_t _ ({node; _} as l) =
        match node with
        | AHashIdx (x, y, ({hi_key_layout= None; _} as m)) ->
            let schema = Meta.find_exn x Meta.schema in
            { l with
              node= AHashIdx (x, y, {m with hi_key_layout= Some (key_layout schema)})
            }
        | AOrderedIdx (x, y, ({oi_key_layout= None; _} as m)) ->
            let schema = Meta.find_exn x Meta.schema in
            { l with
              node=
                AOrderedIdx (x, y, {m with oi_key_layout= Some (key_layout schema)})
            }
        | _ -> l
    end
  in
  annotator#visit_t ()
