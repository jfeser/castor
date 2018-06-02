open Base
open Base.Printf
module Pervasives = Caml.Pervasives

open Bin_prot.Std

open Collections
open Hashcons
open Db
open Type0

exception TransformError of Error.t

module PredCtx = struct
  module Key = struct
    module T = struct
      type t =
        | Field of Field.t
        | Var of TypedName.NameOnly.t
        | Dummy
      [@@deriving compare, sexp, hash, bin_io]
    end
    include T
    include Comparable.Make(T)

    let dummy : t = Dummy

    let params : t -> Set.M(TypedName).t = function
      | Var v -> Set.singleton (module TypedName) v
      | Field _ | Dummy -> Set.empty (module TypedName)
  end

  type t = primvalue Map.M(Key).t [@@deriving compare, sexp]

  let find_var : t -> string -> primvalue option = fun m x ->
    Map.find m (Var (x, BoolT))

  let find_field : t -> Field.t -> primvalue option = fun m x ->
    Map.find m (Field x)

  let of_tuple : Tuple.t -> t = fun t ->
    List.fold_left t ~init:(Map.empty (module Key)) ~f:(fun m v ->
        Map.set m ~key:(Field v.field) ~data:v.value)

  let of_vars : (string * primvalue) list -> t = fun l ->
    List.fold_left l ~init:(Map.empty (module Key)) ~f:(fun m (k, v) ->
        Map.set m ~key:(Var (k, PrimType.of_primvalue v))
          ~data:v)
end

module ValueMap = struct
  module Elem = struct
    module T = struct
      type t = Value.t = {
        rel : Relation.t; [@compare.ignore]
          field : Field.t; [@compare.ignore]
          value : primvalue;
      } [@@deriving compare, sexp, hash, bin_io]
    end
    include T
    include Comparable.Make(T)

    let of_primvalue : primvalue -> t = fun p ->
      { value = p; rel = Relation.dummy; field = Field.dummy }
  end
  type 'a t = 'a Map.M(Elem).t [@@deriving compare, sexp, hash]
end

module Range = struct
  module T = struct
    type t = PredCtx.Key.t option * PredCtx.Key.t option
    [@@deriving compare, sexp, hash, bin_io]
  end
  include T
  include Comparable.Make(T)
end

type ordered_list = {
  field : Field.t;
  order : [`Asc | `Desc];
  lookup : Range.t;
} [@@deriving compare, sexp, hash, bin_io]

type table = {
  field : Field.t;
  lookup : PredCtx.Key.t;
} [@@deriving compare, sexp, hash, bin_io]

type grouping = {
  key : Field.t list;
  output : Field.t Ralgebra0.agg list;
} [@@deriving compare, sexp, hash, bin_io]

type scalar_node = {
  rel : Relation.t;
  field : Field.t;
} [@@deriving compare, sexp, hash, bin_io]
type scalar = scalar_node hash_consed

module ScalarHashT = Hashcons.Make(struct
    type t = scalar_node
    let hash = hash_scalar_node
    let equal x y = compare_scalar_node x y = 0
  end)
let scalar_table = ScalarHashT.create 10

let scalar rel field = ScalarHashT.hashcons scalar_table { rel; field }
let scalar_of_node = ScalarHashT.hashcons scalar_table
let compare_scalar : scalar -> scalar -> int = fun s1 s2 ->
  compare s1.tag s2.tag
let scalar_of_sexp : Sexp.t -> scalar = fun s ->
  let n = [%of_sexp:scalar_node] s in
  scalar n.rel n.field
let sexp_of_scalar : scalar -> Sexp.t = fun s ->
  [%sexp_of:scalar_node] s.node
let hash_scalar { hkey } = hkey
let hash_fold_scalar state x = Hash.fold_int state (hash_scalar x)
let bin_shape_scalar = bin_shape_scalar_node
let bin_size_scalar = bin_size_scalar_node

type t = node hash_consed
and node =
  | Int of int * scalar
  | Bool of bool * scalar
  | String of string * scalar
  | Null of scalar
  | CrossTuple of t list
  | ZipTuple of t list
  | UnorderedList of t list
  | OrderedList of t list * ordered_list
  | Table of t ValueMap.t * table
  | Grouping of (t * t) list * grouping
  | Empty
[@@deriving compare, sexp, hash]

module HashT = Hashcons.Make(struct
    type t = node
    let hash = hash_node
    let equal x y = compare_node x y = 0
  end)

let ht = HashT.create 256

let int x m = HashT.hashcons ht (Int (x, m))
let bool x m = HashT.hashcons ht (Bool (x, m))
let string x m = HashT.hashcons ht (String (x, m))
let null m = HashT.hashcons ht (Null m)
let cross_tuple x = HashT.hashcons ht (CrossTuple x)
let zip_tuple x = HashT.hashcons ht (ZipTuple x)
let unordered_list x = HashT.hashcons ht (UnorderedList x)
let ordered_list x m = HashT.hashcons ht (OrderedList (x, m))
let table x m = HashT.hashcons ht (Table (x, m))
let grouping x m = HashT.hashcons ht (Grouping (x, m))
let empty = HashT.hashcons ht Empty

module Binable = struct
  type layout = t

  type binable_schema =
    | ScalarS of scalar_node
    | CrossTupleS of binable_schema list
    | ZipTupleS of binable_schema list
    | OrderedListS of binable_schema
    | UnorderedListS of binable_schema
    | TableS of scalar_node * binable_schema
    | GroupingS of binable_schema * binable_schema
    | EmptyS
  [@@deriving sexp, bin_io, hash]

  type binable_layout =
    | Int of int
    | Bool of bool
    | String of string
    | Null
    | CrossTuple of binable_layout list
    | ZipTuple of binable_layout list
    | UnorderedList of binable_layout list
    | OrderedList of binable_layout list * ordered_list
    | Table of (primvalue * binable_layout) list * table
    | Grouping of (binable_layout * binable_layout) list * grouping
    | Empty
  [@@deriving sexp, bin_io, hash]

  type t = {
    layout : binable_layout;
    schema : binable_schema;
  } [@@deriving sexp, bin_io, hash]

  let binable_schema_of_layout : layout -> binable_schema = fun l ->
    let rec f : layout -> binable_schema = fun l -> match l.node with
      | Int (_, x) | Bool (_, x) | String (_, x) | Null x -> ScalarS x.node
      | CrossTuple ls -> CrossTupleS (List.map ls ~f)
      | ZipTuple ls -> ZipTupleS (List.map ls ~f)
      | UnorderedList ls -> UnorderedListS (List.map ls ~f |> List.all_equal_exn)
      | OrderedList (ls, _) -> OrderedListS (List.map ls ~f |> List.all_equal_exn)
      | Table (ls, _) ->
        let (k, v) = Map.to_alist ls
                     |> List.map ~f:(fun (k, v) ->
                         ({ rel = k.ValueMap.Elem.rel; field = k.field }, f v))
                     |> List.all_equal_exn
        in
        TableS (k, v)
      | Grouping (ls, _) ->
        let (x1, x2) = List.map ls ~f:(fun (x1, x2) -> (f x1, f x2))
                       |> List.all_equal_exn in
        GroupingS (x1, x2)
      | Empty -> EmptyS
    in
    f l

  let binable_layout_of_layout : layout -> binable_layout = fun l ->
    let rec of_layout : layout -> binable_layout =
      fun l -> match l.node with
        | Int (v, _) -> Int v
        | Bool (v, _) -> Bool v
        | String (v, _) -> String v
        | Null _ -> Null
        | CrossTuple v -> CrossTuple (List.map ~f:of_layout v)
        | ZipTuple v -> ZipTuple (List.map ~f:of_layout v)
        | UnorderedList v -> UnorderedList (List.map ~f:of_layout v)
        | OrderedList (v, x) -> OrderedList (List.map ~f:of_layout v, x)
        | Table (v, x) -> Table (Map.to_alist v |> List.map ~f:(fun (k, v) ->
            k.Value.value, of_layout v), x)
        | Grouping (v, x) ->
          Grouping (List.map ~f:(fun (k, v) -> of_layout k, of_layout v) v, x)
        | Empty -> Empty
    in
    of_layout l

  let of_layout : layout -> t = fun l ->
    { layout = binable_layout_of_layout l; schema = binable_schema_of_layout l }

  let rec to_layout : t -> layout = fun { layout; schema } ->
    let rec f l s = match l, s with
      | Int x, ScalarS m -> int x (scalar_of_node m)
      | Bool x, ScalarS m -> bool x (scalar_of_node m)
      | String x, ScalarS m -> string x (scalar_of_node m)
      | Null, ScalarS m -> null (scalar_of_node m)
      | CrossTuple xs, CrossTupleS ss -> cross_tuple (List.map2_exn xs ss ~f)
      | ZipTuple xs, ZipTupleS ss -> zip_tuple (List.map2_exn xs ss ~f)
      | UnorderedList xs, UnorderedListS s ->
        unordered_list (List.map xs ~f:(fun l -> f l s))
      | OrderedList (xs, m), OrderedListS s ->
        ordered_list (List.map xs ~f:(fun l -> f l s)) m
      | Table (xs, m), TableS (scalar, s) ->
        table (List.map ~f:(fun (k, v) ->
            (Value.({ rel = scalar.rel; field = scalar.field; value = k }), f v s)) xs
               |> Map.of_alist_exn (module ValueMap.Elem)) m
      | Grouping (xs, m), GroupingS (s1, s2) ->
        grouping (List.map xs ~f:(fun (x1, x2) -> (f x1 s1, f x2 s2))) m
      | Empty, EmptyS | Empty, ScalarS _ -> empty
      | _ -> Error.create "Mismatched layout & schema." (l, s)
               [%sexp_of:binable_layout * binable_schema] |> Error.raise
    in
    f layout schema
end

let to_value : t -> Value.t = fun l -> match l.node with
  | Int (x, m) -> { field = m.node.field; rel = m.node.rel; value = `Int x }
  | Bool (x, m) -> { field = m.node.field; rel = m.node.rel; value = `Bool x }
  | String (x, m) -> { field = m.node.field; rel = m.node.rel; value = `String x }
  | Null m -> { field = m.node.field; rel = m.node.rel; value = `Null }
  | x -> Error.create "Cannot be converted to value." x [%sexp_of:node]
         |> Error.raise

let of_value : Value.t -> t = fun { field; rel; value } ->
  let s = scalar rel field in
  match value with
  | `Int x -> int x s
  | `Bool x -> bool x s
  | `String x -> string x s
  | `Unknown x -> string x s
  | `Null -> null s

(* let rec to_string : t -> string = fun l -> match l.node with
 *   | Int _ -> "i"
 *   | Bool _ -> "b"
 *   | String _ -> "s"
 *   | Null _ -> "n"
 *   | ZipTuple ls ->
 *     List.map ls ~f:to_string |> String.concat ~sep:", " |> sprintf "z(%s)"
 *   | CrossTuple ls ->
 *     List.map ls ~f:to_string |> String.concat ~sep:", " |> sprintf "c(%s)"
 *   | UnorderedList ls | OrderedList (ls, _) ->
 *     List.map ls ~f:to_string
 *     |> List.count_consecutive_duplicates ~equal:String.equal
 *     |> List.map ~f:(fun (s, c) -> sprintf "%s x %d" s c)
 *     |> String.concat ~sep:", "
 *     |> sprintf "[%s]"
 *   | Table (m, _) ->
 *     Map.to_alist m
 *     |> List.map ~f:(fun (k, v) -> sprintf "k -> %s"  (to_string v))
 *     |> List.count_consecutive_duplicates ~equal:String.equal
 *     |> List.map ~f:(fun (s, c) -> sprintf "%s x %d" s c)
 *     |> String.concat ~sep:", "
 *     |> sprintf "{%s}"
 *   | Empty -> "[]" *)

let grouping_to_schema : Field.t Ralgebra0.agg list -> Schema.t = fun output ->
  List.map output ~f:(function
      | Count -> Field.({ name = "count"; dtype = DInt; })
      | Key f -> f
      | Sum f -> Field.({ name = sprintf "sum(%s)" f.name; dtype = DInt; })
      | Avg f -> Field.({ name = sprintf "avg(%s)" f.name; dtype = DInt; })
      | Min f -> Field.({ name = sprintf "min(%s)" f.name; dtype = DInt; })
      | Max f -> Field.({ name = sprintf "max(%s)" f.name; dtype = DInt; }))

  let ok_exn = function
    | Ok x -> x
    | Error e -> raise (TransformError e)

let rec to_schema_exn : t -> Schema.t = fun l -> match l.node with
  | Empty -> []
  | Int (_, m) | Bool (_, m) | String (_, m) | Null m -> [m.node.field]
  | ZipTuple ls
  | CrossTuple ls -> List.concat_map ls ~f:to_schema_exn
  | UnorderedList ls | OrderedList (ls, _) ->
    List.map ls ~f:to_schema_exn
    |> List.all_equal ~sexp_of_t:[%sexp_of:Field.t list]
    |> Or_error.tag ~tag:"to_schema_exn List"
    |> ok_exn
  | Table (m, { field = f }) ->
    let v_schema =
      Map.data m
      |> List.map ~f:to_schema_exn
      |> List.all_equal ~sexp_of_t:[%sexp_of:Field.t list]
      |> Or_error.tag ~tag:"to_schema_exn Table value"
      |> ok_exn
    in
    List.merge ~compare:Field.compare [f] v_schema
  | Grouping (_, { output }) -> grouping_to_schema output

let rec params : t -> Set.M(TypedName).t =
  let empty = Set.empty (module TypedName) in
  let union_list = Set.union_list (module TypedName) in
  let kparams = PredCtx.Key.params in
  let lparams ls = List.map ~f:params ls in
  fun l -> match l.node with
    | Empty | Int _ | Bool _ | String _ | Null _ -> empty
    | CrossTuple ls | ZipTuple ls | UnorderedList ls -> lparams ls |> union_list
    | OrderedList (ls, { lookup = (k1, k2) }) ->
      let p1 = Option.value_map ~f:kparams k1 ~default:empty in
      let p2 = Option.value_map ~f:kparams k2 ~default:empty in
      union_list (p1::p2::(lparams ls))
    | Grouping (ls, _) ->
      let pk, pv = List.map ls ~f:(fun (k, v) -> params k, params v)
                   |> List.unzip
      in
      union_list (pk @ pv)
    | Table (m, {lookup = k}) -> union_list (kparams k::(lparams (Map.data m)))

let rec partition : PredCtx.Key.t -> Field.t -> t -> t =
  let p_int k f x m =
    let v = int x m in
    if Field.(f = m.node.field) then
      table (Map.singleton (module ValueMap.Elem) (to_value v) v)
        {field = f; lookup = k}
    else v
  in

  let p_bool k f x m =
    let v = bool x m in
    if Field.(f = m.node.field) then
      table (Map.singleton (module ValueMap.Elem) (to_value v) v)
        {field = f; lookup = k}
    else v
  in

  let p_string k f x m =
    let v = string x m in
    if Field.(f = m.node.field) then
      table (Map.singleton (module ValueMap.Elem) (to_value v) v)
        {field = f; lookup = k}
    else v
  in

  let p_crosstuple k f ls =
    let tbls, others =
      List.mapi ls ~f:(fun i l -> (i, partition k f l))
      |> List.partition_tf ~f:(fun (_, l) -> match l.node with
            Table _ -> true | _ -> false)
    in
    match tbls with
    | [] -> cross_tuple (List.map others ~f:(fun (_, l) -> l))
    | [(i, {node = Table (m, {field = f})})] ->
      let elems =
        Map.map m ~f:(fun l ->
            List.fmerge ~cmp:Int.compare [(i, l)] others
            |> List.map ~f:snd
            |> fun x -> cross_tuple x)
      in
      table elems {field = f; lookup = k}
    | _ -> raise (TransformError (Error.of_string "Bad schema."))
  in

  let p_ziptuple k f ls =
    let all_have_f =
      List.for_all ls ~f:(fun l ->
          List.mem ~equal:Field.(=) (to_schema_exn l) f)
    in
    if all_have_f then
      List.map ls ~f:(partition k f)
      |> List.fold_left ~init:(Map.empty (module ValueMap.Elem))
        ~f:(fun m l -> match l.node with
            | Table (m', {field = f'}) when Field.(=) f f' ->
              Map.merge m' m ~f:(fun ~key -> function
                  | `Both (l, ls) -> Some (l::ls)
                  | `Left l -> Some [l]
                  | `Right ls -> Some ls)
            | _ -> Map.map m ~f:(fun ls -> l::ls))
      |> Map.map ~f:(fun ls -> zip_tuple ls)
      |> fun m -> table m {field = f; lookup = k}
    else raise (TransformError (Error.of_string "Must have partition field in all positions."))
  in

  let p_unorderedlist k f ls =
    List.map ls ~f:(partition k f)
    |> List.fold_left ~init:(Map.empty (module ValueMap.Elem))
      ~f:(fun m l -> match l.node with
          | Table (m', {field = f'}) when Field.(=) f f' ->
            Map.merge m' m ~f:(fun ~key -> function
                | `Both (l, ls) -> Some (l::ls)
                | `Left l -> Some [l]
                | `Right ls -> Some ls)
          | _ -> Map.map m ~f:(fun ls -> l::ls))
    |> Map.map ~f:(fun ls -> unordered_list (List.rev ls))
    |> fun m -> table m {field = f; lookup = k}
  in

  let p_orderedlist k f ls ol =
    let tbls, others =
      ls
      |> List.mapi ~f:(fun i l -> (i, partition k f l))
      |> List.partition_tf ~f:(fun (_, l) -> match l.node with
            Table _ -> true | _ -> false)
    in
    match tbls with
    | [] -> failwith "Expected a table."
    | ts ->
      let merged_ts = List.fold_left ts ~init:(Map.empty (module ValueMap.Elem))
          ~f:(fun m -> fun (i, l) -> match l.node with
              | Table (m', _) ->
                Map.merge m' m ~f:(fun ~key -> function
                    | `Left l -> Some [i, l]
                    | `Right ls -> Some ls
                    | `Both (l, ls) ->
                      Some (List.fmerge Int.compare [i, l] ls))
              | _ -> raise (TransformError (Error.of_string "Expected a table.")))
      in
      Map.map merged_ts ~f:(fun ls ->
          List.fmerge Int.compare others ls
          |> List.map ~f:snd
          |> fun x -> ordered_list x ol)
      |> fun m -> table m { field = f; lookup = k }
  in

  let p_table k f ls (tbl: table) =
    Map.map ls ~f:(fun l -> partition k f l)
    |> Map.fold ~init:(Map.empty (module ValueMap.Elem))
      ~f:(fun ~key ~data m_outer ->
          match data.node with
          | Table (m_inner, _) ->
            Map.merge (Map.map m_inner ~f:(fun v ->
                Map.singleton (module ValueMap.Elem) key v))
              m_outer
              ~f:(fun ~key -> function
                  | `Both (l, ls) ->
                    Some (Map.merge l ls ~f:(fun ~key -> function
                        | `Both (x, xs) -> Some (x::xs)
                        | `Left x -> Some [x]
                        | `Right xs -> Some xs))
                  | `Left l -> Some (Map.map l ~f:(fun x -> [x]))
                  | `Right ls -> Some ls)
          | _ -> failwith "BUG: Map rhs must have same schema.")
    |> Map.map ~f:(fun m -> Map.map m ~f:(fun ls -> unordered_list ls))
    |> Map.map ~f:(fun m -> table m tbl)
    |> fun m -> table m { field = f; lookup = k }
  in

  let p_grouping k f ls m =
    let contents =
      List.map ls ~f:(fun (k, v) -> cross_tuple [k; v])
      |> unordered_list
    in
    match (partition k f contents).node with
    | Table (ls, m') ->
      Map.map ls ~f:(fun x -> match x.node with
          | UnorderedList y ->
            List.map y ~f:(fun z -> match z.node with
                | CrossTuple [k; v] -> k, v
                | _ -> failwith "")
            |> fun ls -> grouping ls m
          | _ -> failwith "")
      |> fun ls -> table ls m'
    | _ -> failwith ""
  in

  fun k f l ->
    if Schema.has_field (to_schema_exn l) f then
      match l.node with
      | Empty -> empty
      | Null _ -> empty
      | Int (x, m) -> p_int k f x m
      | Bool (x, m) -> p_bool k f x m
      | String (x, m) -> p_string k f x m
      | Table (_, { field = f' }) when Field.(=) f f' -> l
      | Table (ls, t) -> p_table k f ls t
      | CrossTuple x -> p_crosstuple k f x
      | ZipTuple x -> p_ziptuple k f x
      | UnorderedList x -> p_unorderedlist k f x
      | OrderedList (ls, t) -> p_orderedlist k f ls t
      | Grouping (ls, m) -> p_grouping k f ls m
    else l

let rec ntuples_exn : t -> int = fun l -> match l.node with
  | Empty -> 0
  | Int _ | Bool _ | String _ | Null _ -> 1
  | CrossTuple ls -> List.fold_left ls ~init:1 ~f:(fun p l -> p * ntuples_exn l)
  | ZipTuple ls ->
    List.map ls ~f:ntuples_exn
    |> List.all_equal ~sexp_of_t:[%sexp_of:int]
    |> Or_error.tag ~tag:"ntuples_exn ZipTuple"
    |> ok_exn
  | UnorderedList ls | OrderedList (ls, _) ->
    List.fold_left ls ~init:0 ~f:(fun p l -> p + ntuples_exn l)
  | Table (m, _) ->
    Map.data m
    |> List.map ~f:ntuples_exn
    |> List.all_equal ~sexp_of_t:[%sexp_of:int]
    |> Or_error.tag ~tag:"ntuples_exn Table"
    |> ok_exn
  | Grouping (xs, _) -> List.length xs

let ntuples : t -> int Or_error.t = fun l ->
  Or_error.try_with (fun () -> ntuples_exn l)

let rec flatten : t -> t = fun l -> match l.node with
  | Null _ | Int _ | Bool _ | String _ | Empty -> l
  | Table (m, x) ->
    let m' = Map.map m ~f:flatten in
    if Map.for_all m' ~f:(fun v -> compare v empty = 0)
    then empty else table m' x
  | CrossTuple ls ->
    let ls = List.map ls ~f:flatten in
    if List.exists ls ~f:(fun v -> compare v empty = 0) then empty else
      let ls = List.concat_map ls ~f:(fun l -> match l.node with
          | CrossTuple ls' -> ls'
          | _ -> [l])
      in
      begin match ls with
      | [l] -> l
      | ls -> cross_tuple ls
      end
  | ZipTuple ls ->
    let ls = List.map ls ~f:flatten in
    if List.exists ls ~f:(fun v -> compare v empty = 0) then empty else
      let ls = List.concat_map ls ~f:(fun l -> match l.node with
          | ZipTuple ls' -> ls'
          | _ -> [l])
      in
      begin match ls with
      | [l] -> l
      | ls -> zip_tuple ls
      end
  | UnorderedList ls ->
    let ls =
      List.map ls ~f:flatten
      |> List.filter ~f:(fun v -> compare v empty <> 0)
      |> List.concat_map ~f:(fun l -> match l.node with
          | UnorderedList ls' | OrderedList (ls', _) -> ls'
          | _ -> [l])
    in
    begin match ls with
      | [] -> empty
      | [l] -> l
      | ls -> unordered_list ls
    end
  | OrderedList (ls, x) ->
    let ls =
      List.map ls ~f:flatten
      |> List.filter ~f:(fun v -> compare v empty <> 0)
      |> List.concat_map ~f:(fun l -> match l.node with
          | UnorderedList ls' | OrderedList (ls', _) -> ls'
          | _ -> [l])
    in
    begin match ls with
      | [] -> empty
      | [l] -> l
      | ls -> ordered_list ls x
    end
  | Grouping (ls, m) ->
    let ls = List.map ls ~f:(fun (k, v) -> flatten k, flatten v) in
    begin match ls with
      | [] -> empty
      | ls -> grouping ls m
    end

let rec order : Range.t -> Field.t -> [`Asc | `Desc] -> t -> t = fun k f o l ->
  let cmp = match o with
    | `Asc -> ValueMap.Elem.compare
    | `Desc -> fun k1 k2 -> ValueMap.Elem.compare k2 k1
  in
  if List.exists (to_schema_exn l) ~f:(Field.(=) f) then
    match (partition PredCtx.Key.dummy f l).node with
    | Table (m, _) ->
      Map.to_alist m
      |> List.sort ~compare:(fun (k1, _) (k2, _) -> cmp k1 k2)
      |> List.map ~f:(fun (k, v) -> cross_tuple [of_value k; v])
      |> fun ls -> ordered_list ls { field = f; order = o; lookup = k }
    | _ -> raise (TransformError (Error.of_string "Expected a table."))
  else l

let merge : Range.t -> Field.t -> [`Asc | `Desc] -> t -> t -> t =
  fun k f o l1 l2 ->
    let cmp = match o with
    | `Asc -> ValueMap.Elem.compare
    | `Desc -> fun k1 k2 -> ValueMap.Elem.compare k2 k1
    in
    match (partition PredCtx.Key.dummy f l1).node,
          (partition PredCtx.Key.dummy f l2).node with
    | Table (m1, _), Table (m2, _) ->
      Map.merge m1 m2 ~f:(fun ~key -> function
          | `Both (l1, l2) -> Some (unordered_list ([l1; l2]))
          | `Left l | `Right l -> Some l)
      |> Map.to_alist
      |> List.sort ~compare:(fun (k1, _) (k2, _) -> cmp k1 k2)
      |> List.map ~f:(fun (k, v) -> cross_tuple [of_value k; v])
      |> fun ls -> ordered_list ls { field = f; order = o; lookup = k }
    | _ -> raise (TransformError (Error.of_string "Expected a table."))

let project : Field.t list -> t -> t = fun fs l ->
  let f_in = List.mem ~equal:Field.equal fs in
  let rec project l = match l.node with
    | Grouping _ | Empty -> l
    | Int (_, m) | Bool (_, m) | String (_, m) | Null m ->
      if f_in m.node.field then l else empty
    | CrossTuple ls ->
      cross_tuple (List.map ls ~f:project |> List.filter ~f:(fun l -> compare l empty <> 0))
    | ZipTuple ls ->
      zip_tuple (List.map ls ~f:project |> List.filter ~f:(fun l -> compare l empty <> 0))
    | UnorderedList ls -> unordered_list (List.map ls ~f:project)
    | OrderedList (ls, ({ field = f } as x)) ->
      let ls' = List.map ls ~f:project in
      ordered_list ls' x
    | Table (ls, ({ field = f; } as x)) ->
      let ls' = Map.map ls ~f:project in
      table ls' x
  in
  project l

let eq_join : Field.t -> Field.t -> t -> t -> t Lazy.t list = fun f1 f2 l1 l2 ->
  let Table (t1, m1) = (partition PredCtx.Key.Dummy f1 l1).node in
  let Table (t2, m2) = (partition PredCtx.Key.Dummy f2 l2).node in
  [
    (* Crosstuples grouping on join keys *)
    lazy begin
      Map.merge t1 t2 ~f:(fun ~key -> function
          | `Both (l1, l2) -> Some (cross_tuple [of_value key; l1; l2])
          | `Left _ | `Right _ -> None)
      |> Map.data
      |> fun x -> unordered_list x
    end;

    (* Materialized hash join on lhs *)
    lazy begin
      cross_tuple [l1; table t2 {m2 with lookup = PredCtx.Key.Field f1}]
    end;

    (* Materialized hash join on rhs *)
    lazy begin
      cross_tuple [l2; table t1 {m1 with lookup = PredCtx.Key.Field f2}]
    end;
  ]

let accum : [`Gt | `Lt | `Ge | `Le] -> t -> t = fun dir l ->
  match l.node with
  | Table (m, x) ->
    let kv = match dir with
      | `Gt | `Ge -> Map.to_alist ~key_order:`Decreasing m
      | `Lt | `Le -> Map.to_alist ~key_order:`Increasing m
    in
    let (m, _) = List.fold_left kv ~init:(Map.empty (module ValueMap.Elem), [])
        ~f:(fun (m, vs) (k, v) ->
            let v' = match dir with
              | `Gt | `Lt -> vs
              | `Ge | `Le -> v::vs
            in
            Map.set m ~key:k ~data:(unordered_list v'), v::vs)
    in
    table m x
  | _ -> raise (TransformError (Error.of_string "Expected a table."))

let group_by : Field.t Ralgebra0.agg list -> Field.t list -> t -> t =
  fun out key l ->
    let kv =
      List.fold_left key ~init:[([], l)] ~f:(fun ls f ->
          List.concat_map ls ~f:(fun (ks, l) ->
              match (partition Dummy f l).node with
              | Table (xs, _) ->
                Map.to_alist xs |> List.map ~f:(fun (k, v) -> k::ks, v)
              | _ -> Error.of_string "Expected a table." |> Error.raise))
      |> List.map ~f:(fun (ks, v) -> cross_tuple (List.map ks ~f:of_value), v)
    in
    grouping kv { key; output = out }

(* let count : t -> t = fun { node } -> match node with
 *   | Int (_, _)
 *   | Bool (_, _)
 *   | String (_, _)
 *   | Null _ -> int 1 ??
 *   | CrossTuple ls -> 
 *   | ZipTuple _
 *   | UnorderedList _
 *   | OrderedList (_, _)
 *   | Table (_, _)
 *   | Empty -> int 0 ?? *)

let tests =
  let open OUnit2 in
  let partition_tests =
    let f1 = Field.({ name = "f1"; dtype = DInt; }) in
    let f2 = Field.({ name = "f2"; dtype = DInt; }) in
    let r = Relation.({ name = "r"; fields = [f1; f2]; }) in
    let assert_equal ~ctxt x y =
      assert_equal ~ctxt ~cmp:(fun a b -> compare a b = 0) x y
    in
    "partition" >::: [
      ("scalar" >:: fun ctxt ->
          let inp = int 0 (scalar r f1) in
          let out = table (Map.singleton (module ValueMap.Elem) (to_value inp) inp)
              { lookup = PredCtx.Key.dummy; field = f1 }
          in
          assert_equal ~ctxt out (partition PredCtx.Key.dummy f1 inp));
    ]
  in

  "layout" >::: [
    partition_tests
  ]
