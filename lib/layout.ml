open Base
open Base.Printf
open Collections
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
      [@@deriving compare, sexp]
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
  module Elem0 = struct
    type t = Value.t = {
      rel : Relation.t;
      field : Field.t;
      idx : int;
      value : primvalue;
    } [@@deriving sexp]

    let compare v1 v2 = compare_primvalue v1.value v2.value
  end
  module Elem = struct
    include Elem0
    include Comparator.Make(Elem0)

    let of_primvalue : primvalue -> t = fun p ->
      { value = p; rel = Relation.dummy; field = Field.dummy; idx = 0 }
  end
  type 'a t = 'a Map.M(Elem).t [@@deriving compare, sexp]
end

type range =
  (PredCtx.Key.t option * PredCtx.Key.t option) [@@deriving compare, sexp]

type ordered_list = {
  field : Field.t;
  order : [`Asc | `Desc];
  lookup : range;
} [@@deriving compare, sexp]

type table = {
  field : Field.t;
  lookup : PredCtx.Key.t;
} [@@deriving compare, sexp]

type scalar = {
  rel : Relation.t;
  field : Field.t;
  idx : int;
} [@@deriving compare, sexp]

type t =
  | Int of int * scalar
  | Bool of bool * scalar
  | String of string * scalar
  | CrossTuple of t list
  | ZipTuple of t list
  | UnorderedList of t list
  | OrderedList of t list * ordered_list
  | Table of t ValueMap.t * table
  | Empty
[@@deriving compare, sexp]

let to_value : t -> Value.t = function
  | Int (x, m) -> { field = m.field; rel = m.rel; idx = m.idx; value = `Int x }
  | Bool (x, m) ->
    { field = m.field; rel = m.rel; idx = m.idx; value = `Bool x }
  | String (x, m) ->
    { field = m.field; rel = m.rel; idx = m.idx; value = `String x }
  | x -> Error.create "Cannot be converted to value." x [%sexp_of:t]
         |> Error.raise

let of_value : Value.t -> t = fun { field; rel; idx; value } -> match value with
  | `Int x -> Int (x, { field; rel; idx })
  | `Bool x -> Bool (x, { field; rel; idx })
  | `String x -> String (x, { field; rel; idx })
  | `Unknown x -> String (x, { field; rel; idx })

let rec to_string : t -> string = function
  | Int _ -> "i"
  | Bool _ -> "b"
  | String _ -> "s"
  | ZipTuple ls ->
    List.map ls ~f:to_string |> String.concat ~sep:", " |> sprintf "z(%s)"
  | CrossTuple ls ->
    List.map ls ~f:to_string |> String.concat ~sep:", " |> sprintf "c(%s)"
  | UnorderedList ls | OrderedList (ls, _) ->
    List.map ls ~f:to_string
    |> List.count_consecutive_duplicates ~equal:String.equal
    |> List.map ~f:(fun (s, c) -> sprintf "%s x %d" s c)
    |> String.concat ~sep:", "
    |> sprintf "[%s]"
  | Table (m, _) ->
    Map.to_alist m
    |> List.map ~f:(fun (k, v) -> sprintf "k -> %s"  (to_string v))
    |> List.count_consecutive_duplicates ~equal:String.equal
    |> List.map ~f:(fun (s, c) -> sprintf "%s x %d" s c)
    |> String.concat ~sep:", "
    |> sprintf "{%s}"
  | Empty -> "[]"

let rec to_schema_exn : t -> Schema.t = function
  | Empty -> []
  | Int (_, m) | Bool (_, m) | String (_, m) -> [m.field]
  | ZipTuple ls
  | CrossTuple ls -> List.concat_map ls ~f:to_schema_exn
  | UnorderedList ls | OrderedList (ls, _) ->
    List.map ls ~f:to_schema_exn |> List.all_equal_exn
  | Table (m, { field = f }) ->
    let v_schema = Map.data m |> List.map ~f:to_schema_exn |> List.all_equal_exn in
    if not (List.mem ~equal:Field.(=) v_schema f) then
      Error.(of_string "Table values must contain the table key." |> raise);
    List.merge ~cmp:Field.compare [f] v_schema

let rec params : t -> Set.M(TypedName).t =
  let empty = Set.empty (module TypedName) in
  let union_list = Set.union_list (module TypedName) in
  let kparams = PredCtx.Key.params in
  let lparams ls = List.map ~f:params ls in
  function
  | Empty | Int _ | Bool _ | String _ -> empty
  | CrossTuple ls | ZipTuple ls | UnorderedList ls -> lparams ls |> union_list
  | OrderedList (ls, { lookup = (k1, k2) }) ->
    let p1 = Option.value_map ~f:kparams k1 ~default:empty in
    let p2 = Option.value_map ~f:kparams k2 ~default:empty in
    union_list (p1::p2::(lparams ls))
  | Table (m, {lookup = k}) -> union_list (kparams k::(lparams (Map.data m)))

let rec partition : PredCtx.Key.t -> Field.t -> t -> t =
  let p_int k f x m =
    let v = Int (x, m) in
    if Field.(f = m.field) then
      Table (Map.singleton (module ValueMap.Elem) (to_value v) v,
             {field = f; lookup = k})
    else v
  in

  let p_bool k f x m =
    let v = Bool (x, m) in
    if Field.(f = m.field) then
      Table (Map.singleton (module ValueMap.Elem) (to_value v) v,
             {field = f; lookup = k})
    else v
  in

  let p_string k f x m =
    let v = String (x, m) in
    if Field.(f = m.field) then
      Table (Map.singleton (module ValueMap.Elem) (to_value v) v,
             {field = f; lookup = k})
    else v
  in

  let p_crosstuple k f ls =
    let tbls, others =
      List.mapi ls ~f:(fun i l -> (i, partition k f l))
      |> List.partition_tf ~f:(function (_, Table _) -> true | _ -> false)
    in
    match tbls with
    | [] -> CrossTuple (List.map others ~f:(fun (_, l) -> l))
    | [(i, Table (m, {field = f}))] ->
      let elems =
        Map.map m ~f:(fun l ->
            List.fmerge ~cmp:Int.compare [(i, l)] others
            |> List.map ~f:snd
            |> fun x -> CrossTuple x)
      in
      Table (elems, {field = f; lookup = k})
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
        ~f:(fun m -> function
            | Table (m', {field = f'}) when Field.(=) f f' ->
              Map.merge m' m ~f:(fun ~key -> function
                  | `Both (l, ls) -> Some (l::ls)
                  | `Left l -> Some [l]
                  | `Right ls -> Some ls)
            | l -> Map.map m ~f:(fun ls -> l::ls))
      |> Map.map ~f:(fun ls -> ZipTuple ls)
      |> fun m -> Table (m, {field = f; lookup = k})
    else raise (TransformError (Error.of_string "Must have partition field in all positions."))
  in

  let p_unorderedlist k f ls =
    List.map ls ~f:(partition k f)
    |> List.fold_left ~init:(Map.empty (module ValueMap.Elem))
      ~f:(fun m -> function
          | Table (m', {field = f'}) when Field.(=) f f' ->
            Map.merge m' m ~f:(fun ~key -> function
                | `Both (l, ls) -> Some (l::ls)
                | `Left l -> Some [l]
                | `Right ls -> Some ls)
          | l -> Map.map m ~f:(fun ls -> l::ls))
    |> Map.map ~f:(fun ls -> UnorderedList (List.rev ls))
    |> fun m -> Table (m, {field = f; lookup = k})
  in

  let p_orderedlist k f ls ordered_list =
    let tbls, others =
      ls
      |> List.mapi ~f:(fun i l -> (i, partition k f l))
      |> List.partition_tf ~f:(function (_, Table _) -> true | _ -> false)
    in
    match tbls with
    | [] -> failwith "Expected a table."
    | ts ->
      let merged_ts = List.fold_left ts ~init:(Map.empty (module ValueMap.Elem))
          ~f:(fun m -> function
              | i, Table (m', _) ->
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
          |> fun x -> OrderedList (x, ordered_list))
      |> fun m -> Table (m, { field = f; lookup = k })
  in

  let p_table k f ls (table: table) =
    Map.map ls ~f:(fun l -> partition k f l)
    |> Map.fold ~init:(Map.empty (module ValueMap.Elem))
      ~f:(fun ~key ~data m_outer ->
          match data with
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
    |> Map.map ~f:(fun m -> Map.map m ~f:(fun ls -> UnorderedList ls))
    |> Map.map ~f:(fun m -> Table (m, table))
    |> fun m -> Table (m, { field = f; lookup = k })
  in

  fun k f l ->
    if Schema.has_field (to_schema_exn l) f then
      match l with
      | Empty -> Empty
      | Int (x, m) -> p_int k f x m
      | Bool (x, m) -> p_bool k f x m
      | String (x, m) -> p_string k f x m
      | Table (_, { field = f' }) when Field.(=) f f' -> l
      | Table (ls, t) -> p_table k f ls t
      | CrossTuple x -> p_crosstuple k f x
      | ZipTuple x -> p_ziptuple k f x
      | UnorderedList x -> p_unorderedlist k f x
      | OrderedList (ls, t) -> p_orderedlist k f ls t
    else l

let rec ntuples : t -> int = function
  | Empty -> 0
  | Int _ | Bool _ | String _ -> 1
  | CrossTuple ls -> List.fold_left ls ~init:1 ~f:(fun p l -> p * ntuples l)
  | ZipTuple ls -> List.map ls ~f:ntuples |> List.all_equal_exn
  | UnorderedList ls | OrderedList (ls, _) ->
    List.fold_left ls ~init:0 ~f:(fun p l -> p + ntuples l)
  | Table (m, _) -> Map.data m |> List.map ~f:ntuples |> List.all_equal_exn

let flatten : t -> t = function
  | Int _ | Bool _ | String _ | Table _ | Empty as l -> l
  | CrossTuple ls ->
    List.concat_map ls ~f:(function
        | CrossTuple ls' -> ls'
        | l -> [l])
    |> fun x -> CrossTuple x
  | ZipTuple ls ->
    List.concat_map ls ~f:(function
        | ZipTuple ls' -> ls'
        | l -> [l])
    |> fun x -> ZipTuple x
  | UnorderedList ls ->
    List.concat_map ls ~f:(function
        | UnorderedList ls' -> ls'
        | l -> [l])
    |> fun x -> UnorderedList x
  | OrderedList (ls, { field = f; order = ord; }) ->
    List.concat_map ls ~f:(function
        | OrderedList (ls', { field = f'; order = ord' }) as l ->
          if Base.Polymorphic_compare.equal (f, ord) (f', ord')
          then ls' else [l]
        | l -> [l])
    |> fun x -> UnorderedList x

let rec order : range -> Field.t -> [`Asc | `Desc] -> t -> t = fun k f o l ->
  let cmp = match o with
    | `Asc -> ValueMap.Elem.compare
    | `Desc -> fun k1 k2 -> ValueMap.Elem.compare k2 k1
  in
  if List.exists (to_schema_exn l) ~f:(Field.(=) f) then
    match partition PredCtx.Key.dummy f l with
    | Table (m, _) ->
      Map.to_alist m
      |> List.sort ~cmp:(fun (k1, _) (k2, _) -> cmp k1 k2)
      |> List.map ~f:(fun (k, v) -> CrossTuple [of_value k; v])
      |> fun ls -> OrderedList (ls, { field = f; order = o; lookup = k })
    | _ -> raise (TransformError (Error.of_string "Expected a table."))
  else l

let merge : range -> Field.t -> [`Asc | `Desc] -> t -> t -> t =
  fun k f o l1 l2 ->
    let cmp = match o with
    | `Asc -> ValueMap.Elem.compare
    | `Desc -> fun k1 k2 -> ValueMap.Elem.compare k2 k1
    in
    match partition PredCtx.Key.dummy f l1, partition PredCtx.Key.dummy f l2 with
    | Table (m1, _), Table (m2, _) ->
      Map.merge m1 m2 ~f:(fun ~key -> function
          | `Both (l1, l2) -> Some (UnorderedList ([l1; l2]))
          | `Left l | `Right l -> Some l)
      |> Map.to_alist
      |> List.sort ~cmp:(fun (k1, _) (k2, _) -> cmp k1 k2)
      |> List.map ~f:(fun (k, v) -> CrossTuple [of_value k; v])
      |> fun ls -> OrderedList (ls, { field = f; order = o; lookup = k })
    | _ -> raise (TransformError (Error.of_string "Expected a table."))

let project : Field.t list -> t -> t = fun fs l ->
  let f_in = List.mem ~equal:Field.equal fs in
  let rec project = function
    | Int (_, m) | Bool (_, m) | String (_, m) as v ->
      if f_in m.field then v else Empty
    | CrossTuple ls -> CrossTuple (List.map ls ~f:project)
    | ZipTuple ls ->
      let ls' =
        List.map ls ~f:project |> List.filter ~f:(fun l -> ntuples l > 0)
      in
      ZipTuple ls'
    | UnorderedList ls -> UnorderedList (List.map ls ~f:project)
    | OrderedList (ls, ({ field = f } as x)) ->
      let ls' = List.map ls ~f:project in
      if f_in f then OrderedList (ls', x) else UnorderedList ls'
    | Table (ls, ({ field = f; } as x)) ->
      let ls' = Map.map ls ~f:project in
      if f_in f then Table (ls', x) else UnorderedList (Map.data ls')
    | Empty -> Empty
  in
  project l

let tests =
  let open OUnit2 in
  let partition_tests =
    let f1 = Field.({ name = "f1"; dtype = DInt { min_val = 0; max_val = 10; distinct = 100; }}) in
    let f2 = Field.({ name = "f2"; dtype = DInt { min_val = 0; max_val = 10; distinct = 100; }}) in
    let r = Relation.({ name = "r"; fields = [f1; f2]; card = 100; }) in
    let assert_equal ~ctxt x y =
      assert_equal ~ctxt ~cmp:(fun a b -> compare a b = 0) x y
    in
    "partition" >::: [
      ("scalar" >:: fun ctxt ->
          let inp = Int (0, { rel = r; field = f1; idx = 0 }) in
          let out = Table (Map.singleton (module ValueMap.Elem) (to_value inp) inp,
                           { lookup = PredCtx.Key.dummy; field = f1 })
          in
          assert_equal ~ctxt out (partition PredCtx.Key.dummy f1 inp));
    ]
  in

  "layout" >::: [
    partition_tests
  ]
