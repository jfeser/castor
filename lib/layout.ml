open Base
open Base.Printf

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
      [@@deriving compare, sexp, hash]
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
    } [@@deriving sexp, hash]

    let compare v1 v2 = compare_primvalue v1.value v2.value
  end
  module Elem = struct
    include Elem0
    include Comparator.Make(Elem0)

    let of_primvalue : primvalue -> t = fun p ->
      { value = p; rel = Relation.dummy; field = Field.dummy; idx = 0 }
  end
  type 'a t = 'a Map.M(Elem).t [@@deriving compare, sexp, hash]
end

module Range = struct
  module T = struct
    type t = PredCtx.Key.t option * PredCtx.Key.t option
    [@@deriving compare, sexp, hash]
  end
  include T
  include Comparable.Make(T)
end

type ordered_list = {
  field : Field.t;
  order : [`Asc | `Desc];
  lookup : Range.t;
} [@@deriving compare, sexp, hash]

type table = {
  field : Field.t;
  lookup : PredCtx.Key.t;
} [@@deriving compare, sexp, hash]

type scalar = {
  rel : Relation.t;
  field : Field.t;
  idx : int;
} [@@deriving compare, sexp, hash]

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
  | Empty
[@@deriving compare, sexp]

let hash : t -> int = fun { hkey } -> hkey
let hash_fold_t state x = Hash.fold_int state (hash x)

let hash_node : node -> int = fun l -> 
  let state = ref (Hash.alloc ()) in
  begin match l with
    | Int (x, m) ->
      state := Hash.fold_int !state x;
      state := hash_fold_scalar !state m
    | Bool (x, m) ->
      state := Bool.hash_fold_t !state x;
      state := hash_fold_scalar !state m
    | String (x, m) ->
      state := String.hash_fold_t !state x;
      state := hash_fold_scalar !state m
    | Null m ->
      state := hash_fold_scalar !state m
    | CrossTuple xs ->
      state := Hash.fold_int !state 0;
      state := List.hash_fold_t hash_fold_t !state xs
    | ZipTuple xs ->
      state := Hash.fold_int !state 1;
      state := List.hash_fold_t hash_fold_t !state xs
    | UnorderedList xs ->
      state := Hash.fold_int !state 2;
      state := List.hash_fold_t hash_fold_t !state xs
    | OrderedList (xs, m) ->
      state := List.hash_fold_t hash_fold_t !state xs;
      state := hash_fold_ordered_list !state m
    | Table (xs, m) ->
      state := ValueMap.hash_fold_t hash_fold_t !state xs;
      state := hash_fold_table !state m
    | Empty -> state := Hash.fold_int !state 0
  end;
  Hash.get_hash_value !state

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
let empty = HashT.hashcons ht Empty

let to_value : t -> Value.t = fun l -> match l.node with
  | Int (x, m) -> { field = m.field; rel = m.rel; idx = m.idx; value = `Int x }
  | Bool (x, m) ->
    { field = m.field; rel = m.rel; idx = m.idx; value = `Bool x }
  | String (x, m) ->
    { field = m.field; rel = m.rel; idx = m.idx; value = `String x }
  | Null m ->
    { field = m.field; rel = m.rel; idx = m.idx; value = `Null }
  | x -> Error.create "Cannot be converted to value." x [%sexp_of:node]
         |> Error.raise

let of_value : Value.t -> t = fun { field; rel; idx; value } -> match value with
  | `Int x -> int x { field; rel; idx }
  | `Bool x -> bool x { field; rel; idx }
  | `String x -> string x { field; rel; idx }
  | `Unknown x -> string x { field; rel; idx }
  | `Null -> null { field; rel; idx }

let rec to_string : t -> string = fun l -> match l.node with
  | Int _ -> "i"
  | Bool _ -> "b"
  | String _ -> "s"
  | Null _ -> "n"
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

let rec to_schema_exn : t -> Schema.t = fun l -> match l.node with
  | Empty -> []
  | Int (_, m) | Bool (_, m) | String (_, m) | Null m -> [m.field]
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
  fun l -> match l.node with
    | Empty | Int _ | Bool _ | String _ | Null _ -> empty
    | CrossTuple ls | ZipTuple ls | UnorderedList ls -> lparams ls |> union_list
    | OrderedList (ls, { lookup = (k1, k2) }) ->
      let p1 = Option.value_map ~f:kparams k1 ~default:empty in
      let p2 = Option.value_map ~f:kparams k2 ~default:empty in
      union_list (p1::p2::(lparams ls))
    | Table (m, {lookup = k}) -> union_list (kparams k::(lparams (Map.data m)))

let rec partition : PredCtx.Key.t -> Field.t -> t -> t =
  let p_int k f x m =
    let v = int x m in
    if Field.(f = m.field) then
      table (Map.singleton (module ValueMap.Elem) (to_value v) v)
        {field = f; lookup = k}
    else v
  in

  let p_bool k f x m =
    let v = bool x m in
    if Field.(f = m.field) then
      table (Map.singleton (module ValueMap.Elem) (to_value v) v)
        {field = f; lookup = k}
    else v
  in

  let p_string k f x m =
    let v = string x m in
    if Field.(f = m.field) then
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
    else l

let rec ntuples_exn : t -> int = fun l -> match l.node with
  | Empty -> 0
  | Int _ | Bool _ | String _ | Null _ -> 1
  | CrossTuple ls -> List.fold_left ls ~init:1 ~f:(fun p l -> p * ntuples_exn l)
  | ZipTuple ls -> List.map ls ~f:ntuples_exn |> List.all_equal_exn
  | UnorderedList ls | OrderedList (ls, _) ->
    List.fold_left ls ~init:0 ~f:(fun p l -> p + ntuples_exn l)
  | Table (m, _) -> Map.data m |> List.map ~f:ntuples_exn |> List.all_equal_exn

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
      | [] -> empty
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
      | [] -> empty
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

let rec order : Range.t -> Field.t -> [`Asc | `Desc] -> t -> t = fun k f o l ->
  let cmp = match o with
    | `Asc -> ValueMap.Elem.compare
    | `Desc -> fun k1 k2 -> ValueMap.Elem.compare k2 k1
  in
  if List.exists (to_schema_exn l) ~f:(Field.(=) f) then
    match (partition PredCtx.Key.dummy f l).node with
    | Table (m, _) ->
      Map.to_alist m
      |> List.sort ~cmp:(fun (k1, _) (k2, _) -> cmp k1 k2)
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
      |> List.sort ~cmp:(fun (k1, _) (k2, _) -> cmp k1 k2)
      |> List.map ~f:(fun (k, v) -> cross_tuple [of_value k; v])
      |> fun ls -> ordered_list ls { field = f; order = o; lookup = k }
    | _ -> raise (TransformError (Error.of_string "Expected a table."))

let project : Field.t list -> t -> t = fun fs l ->
  let f_in = List.mem ~equal:Field.equal fs in
  let rec project = fun l -> match l.node with
    | Int (_, m) | Bool (_, m) | String (_, m) | Null m ->
      if f_in m.field then l else empty
    | CrossTuple ls ->
      cross_tuple (List.map ls ~f:project |> List.filter ~f:(fun l -> ntuples_exn l > 0))
    | ZipTuple ls ->
      let ls' =
        List.map ls ~f:project |> List.filter ~f:(fun l -> ntuples_exn l > 0)
      in
      zip_tuple ls'
    | UnorderedList ls -> unordered_list (List.map ls ~f:project)
    | OrderedList (ls, ({ field = f } as x)) ->
      let ls' = List.map ls ~f:project in
      if f_in f then ordered_list ls' x else unordered_list ls'
    | Table (ls, ({ field = f; } as x)) ->
      let ls' = Map.map ls ~f:project in
      if f_in f then table ls' x else unordered_list (Map.data ls')
    | Empty -> empty
  in
  project l

let eq_join : Field.t -> Field.t -> t -> t -> t = fun f1 f2 l1 l2 ->
  match (partition PredCtx.Key.Dummy f1 l1).node,
        (partition PredCtx.Key.Dummy f1 l1).node with
  | Table (m1, _), Table (m2, _) ->
    Map.merge m1 m2 ~f:(fun ~key -> function
        | `Both (l1, l2) -> Some (cross_tuple [of_value key; l1; l2])
        | `Left _ | `Right _ -> None)
    |> Map.data
    |> fun x -> unordered_list x
  | _ -> raise (TransformError (Error.of_string "Expected a table."))

let tests =
  let open OUnit2 in
  let partition_tests =
    let f1 = Field.({ name = "f1"; dtype = DInt }) in
    let f2 = Field.({ name = "f2"; dtype = DInt }) in
    let r = Relation.({ name = "r"; fields = [f1; f2]; }) in
    let assert_equal ~ctxt x y =
      assert_equal ~ctxt ~cmp:(fun a b -> compare a b = 0) x y
    in
    "partition" >::: [
      ("scalar" >:: fun ctxt ->
          let inp = int 0 { rel = r; field = f1; idx = 0 } in
          let out = table (Map.singleton (module ValueMap.Elem) (to_value inp) inp)
              { lookup = PredCtx.Key.dummy; field = f1 }
          in
          assert_equal ~ctxt out (partition PredCtx.Key.dummy f1 inp));
    ]
  in

  "layout" >::: [
    partition_tests
  ]
