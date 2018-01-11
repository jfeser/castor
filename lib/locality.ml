open Base
open Base.Printf
module Format = Caml.Format

open Collections

type dtype =
  | DInt of { min_val : int; max_val : int; distinct : int }
  | DString of { min_bits : int; max_bits : int; distinct : int }
  | DTimestamp of { distinct : int }
  | DInterval of { distinct : int }
  | DBool of { distinct : int }
[@@deriving compare, sexp]

module Field = struct
  module T = struct
    type t = {
      name: string;
      dtype : dtype;
    } [@@deriving compare, sexp]
  end
  include T
  include Comparable.Make(T)

  let dummy = { name = ""; dtype = DBool { distinct = 0 } }
end

module Relation = struct
  type t = {
    name : string;
    fields : Field.t list;
    card : int;
  } [@@deriving compare, sexp]

  let dummy = { name = ""; fields = []; card = 0; }
end

let exec : ?verbose : bool -> ?params : string list -> Postgresql.connection -> string -> string list list =
  fun ?(verbose=true) ?(params=[]) conn query ->
    let query = match params with
      | [] -> query
      | _ ->
        List.foldi params ~init:query ~f:(fun i q v ->
            String.substr_replace_all ~pattern:(sprintf "$%d" i) ~with_:v q)
    in
    if verbose then Stdio.print_endline query;
    let r = conn#exec query in
    match r#status with
    | Postgresql.Fatal_error -> failwith r#error
    | _ -> r#get_all_lst

(* type op =
 *   | Eq
 *   | Le
 *   | Ge
 *   | Lt
 *   | Gt
 *   | And
 *   | Or *)

type primvalue =
  [`Int of int | `String of string | `Bool of bool | `Unknown of string]
[@@deriving compare, sexp]

module Value = struct
  module T = struct
    type t = {
      rel : Relation.t;
      field : Field.t;
      idx : int;
      value : primvalue;
    } [@@deriving compare, sexp]
  end
  include T
  include Comparator.Make(T)
end

module Tuple = struct
  module T = struct
    type t = Value.t list [@@deriving compare, sexp]
  end
  include T
  include Comparable.Make(T)

  module ValueF = struct
    module T = struct
      type t = Value.t
      let compare v1 v2 = Field.compare v1.Value.field v2.Value.field
      let sexp_of_t = Value.sexp_of_t
    end
    include T
    include Comparable.Make(T)
  end

  let field : t -> Field.t -> Value.t option = fun t f ->
    List.find t ~f:(fun v -> Field.(f = v.field))

  let field_exn : t -> Field.t -> Value.t = fun t f ->
    Option.value_exn
      (List.find t ~f:(fun v -> Field.(f = v.field)))

  let merge : t -> t -> t = fun t1 t2 ->
    List.append t1 t2
    |> List.remove_duplicates (module ValueF)

  let merge_many : t list -> t = fun ts -> 
    List.concat ts
    |> List.remove_duplicates (module ValueF)
end

module PredCtx = struct
  module Key = struct
    module T = struct
      type t =
        | Field of Field.t
        | Var of string
        | Dummy
      [@@deriving compare, sexp]
    end
    include T
    include Comparable.Make(T)

    let dummy : t = Dummy
  end

  type t = primvalue Map.M(Key).t [@@deriving compare, sexp]

  let of_tuple : Tuple.t -> t = fun t ->
    List.fold_left t ~init:(Map.empty (module Key)) ~f:(fun m v ->
        Map.set m ~key:(Field v.field) ~data:v.value)

  let of_vars : (string * primvalue) list -> t = fun l ->
    List.fold_left l ~init:(Map.empty (module Key)) ~f:(fun m (k, v) ->
        Map.set m ~key:(Var k) ~data:v)
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

type layout =
  | Scalar of Value.t
  | CrossTuple of layout list
  | ZipTuple of layout list
  | UnorderedList of layout list
  | OrderedList of ordered_list
  | Table of table
  | Empty
and range = (PredCtx.Key.t option * PredCtx.Key.t option)
and ordered_list = {
  field : Field.t;
  order : [`Asc | `Desc];
  elems : layout list;
  lookup : range;
}
and table = {
  field : Field.t;
  elems : layout ValueMap.t;
  lookup : PredCtx.Key.t;
}
[@@deriving compare, sexp]

exception TransformError of Error.t

module Schema = struct
  type t = Field.t list [@@deriving compare, sexp]

  let rec of_layout_exn : layout -> t =
    function
    | Empty -> []
    | Scalar v -> [v.field]
    | ZipTuple ls ->
      List.concat_map ls ~f:of_layout_exn
      |> List.dedup ~compare:Field.compare
    | CrossTuple ls ->
      List.map ls ~f:of_layout_exn
      |> List.fold_left1_exn ~f:(fun s s' ->
          let s = List.merge ~cmp:Field.compare s s' in
          let has_dups =
            List.find_consecutive_duplicate s ~equal:Field.(=)
            |> Option.is_some
          in
          if has_dups then
            raise (TransformError
                     (Error.of_string "Cannot cross streams with the same field."));
          s)
    | UnorderedList ls | OrderedList { elems = ls } ->
      List.map ls ~f:of_layout_exn |> List.all_equal_exn
    | Table { field = f; elems = m } ->
      let v_schema =
        Map.data m |> List.map ~f:of_layout_exn |> List.all_equal_exn
      in
      if not (List.mem ~equal:Field.(=) v_schema f) then
        raise (TransformError (Error.of_string "Table values must contain the table key."));
      List.merge ~cmp:Field.compare [f] v_schema

  let of_tuple : Tuple.t -> t = List.map ~f:(fun v -> v.Value.field)

  let has_field : t -> Field.t -> bool = List.mem ~equal:Field.(=)

  let overlaps : t list -> bool = fun schemas ->
    let schemas = List.map schemas ~f:(Set.of_list (module Field)) in
    let tot = List.sum (module Int) schemas ~f:Set.length in
    let utot =
      schemas
      |> Set.union_list (module Field)
      |> Set.length
    in
    tot > utot
end

let rec field : Field.t -> Tuple.t -> Value.t = fun f t ->
  List.find_exn t ~f:(fun x -> Field.(=) f x.field)

let rec ntuples : layout -> int =
  function
  | Empty -> 0
  | Scalar _ -> 1
  | CrossTuple ls -> List.fold_left ls ~init:1 ~f:(fun p l -> p * ntuples l)
  | ZipTuple ls -> List.map ls ~f:ntuples |> List.all_equal_exn
  | UnorderedList ls
  | OrderedList { elems = ls } ->
    List.fold_left ls ~init:0 ~f:(fun p l -> p + ntuples l)
  | Table { elems = m } ->
    Map.data m |> List.map ~f:ntuples |> List.all_equal_exn

(* let rec slice_l lo hi acc = function
 *   | x::xs ->
 *     let nt = ntuples x in
 *     let acc = if lo < nt then slice lo (min hi nt) x :: acc else acc in
 *     slice_l (lo - nt) (hi - nt) acc xs
 *   | [] -> acc
 * 
 * and slice_t lo hi acc = function
 *   | [] -> acc
 *   | l::ls ->
 *     let nrest =
 *       List.fold_left ls ~init:1 ~f:(fun x l -> x * (ntuples l))
 *     in
 *     let lo' = lo / nrest in
 *     let hi' = hi / nrest + 1 in
 *     let 
 *     slice_l (slice lo' hi' l :: acc)
 * 
 * and slice : int -> int -> layout -> layout =
 *   fun lo hi l ->
 *     let nt = ntuples l in
 *     if lo < 0 || hi < 0 || lo > hi || hi - lo > nt then
 *       failwith "Out of bounds slice.";
 *     match l with
 *     | Scalar v as l ->
 *       if lo = 0 && hi = 1 then l else failwith "Out of bounds slice."
 *     | CrossTuple ls ->
 *       begin match ls with
 *         | [] -> failwith "Bad layout."
 *         | [l] -> slice lo hi l
 *         | l::ls ->
 *           let nfirst = ntuples l in
 *           let nrest =
 *             List.fold_left ls ~init:1 ~f:(fun x l -> x * (ntuples l))
 *           in
 *           let lo' = lo / nrest in
 *           let hi' = hi / nrest in
 *           slice lo' hi' l
 *       end
 *     | ZipTuple ls -> ZipTuple (List.map ~f:(slice lo hi) ls)
 *     | UnorderedList ls -> UnorderedList (slice_l lo hi [] ls)
 *     | OrderedList (f,o,ls) -> OrderedList (f, o, slice_l lo hi [] ls)
 *     | Table (f, m) -> Table (f, Map.map m ~f:(slice lo hi)) *)

module Ralgebra = struct
  module T = struct
    type op =
      | Eq
      | Lt
      | Le
      | Gt
      | Ge
      | And
      | Or
    [@@deriving compare, sexp]

    type pred =
      | Var of string
      | Field of Field.t
      | Binop of (op * pred * pred)
      | Varop of (op * pred list)
    [@@deriving compare, sexp]

    type t =
      | Project of Field.t list * t
      | Filter of pred * t
      | EqJoin of Field.t * Field.t * t * t
      | Scan of layout
      | Concat of t list
      | Relation of Relation.t
    [@@deriving compare, sexp]
  end
  include T
  include Comparable.Make(T)

  let rec pred_fields : pred -> Field.t list = function
    | Var _ -> []
    | Field f -> [f]
    | Binop (_, p1, p2) -> (pred_fields p1) @ (pred_fields p2)
    | Varop (_, ps) -> List.concat_map ps ~f:pred_fields

  let op_to_string : op -> string = function
    | Eq -> "="
    | Lt -> "<"
    | Le -> "<="
    | Gt -> ">"
    | Ge -> ">="
    | And -> "And"
    | Or -> "Or"

  let rec pred_to_string : pred -> string = function
    | Var v -> v
    | Field f -> f.name
    | Binop (op, p1, p2) ->
      let os = op_to_string op in
      let s1 = pred_to_string p1 in
      let s2 = pred_to_string p2 in
      sprintf "%s %s %s" s1 os s2
    | Varop (op, ps) ->
      let ss =
        List.map ps ~f:pred_to_string
        |> String.concat ~sep:", "
      in
      sprintf "%s(%s)" (op_to_string op) ss

  let rec layouts : t -> layout list = function
    | Project (_, r)
    | Filter (_, r) -> layouts r
    | EqJoin (_,_, r1, r2) -> layouts r1 @ layouts r2
    | Scan l -> [l]
    | Concat rs -> List.map rs ~f:layouts |> List.concat
    | Relation _ -> []
end

(* let rec eval : expr -> primvalue = function
 *   | Value v -> v.value
 *   | Const v -> v
 *   | Binop (op, e1, e2) ->
 *     let v1 = eval e1 in
 *     let v2 = eval e2 in
 *     begin match op, v1, v2 with
 *       | (Eq, `Bool x, `Bool y) -> `Bool (Bool.equal x y)
 *       | (Eq, `Int x, `Int y) -> `Bool (Int.equal x y)
 *       | (Eq, `String x, `String y) -> `Bool (String.equal x y)
 *       | (And, `Bool x, `Bool y) -> `Bool (x && y)
 *       | (Leq, `Int x, `Int y) -> `Bool (x <= y)
 *       | _ -> failwith "Unexpected expr."
 *     end *)

module Ctx = struct
  type t = {
    mutable conn : Postgresql.connection option;
    mutable testctx : PredCtx.t option;
  }

  let global = { conn = None; testctx = None }

  let conn : unit -> Postgresql.connection = fun () ->
    Option.value_exn global.conn

  let testctx = fun () ->
    Option.value_exn global.testctx
end

exception EvalError of Error.t

let rec eval_pred : PredCtx.t -> Ralgebra.pred -> primvalue =
  fun ctx -> function
    | Var n ->
      begin match Map.find ctx (PredCtx.Key.Var n) with
        | Some v -> v
        | None -> failwith "Unbound variable."
      end
    | Field f ->
      begin match Map.find ctx (PredCtx.Key.Field f) with
        | Some v -> v
        | None -> failwith "Unbound variable."
      end
    | Binop (op, p1, p2) ->
      let v1 = eval_pred ctx p1 in
      let v2 = eval_pred ctx p2 in
      begin match op, v1, v2 with
        | Eq, `Bool x1, `Bool x2 -> Polymorphic_compare.(`Bool (x1 = x2))
        | Eq, `Int x1, `Int x2 -> Polymorphic_compare.(`Bool (x1 = x2))
        | Lt, `Int x1, `Int x2 -> `Bool (x1 < x2)
        | Le, `Int x1, `Int x2 -> `Bool (x1 <= x2)
        | Gt, `Int x1, `Int x2 -> `Bool (x1 > x2)
        | Ge, `Int x1, `Int x2 -> `Bool (x1 >= x2)
        | _ -> failwith "Unexpected argument types."
      end
    | Varop (op, ps) ->
      let vs = List.map ps ~f:(eval_pred ctx) in
      begin match op with
        | And ->
          List.for_all vs ~f:(function
              | `Bool x -> x
              | _ -> failwith "Unexpected argument type.")
          |> fun x -> `Bool x
        | Or ->
          List.exists vs ~f:(function
              | `Bool x -> x
              | _ -> failwith "Unexpected argument type.")
          |> fun x -> `Bool x
        | _ -> failwith "Unexpected argument types."
      end

let rec eval_layout : PredCtx.t -> layout -> Tuple.t Seq.t =
  fun ctx -> function
    | Scalar v -> Seq.singleton [v]
    | CrossTuple ls ->
      List.fold_left ls ~init:(Seq.singleton []) ~f:(fun ts l ->
          Seq.cartesian_product ts (eval_layout ctx l)
          |> Seq.map ~f:(fun (ts, v) -> List.rev v @ ts))
      |> Seq.map ~f:List.rev
    | ZipTuple ls ->
      List.map ls ~f:(eval_layout ctx)
      |> Seq.zip_many
      |> Seq.map ~f:Tuple.merge_many
    | UnorderedList ls
    | OrderedList { elems = ls } ->
      Seq.concat_map ~f:(eval_layout ctx) (Seq.of_list ls)
    | Table { lookup = k; field = f; elems = ls } ->
      begin match Map.find ctx k with
        | Some v ->
          begin match Map.find ls (ValueMap.Elem.of_primvalue v) with
            | Some l -> eval_layout ctx l
            | None -> Seq.empty
          end
        | None -> raise (EvalError (Error.create "Missing key." (k, ctx) [%sexp_of:(PredCtx.Key.t * PredCtx.t)]))
      end
    | Empty -> Seq.empty

let eval_relation : Relation.t -> Tuple.t Seq.t =
  fun r ->
    exec ~verbose:false (Ctx.conn ()) "select * from $0" ~params:[r.name]
    |> Seq.of_list
    |> Seq.mapi ~f:(fun i vs ->
        List.map2_exn vs r.fields ~f:(fun v f ->
            let pval =
              match f.Field.dtype with
              | DInt _ -> `Int (Int.of_string v)
              | DString _ -> `String v
              | DBool _ ->
                begin match v with
                  | "t" -> `Bool true
                  | "f" -> `Bool false
                  | _ -> failwith "Unknown boolean value."
                end
              | DTimestamp _
              | DInterval _ -> `Unknown v
            in
            let value = Value.({ rel = r; field = f; idx = i; value = pval }) in
            value))

let eval : PredCtx.t -> Ralgebra.t -> Tuple.t Seq.t =
  fun ctx r ->
    let open Ralgebra in
    let rec eval = function
      | Scan l -> eval_layout ctx l
      | Project (fs, r) ->
        eval r
        |> Seq.map ~f:(fun t ->
            List.filter t ~f:(fun v ->
                List.mem ~equal:Field.(=) fs v.Value.field))
      | Filter (p, r) ->
        eval r
        |> Seq.filter ~f:(fun t ->
            let ctx = Map.merge_right ctx (PredCtx.of_tuple t) in
            match eval_pred ctx p with
            | `Bool x -> x
            | _ -> failwith "Expected a boolean.")
      | EqJoin (f1, f2, r1, r2) ->
        let m =
          eval r1
          |> Seq.fold ~init:(Map.empty (module Value)) ~f:(fun m t ->
              let v = Tuple.field_exn t f1 in
              Map.add_multi m ~key:v ~data:t)
        in
        eval r2
        |> Seq.concat_map ~f:(fun t ->
            let v = Tuple.field_exn t f2 in
            Option.value ~default:[] (Map.find m v)
            |> Seq.of_list)

      | Concat rs -> Seq.of_list rs |> Seq.concat_map ~f:eval
      | Relation r -> eval_relation r
    in
    eval r

(* let rec filter : predctx -> pred -> layout -> layout =
 *   fun ctx p l -> match l with
 *     | Scalar v ->
 *       let ctx = Map.add ctx ~key:(CtxKey.Field v.field) ~data:v.value in
 *       begin match eval_pred ctx p with
 *         | `Bool true -> l
 *         | `Bool false -> Empty
 *         | _ -> failwith "Expected a bool."
 *       end
 *     | CrossTuple ls ->
 *       
 *     | ZipTuple ls -> failwith ""
 *     | UnorderedList ls -> UnorderedList (List.map ls ~f:(filter p))
 *     | OrderedList (f, o, ls) -> OrderedList (f, o, List.map ls ~f:(filter p))
 *     | Table (f, m) -> failwith "Not sure."
 *     | Empty -> Empty *)

(* let rec project : Layout.Field.t list -> layout -> layout =
 *   fun fs l ->
 *     let f_ok = List.mem fs ~equal:Layout.Field.(=) in
 *     match l with
 *     | Scalar v -> if f_ok v.field then l else Empty
 *     | CrossTuple ls -> ??
 *     | ZipTuple ls -> ZipTuple (List.map ~f:(project fs) ls)
 *     | UnorderedList ls -> UnorderedList (List.map ~f:(project fs) ls)
 *     | OrderedList (f, o, ls) ->
 *       let ls' = List.map ~f:(project fs) ls in
 *       if f_ok f then OrderedList (f, o, ls') else UnorderedList ls'
 *     | Table (f, m) ->
 *       if f_ok f then Table (f, Map.map m ~f:(project fs)) else
 *         UnorderedList (Map.map m ~f:(project fs) |> Map.data)
 *     | Empty -> Empty *)

let rec partition : PredCtx.Key.t -> Field.t -> layout -> layout =
  let p_scalar k f v =
    if Field.(f = v.Value.field) then
      Table {
        field = f;
        elems = Map.singleton (module ValueMap.Elem) v (Scalar v);
        lookup = k
      }
    else Scalar v
  in

  let p_crosstuple k f ls =
    let tbls, others =
      List.mapi ls ~f:(fun i l -> (i, partition k f l))
      |> List.partition_tf ~f:(function (_, Table _) -> true | _ -> false)
    in
    match tbls with
    | [] -> CrossTuple (List.map others ~f:(fun (_, l) -> l))
    | [(i, Table { field = f; elems = m})] ->
      Table {
        field = f;
        elems = Map.map m ~f:(fun l ->
          List.fmerge ~cmp:Int.compare [(i, l)] others
          |> List.map ~f:snd
          |> fun x -> CrossTuple x
          );
        lookup = k;
      }
    | _ -> raise (TransformError (Error.of_string "Bad schema."))
  in

  let p_ziptuple k f ls =
    let all_have_f =
      List.for_all ls ~f:(fun l ->
          List.mem ~equal:Field.(=) (Schema.of_layout_exn l) f)
    in
    if all_have_f then
      List.map ls ~f:(partition k f)
      |> List.fold_left ~init:(Map.empty (module ValueMap.Elem))
        ~f:(fun m -> function
            | Table {field = f'; elems = m'} when Field.(=) f f' ->
              Map.merge m' m ~f:(fun ~key -> function
                  | `Both (l, ls) -> Some (l::ls)
                  | `Left l -> Some [l]
                  | `Right ls -> Some ls)
            | l -> Map.map m ~f:(fun ls -> l::ls))
      |> Map.map ~f:(fun ls -> ZipTuple ls)
      |> fun m -> Table {field = f; elems = m; lookup = k}
    else raise (TransformError (Error.of_string "Must have partition field in all positions."))
  in

  let p_unorderedlist k f ls =
    List.map ls ~f:(partition k f)
    |> List.fold_left ~init:(Map.empty (module ValueMap.Elem))
      ~f:(fun m -> function
          | Table {field = f'; elems = m'} when Field.(=) f f' ->
            Map.merge m' m ~f:(fun ~key -> function
                | `Both (l, ls) -> Some (l::ls)
                | `Left l -> Some [l]
                | `Right ls -> Some ls)
          | l -> Map.map m ~f:(fun ls -> l::ls))
    |> Map.map ~f:(fun ls -> UnorderedList (List.rev ls))
    |> fun m -> Table {field = f; elems = m; lookup = k}
  in

  let p_orderedlist k f ordered_list =
    let tbls, others =
      ordered_list.elems
      |> List.mapi ~f:(fun i l -> (i, partition k f l))
      |> List.partition_tf ~f:(function (_, Table _) -> true | _ -> false)
    in
    match tbls with
    | [] -> failwith "Expected a table."
    | ts ->
      let merged_ts =
        List.fold_left ts ~init:(Map.empty (module ValueMap.Elem))
          ~f:(fun m -> function
              | i, Table { elems = m' } ->
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
          |> fun x -> OrderedList { ordered_list with elems = x })
      |> fun m -> Table { field = f; elems = m; lookup = k }
  in

  let p_table k f (table: table) =
    Map.map table.elems ~f:(fun l -> partition k f l)
    |> Map.fold ~init:(Map.empty (module ValueMap.Elem))
      ~f:(fun ~key ~data m_outer ->
          match data with
          | Table { elems = m_inner } ->
            Map.merge
              (Map.map m_inner ~f:(fun v ->
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
    |> Map.map ~f:(fun m -> Table { table with elems = m })
    |> fun m -> Table { field = f; elems = m; lookup = k }
  in

  fun k f l ->
    if Schema.has_field (Schema.of_layout_exn l) f then
      match l with
      | Empty -> Empty
      | Scalar x -> p_scalar k f x
      | Table { field = f' } when Field.(=) f f' -> l
      | Table x -> p_table k f x
      | CrossTuple x -> p_crosstuple k f x
      | ZipTuple x -> p_ziptuple k f x
      | UnorderedList x -> p_unorderedlist k f x
      | OrderedList x -> p_orderedlist k f x
    else l

let flatten : layout -> layout = function
  | Scalar _ | Table _ | Empty as l -> l
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
  | OrderedList { field = f; order = ord; elems = ls } ->
    List.concat_map ls ~f:(function
        | OrderedList { field = f'; order = ord'; elems = ls' } as l ->
          if Base.Polymorphic_compare.equal (f, ord) (f', ord')
          then ls' else [l]
        | l -> [l])
    |> fun x -> UnorderedList x

let rec order : range -> Field.t -> [`Asc | `Desc] -> layout -> layout =
  fun k f o l ->
    let cmp = match o with
      | `Asc -> Value.compare
      | `Desc -> fun k1 k2 -> Value.compare k2 k1
    in
    if List.exists (Schema.of_layout_exn l) ~f:(Field.(=) f) then
      match partition PredCtx.Key.dummy f l with
      | Table { elems = m } ->
        Map.to_alist m
        |> List.sort ~cmp:(fun (k1, _) (k2, _) -> cmp k1 k2)
        |> List.map ~f:(fun (k, v) -> CrossTuple [Scalar k; v])
        |> fun ls -> OrderedList { field = f; order = o; elems = ls; lookup = k }
      | _ -> raise (TransformError (Error.of_string "Expected a table."))
    else l

let merge : range -> Field.t -> [`Asc | `Desc] -> layout -> layout -> layout =
  fun k f o l1 l2 ->
    let cmp = match o with
      | `Asc -> Value.compare
      | `Desc -> fun k1 k2 -> Value.compare k2 k1
    in
    match partition PredCtx.Key.dummy f l1, partition PredCtx.Key.dummy f l2 with
    | Table { elems = m1 }, Table { elems = m2 } ->
      Map.merge m1 m2 ~f:(fun ~key -> function
          | `Both (l1, l2) -> Some (UnorderedList ([l1; l2]))
          | `Left l | `Right l -> Some l)
      |> Map.to_alist
      |> List.sort ~cmp:(fun (k1, _) (k2, _) -> cmp k1 k2)
      |> List.map ~f:(fun (k, v) -> CrossTuple [Scalar k; v])
      |> fun ls -> OrderedList { field = f; order = o; elems = ls; lookup = k }
    | _ -> raise (TransformError (Error.of_string "Expected a table."))

let project : Field.t list -> layout -> layout = fun fs l ->
  let f_in = List.mem ~equal:Field.equal fs in
  let rec project = function
    | Scalar v -> if f_in v.field then Scalar v else Empty
    | CrossTuple ls -> CrossTuple (List.map ls ~f:project)
    | ZipTuple ls ->
      let ls' =
        List.map ls ~f:project
        |> List.filter ~f:(fun l -> ntuples l > 0)
      in
      ZipTuple ls'
    | UnorderedList ls -> UnorderedList (List.map ls ~f:project)
    | OrderedList ({ field = f; elems = ls } as x) ->
      let ls' = List.map ls ~f:project in
      if f_in f then OrderedList { x with elems = ls' } else UnorderedList ls'
    | Table ({ field = f; elems = ls } as x) ->
      let ls' = Map.map ls ~f:project in
      if f_in f then Table { x with elems = ls' }
      else UnorderedList (Map.data ls')
    | Empty -> Empty
  in
  project l

let op_to_string : Ralgebra.op -> string = function
  | Eq -> "="
  | Le -> "<="
  | Ge -> ">="
  | Lt -> "<"
  | Gt -> ">"
  | And -> "&"
  | Or -> "|"

(* let rec expr_to_string : expr -> string = function
 *   | Value v -> sprintf "%s.%s[%d]" v.rel.name v.field.name v.idx
 *   | Const v -> sexp_of_primvalue v |> Sexp.to_string_hum
 *   | Binop (op, e1, e2) ->
 *     sprintf "(%s %s %s)"
 *       (op_to_string op) (expr_to_string e1) (expr_to_string e2) *)

(* module Infix = struct
 *   let (<=) = fun x y -> Binop (Leq, x, y)
 *   let (&&) = fun x y -> Binop (And, x, y)
 *   let i = fun x -> Const (`Int x)
 *   let v = fun x -> Value x
 * end *)

(* let filter : stream -> (Value.t list -> expr) -> stream =
 *   fun seq f ->
 *     Seq.filter_map seq ~f:(fun (t, e) ->
 *         let cond = f t in
 *         match eval cond with
 *         | `Bool true -> Some (t, e)
 *         | `Bool false -> None
 *         | _ -> failwith "Expected a boolean.")
 * 
 * let join : stream -> stream -> (Value.t list -> Value.t list -> expr) -> stream =
 *   fun seq1 seq2 f ->
 *     Seq.cartesian_product seq1 seq2
 *     |> Seq.filter_map ~f:(fun ((t1, e1), (t2, e2)) ->
 *         let cond = f t1 t2 in
 *         match eval cond with
 *         | `Bool true -> Some (t1 @ t2, e1 @ e2)
 *         | `Bool false -> None
 *         | _ -> failwith "Expected a boolean.")
 * 
 * let intra_tuple_locality : stream -> float =
 *   let rec tuples : expr -> int list = function
 *     | Value v -> [v.idx]
 *     | Const _ -> []
 *     | Binop (_,e1,e2) -> tuples e1 @ tuples e2
 *   in
 *   fun s ->
 *     let (n, d) =
 *       Seq.map s ~f:(fun (v, e) -> e)
 *       |> Seq.concat_map ~f:(fun es ->
 *           List.map es ~f:(fun e -> 
 *               tuples e |> List.dedup ~compare:Int.compare |> List.length)
 *           |> Seq.of_list)
 *       |> Seq.fold ~init:(0, 0) ~f:(fun (n, d) t -> (n + t, d + 1))
 *     in
 *     Float.of_int n /. Float.of_int d *)

let rec layout_to_string : layout -> string = function
  | Scalar _ -> "s"
  | ZipTuple ls -> 
    List.map ls ~f:layout_to_string
    |> String.concat ~sep:", "
    |> sprintf "z(%s)"
  | CrossTuple ls ->
    List.map ls ~f:layout_to_string
    |> String.concat ~sep:", "
    |> sprintf "c(%s)"
  | UnorderedList ls | OrderedList { elems = ls } ->
    List.map ls ~f:layout_to_string
    |> List.count_consecutive_duplicates ~equal:String.equal
    |> List.map ~f:(fun (s, c) -> sprintf "%s x %d" s c)
    |> String.concat ~sep:", "
    |> sprintf "[%s]"
  | Table { elems = m } ->
    Map.to_alist m
    |> List.map ~f:(fun (k, v) -> sprintf "k -> %s"  (layout_to_string v))
    |> List.count_consecutive_duplicates ~equal:String.equal
    |> List.map ~f:(fun (s, c) -> sprintf "%s x %d" s c)
    |> String.concat ~sep:", "
    |> sprintf "{%s}"
  | Empty -> "[]"

let rec ralgebra_to_string : Ralgebra.t -> string = function
  | Project (fs, r) -> sprintf "Project(%s)" (ralgebra_to_string r)
  | Filter (p, r) ->
    sprintf "Filter(%s, %s)" (Ralgebra.pred_to_string p) (ralgebra_to_string r)
  | EqJoin (f1, f2, r1, r2) ->
    sprintf "EqJoin(%s, %s)" (ralgebra_to_string r1) (ralgebra_to_string r2)
  | Scan l -> sprintf "Scan(%s)" (layout_to_string l)
  | Concat rs ->
    List.map rs ~f:ralgebra_to_string
    |> String.concat ~sep:", "
    |> sprintf "Concat(%s)"
  | Relation r -> r.name

module Transform = struct
  type t = {
    name : string;
    f : Ralgebra.t -> Ralgebra.t list;
  }

  let run : t -> Ralgebra.t -> Ralgebra.t list = fun t r ->
    try t.f r with TransformError _ -> []

  let run_checked : t -> Ralgebra.t -> Ralgebra.t list = fun t r ->
    try
      let rs = t.f r in
      (* Stdio.printf "INFO: Running %s. Got %d new exprs:\n%s\n" t.name (List.length rs) (ralgebra_to_string r); *)
      List.filter rs ~f:(fun r' ->
          let eval_to_set r =
            eval (Ctx.testctx ()) r
            |> Seq.fold ~init:(Set.empty (module Tuple)) ~f:Set.add
          in
          let s1 = eval_to_set r in
          let s2 = eval_to_set r' in
          if Set.equal s1 s2 then true else begin
            Stdio.printf "ERROR: Transform %s not equivalent.\n%s => %s\n"
              t.name (ralgebra_to_string r) (ralgebra_to_string r');
            Stdio.printf "New relation has %d records, old has %d.\n" (Set.length s2) (Set.length s1);
            false
          end)
    with TransformError e ->
      Stdio.printf "WARN: Transform %s failed: %s.\n" t.name (Error.to_string_hum e);
      []
end

let col_layout : Relation.t -> layout = fun r -> 
  let stream = eval_relation r |> Seq.to_list in
  List.transpose stream
  |> (fun v -> Option.value_exn v)
  |> List.map ~f:(fun col ->
      UnorderedList (List.map col ~f:(fun v -> Scalar v)))
  |> (fun cols -> ZipTuple cols)

let row_layout : Relation.t -> layout = fun r -> 
  eval_relation r
  |> Seq.map ~f:(fun tup ->
      CrossTuple (List.map ~f:(fun v -> Scalar v) tup))
  |> Seq.to_list
  |> fun l -> UnorderedList l

let tf_col_layout : Transform.t = {
  name = "col-layout";
  f = function
    | Relation r -> [Scan (col_layout r)]
    | _ -> []
}

let tf_row_layout : Transform.t = {
  name = "row-layout";
  f = function
    | Relation r -> [Scan (row_layout r)]
    | _ -> []
}

let tf_eq_filter : Transform.t = {
  name = "eq-filter";
  f = function
    | Filter (Binop (Eq, Field f, Var v), Scan l)
    | Filter (Binop (Eq, Var v, Field f), Scan l) ->
      [Scan (partition (Var v) f l)]
    | _ -> []
}

let tf_cmp_filter : Transform.t = {
  name = "cmp-filter";
  f = function
    | Filter (Binop (Gt, Field f, Var v), Scan l)
    | Filter (Binop (Gt, Var v, Field f), Scan l)
    | Filter (Binop (Ge, Field f, Var v), Scan l)
    | Filter (Binop (Ge, Var v, Field f), Scan l)
    | Filter (Binop (Lt, Field f, Var v), Scan l)
    | Filter (Binop (Lt, Var v, Field f), Scan l)
    | Filter (Binop (Le, Field f, Var v), Scan l)
    | Filter (Binop (Le, Var v, Field f), Scan l) ->
      [
        Scan (order (None, None) f `Asc l);
        Scan (order (None, None) f `Desc l);
      ]
    | _ -> []
}

let tf_and_filter : Transform.t = {
  name = "and-filter";
  f = function
    | Filter (Varop (And, ps), q) ->
      Combinat.permutations_poly (Array.of_list ps)
      |> Seq.map ~f:(Array.fold ~init:q ~f:(fun q p -> Filter (p, q)))
      |> Seq.to_list
    | _ -> []
}

let tf_or_filter : Transform.t = {
  name = "or-filter";
  f = function
    | Filter (Varop (Or, ps), q) ->
      [ Concat (List.map ps ~f:(fun p -> Ralgebra.Filter (p, q))) ]
    | _ -> []
}

let tf_eqjoin : Transform.t = {
  name = "eqjoin";
  f = function
    | EqJoin (f1, f2, Scan l1, Scan l2) ->
      [ Filter (Binop (Eq, Field f1, Field f2), Scan (CrossTuple [l1; l2])); ]
    | _ -> []
}

let tf_concat : Transform.t = {
  name = "concat";
  f = function
    | Concat qs ->
      if List.for_all qs ~f:(function Scan _ -> true | _ -> false) then
        [Scan (UnorderedList (List.map qs ~f:(fun (Scan l) -> l)))]
      else []
    | _ -> []
}

let tf_flatten : Transform.t = {
  name = "flatten";
  f = function
    | Scan l -> [Scan (flatten l)]
    | _ -> []
}

let tf_empty_project : Transform.t = {
  name = "empty";
  f = function
    | Project ([], q) -> [q]
    | _ -> []
}

let tf_push_project : Transform.t =
  let open Ralgebra in 
  {
  name = "push-project";
  f = function
    | Project (fs, (Filter (p, q))) ->
      let fs_post = Set.of_list (module Field) fs in
      let fs_pre =
        Set.union fs_post
          (Set.of_list (module Field) (Ralgebra.pred_fields p))
      in
      if Set.equal fs_post fs_pre then
        [ Filter (p, Project (Set.to_list fs_pre, q)) ]
      else
        [ Project (Set.to_list fs_post,
                   Filter (p, Project (Set.to_list fs_pre, q))) ]
    | Project (fs, (Concat qs)) ->
      [ Concat (List.map qs ~f:(fun q -> Project (fs, q))) ]
    | Project (fs, (EqJoin (f1, f2, q1, q2))) ->
      let fs_post = Set.of_list (module Field) fs in
      let fs_pre =
        Set.of_list (module Field) fs
        |> (fun s -> Set.add s f1)
        |> (fun s -> Set.add s f2)
      in
      if Set.equal fs_post fs_pre then
        [ EqJoin (f1, f2, Project (Set.to_list fs_pre, q1),
                  Project (Set.to_list fs_pre, q2)) ]
      else
        [ Project (Set.to_list fs_post,
                   EqJoin (f1, f2, Project (Set.to_list fs_pre, q1),
                           Project (Set.to_list fs_pre, q2))) ]
    | Project (fs, Scan l) -> [Scan (project fs l)]
    | Project (fs_post, Project (fs_pre, q)) ->
      let fs =
        Set.inter
          (Set.of_list (module Field) fs_pre)
          (Set.of_list (module Field) fs_post)
        |> Set.to_list
      in
      [Project (fs, q)]
    | _ -> []
}

let transform : Postgresql.connection -> Ralgebra.t -> Ralgebra.t list =
  let open Ralgebra in
  let tfs = [
    tf_eq_filter;
    tf_cmp_filter;
    tf_and_filter;
    tf_or_filter;
    tf_eqjoin;
    tf_concat;
    tf_row_layout;
    tf_col_layout;
    tf_flatten;
    tf_empty_project;
    tf_push_project;
  ] in
  fun conn r ->
    let rec transform r =
      let rs = List.concat_map tfs ~f:(fun tf -> Transform.run_checked tf r) in
      let rs' = match r with
        | Scan _ | Relation _ -> []
        | Project (fs, r') ->
          List.map (transform r') ~f:(fun r' -> Project (fs, r'))
        | Filter (ps, r') ->
          List.map (transform r') ~f:(fun r' -> Filter (ps, r'))
        | EqJoin (f1, f2, r1, r2) ->
          List.map (transform r1) ~f:(fun r1 -> EqJoin (f1, f2, r1, r2))
          @ List.map (transform r2) ~f:(fun r2 -> EqJoin (f1, f2, r1, r2))
        | Concat rs ->
          List.map rs ~f:transform
          |> List.transpose
          |> (fun x -> Option.value_exn x)
          |> List.map ~f:(fun rs -> Concat rs)
      in
      rs @ rs'
    in
    transform r

let search : Postgresql.connection -> Ralgebra.t -> Ralgebra.t Seq.t =
  fun conn r ->
    let pool = Set.singleton (module Ralgebra) r in
    let rec search pool =
      Stdio.printf "Pool size: %d\n" (Set.length pool);
      let pool' =
        Set.to_list pool
        |> List.concat_map ~f:(transform conn)
        |> Set.of_list (module Ralgebra)
        |> Set.union pool
      in
      if Set.equal pool' pool then pool' else search pool'
    in
    Set.to_sequence (search pool)

let exec2 : ?verbose : bool -> ?params : string list -> Postgresql.connection -> string -> (string * string) list =
  fun ?verbose ?params conn query ->
    exec ?verbose ?params conn query
    |> List.map ~f:(function
        | [x; y] -> (x, y)
        | _ -> failwith "Unexpected query results.")

let relation_from_db : Postgresql.connection -> string -> Relation.t =
  fun conn name ->
    let card =
      exec ~params:[name] conn "select count(*) from $0"
      |> (fun ([ct_s]::_) -> int_of_string ct_s)
    in
    let fields =
      exec2 ~params:[name] conn
        "select column_name, data_type from information_schema.columns where table_name='$0'"
      |> List.map ~f:(fun (field_name, dtype_s) ->
          let distinct =
            exec ~params:[name; field_name] conn "select count(*) from (select distinct $1 from $0) as t"
            |> (fun ([ct_s]::_) -> int_of_string ct_s)
          in
          let dtype = match dtype_s with
            | "character varying" ->
              let [min_bits; max_bits] =
                exec ~params:[field_name; name] conn
                  "select min(l), max(l) from (select bit_length($0) as l from $1) as t"
                |> (fun (t::_) -> List.map ~f:int_of_string t)
              in
              DString { distinct; min_bits; max_bits }
            | "integer" ->
              let [min_val; max_val] =
                exec ~params:[field_name; name] conn "select min($0), max($0) from $1"
                |> (fun (t::_) -> List.map ~f:int_of_string t)
              in
              DInt { distinct; min_val; max_val }
            | "timestamp without time zone" -> DTimestamp { distinct }
            | "interval" -> DInterval { distinct }
            | "boolean" -> DBool { distinct }
            | s -> failwith (sprintf "Unknown dtype %s" s)
          in
          Field.({ name = field_name; dtype }))
    in
    { name; fields; card }


let tests =
  let open OUnit2 in
  let partition_tests =
    let f1 = Field.({ name = "f1"; dtype = DInt { min_val = 0; max_val = 10; distinct = 100; }}) in
    let f2 = Field.({ name = "f2"; dtype = DInt { min_val = 0; max_val = 10; distinct = 100; }}) in
    let r = Relation.({ name = "r"; fields = [f1; f2]; card = 100; }) in
    let assert_equal ~ctxt x y =
      assert_equal ~ctxt ~cmp:(fun a b -> compare_layout a b = 0) x y
    in
    "partition" >::: [
      ("scalar" >:: fun ctxt ->
          let inp_v = Value.({ rel = r; field = f1; idx = 0; value = `Int 0 }) in
          let inp = Scalar inp_v in
          let out = Table { lookup = PredCtx.Key.dummy; field = f1; elems = Map.singleton (module ValueMap.Elem) inp_v inp} in
          assert_equal ~ctxt out (partition PredCtx.Key.dummy f1 inp));
    ]
  in

  "locality" >::: [
    partition_tests
  ]
