open Base
open Printf
open Db
open Type0

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
    | Var of TypedName.t
    | Field of Field.t
    | Binop of (op * pred * pred)
    | Varop of (op * pred list)
  [@@deriving compare, sexp]

  type t =
    | Project of Field.t list * t
    | Filter of pred * t
    | EqJoin of Field.t * Field.t * t * t
    | Scan of Layout.t
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
  | Var v -> TypedName.to_string v
  | Field f -> f.name
  | Binop (op, p1, p2) ->
    let os = op_to_string op in
    let s1 = pred_to_string p1 in
    let s2 = pred_to_string p2 in
    sprintf "%s %s %s" s1 os s2
  | Varop (op, ps) ->
    let ss = List.map ps ~f:pred_to_string |> String.concat ~sep:", " in
    sprintf "%s(%s)" (op_to_string op) ss

let rec to_string : t -> string =
  function
  | Project (fs, r) -> sprintf "Project(%s)" (to_string r)
  | Filter (p, r) -> sprintf "Filter(%s, %s)" (pred_to_string p) (to_string r)
  | EqJoin (f1, f2, r1, r2) ->
    sprintf "EqJoin(%s, %s)" (to_string r1) (to_string r2)
  | Scan l -> sprintf "Scan(%s)" (Layout.to_string l)
  | Concat rs ->
    List.map rs ~f:to_string |> String.concat ~sep:", " |> sprintf "Concat(%s)"
  | Relation r -> r.name

let rec layouts : t -> Layout.t list = function
  | Project (_, r)
  | Filter (_, r) -> layouts r
  | EqJoin (_,_, r1, r2) -> layouts r1 @ layouts r2
  | Scan l -> [l]
  | Concat rs -> List.map rs ~f:layouts |> List.concat
  | Relation _ -> []

let rec to_schema : t -> Schema.t = function
  | Project (fs, r) ->
    to_schema r |> List.filter ~f:(List.mem fs ~equal:Field.(=))
  | Filter (_, r) -> to_schema r
  | EqJoin (_, _, r1, r2) -> to_schema r1 @ to_schema r2
  | Scan l -> Layout.to_schema_exn l
  | Concat rs -> List.concat_map rs ~f:to_schema
  | Relation r -> Schema.of_relation r

let rec params : t -> Set.M(TypedName).t =
  let empty = Set.empty (module TypedName) in
  let union_list = Set.union_list (module TypedName) in
  function
  | Project (_, _) | Relation _ -> empty
  | Scan l -> Layout.params l
  | Filter (p, r) ->
    let rec pred_params = function
      | Field _ -> empty
      | Var v -> Set.singleton (module TypedName) v
      | Binop (_, p1, p2) -> Set.union (pred_params p1) (pred_params p2)
      | Varop (_, ps) -> List.map ~f:pred_params ps |> union_list
    in
    Set.union (pred_params p) (params r)
  | EqJoin (_, _, r1, r2) -> Set.union (params r1) (params r2)
  | Concat rs -> List.map ~f:params rs |> union_list 
  
