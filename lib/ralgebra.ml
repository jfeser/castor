open Base
open Printf
open Db
open Type0

module T = struct
  type op = Ralgebra0.op [@@deriving compare, sexp]
  type pred = Field.t Ralgebra0.pred [@@deriving compare, sexp]
  type t = (Field.t, Relation.t) Ralgebra0.t [@@deriving compare, sexp]
end
include T
include Comparable.Make(T)

let rec relations : ('f, 'r) Ralgebra0.t -> 'r list = function
  | Project (_, r)
  | Filter (_, r) -> relations r
  | EqJoin (_, _, r1, r2) -> relations r1 @ relations r2
  | Scan _ -> []
  | Concat rs -> List.concat_map rs ~f:relations
  | Relation r -> [r]

let resolve : Postgresql.connection -> (string * string, string) Ralgebra0.t
  -> t = fun conn ralgebra ->
  let rels =
    relations ralgebra
    |> List.dedup_and_sort
    |> List.map ~f:(fun r -> (r, Relation.from_db conn r))
    |> Map.of_alist_exn (module String)
  in
  let open Ralgebra0 in
  let resolve_relation = Map.find_exn rels in
  let resolve_field (r, f) = Relation.field_exn (resolve_relation r) f in
  let rec resolve_pred = function
    | Var _ as p -> p
    | Field f -> Field (resolve_field f)
    | Binop (op, p1, p2) -> Binop (op, resolve_pred p1, resolve_pred p2)
    | Varop (op, ps) -> Varop (op, List.map ps ~f:resolve_pred)
  in
  let rec resolve = function
    | Project (fs, r) -> Project (List.map ~f:resolve_field fs, resolve r)
    | Filter (p, r) -> Filter (resolve_pred p, resolve r)
    | Concat rs -> Concat (List.map rs ~f:resolve)
    | Relation r -> Relation (resolve_relation r)
    | Scan _ as r -> r
    | EqJoin (f1, f2, r1, r2) ->
      EqJoin (resolve_field f1, resolve_field f2, resolve r1, resolve r2)
  in
  resolve ralgebra

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

let of_string_exn : string -> (string * string, string) Ralgebra0.t = fun s ->
  let lexbuf = Lexing.from_string s in
  try Ralgebra_parser.ralgebra_eof Ralgebra_lexer.token lexbuf with e ->
    let cnum = lexbuf.lex_curr_p.pos_cnum in
    let tok = Lexing.lexeme lexbuf in
    Error.(tag_arg (of_exn e) "Parse error." (s, cnum, tok)
             [%sexp_of:string * int * string] |> raise)

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
  | Relation _ -> empty
  | Scan l -> Layout.params l
  | Project (_, r) -> params r
  | Filter (p, r) ->
    let open Ralgebra0 in
    let rec pred_params = function
      | Field _ -> empty
      | Var v -> Set.singleton (module TypedName) v
      | Binop (_, p1, p2) -> Set.union (pred_params p1) (pred_params p2)
      | Varop (_, ps) -> List.map ~f:pred_params ps |> union_list
    in
    Set.union (pred_params p) (params r)
  | EqJoin (_, _, r1, r2) -> Set.union (params r1) (params r2)
  | Concat rs -> List.map ~f:params rs |> union_list

let tests =
  let open OUnit2 in
  let parse_test s = s >:: fun _ ->
      try of_string_exn s |> ignore
      with e -> assert_failure (Exn.to_string e)
  in
  "ralgebra" >::: [
    "of_string" >::: [
      parse_test "Filter(taxi.xpos > xv:int, taxi)";
      parse_test "Filter(taxi.xpos > xv:int && taxi.xpos > xv:int, taxi)";
      parse_test "Filter(taxi.xpos > xv:int && taxi.ypos > yv:int, taxi)";
    ]
  ]
