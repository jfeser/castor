open Base
open Printf
open Db
open Type0

open Ralgebra0

module T = struct
  type op = Ralgebra0.op [@@deriving compare, sexp]
  type pred = Field.t Ralgebra0.pred [@@deriving compare, sexp]
  type t = (Field.t, Relation.t, Layout.t) Ralgebra0.t [@@deriving compare, sexp]
end
include T
include Comparable.Make(T)

module Binable = struct
  type ralgebra = t
  type t = (Field.t, Relation.t, Layout.Binable.t) Ralgebra0.t [@@deriving bin_io, hash]

  let rec of_ralgebra : ralgebra -> t = function
    | Project (fs, r) -> Project (fs, of_ralgebra r)
    | Filter (p, r) -> Filter (p, of_ralgebra r)
    | Concat rs -> Concat (List.map rs ~f:of_ralgebra)
    | Relation r -> Relation r
    | Scan l -> Scan (Layout.Binable.of_layout l)
    | Count r -> Count (of_ralgebra r)
    | EqJoin (f1, f2, r1, r2) -> EqJoin (f1, f2, of_ralgebra r1, of_ralgebra r2)

  let rec to_ralgebra : t -> ralgebra = function
    | Project (fs, r) -> Project (fs, to_ralgebra r)
    | Filter (p, r) -> Filter (p, to_ralgebra r)
    | Concat rs -> Concat (List.map rs ~f:to_ralgebra)
    | Relation r -> Relation r
    | Scan l -> Scan (Layout.Binable.to_layout l)
    | Count r -> Count (to_ralgebra r)
    | EqJoin (f1, f2, r1, r2) -> EqJoin (f1, f2, to_ralgebra r1, to_ralgebra r2)
end

let rec relations : ('f, 'r, 'l) Ralgebra0.t -> 'r list = function
  | Project (_, r)
  | Filter (_, r)
  | Count r -> relations r
  | EqJoin (_, _, r1, r2) -> relations r1 @ relations r2
  | Scan _ -> []
  | Concat rs -> List.concat_map rs ~f:relations
  | Relation r -> [r]

let predicates : ('f, 'r, 'l) Ralgebra0.t -> 'f Ralgebra0.pred list = 
  let rec f = function
    | Scan _ | Relation _ -> []
    | Project (_, r) | Count r -> f r
    | EqJoin (_, _, r1, r2) -> f r1 @ f r2
    | Concat rs -> List.concat_map rs ~f
    | Filter (p, r) -> p::(f r)
  in
  f

let resolve : Postgresql.connection -> (string * string, string, Layout.t) Ralgebra0.t
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
    | Var _ | Int _ | Bool _ | String _ as p -> p
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
    | Count r -> Count (resolve r)
    | EqJoin (f1, f2, r1, r2) ->
      EqJoin (resolve_field f1, resolve_field f2, resolve r1, resolve r2)
  in
  resolve ralgebra

let rec pred_fields : pred -> Field.t list = function
  | Var _ | Int _ | Bool _ | String _ -> []
  | Field f -> [f]
  | Binop (_, p1, p2) -> (pred_fields p1) @ (pred_fields p2)
  | Varop (_, ps) -> List.concat_map ps ~f:pred_fields

let rec pred_params : pred -> Set.M(TypedName).t = function
  | Field _ | Int _ | Bool _ | String _ -> Set.empty (module TypedName)
  | Var v -> Set.singleton (module TypedName) v
  | Binop (_, p1, p2) -> Set.union (pred_params p1) (pred_params p2)
  | Varop (_, ps) ->
    List.map ~f:pred_params ps
    |> List.fold_left ~init:(Set.empty (module TypedName)) ~f:Set.union

(** Collect predicates which apply to the leaf relations and must be satisfied. *)
let required_predicates : t -> pred list Map.M(Relation).t = fun r ->
  let preds = predicates r in
  let rels = relations r |> List.map ~f:(fun r ->
      r, Set.of_list (module Field) r.Relation.fields)
  in
  List.concat_map preds ~f:(function
      | Binop (And, p1, p2) -> [p1; p2]
      | Varop (And, ps) -> ps
      | p -> [p])
  |> List.filter ~f:(fun p -> Int.(Set.length (pred_params p) = 0))
  |> List.filter_map ~f:(fun p ->
      let fs = pred_fields p |> Set.of_list (module Field) in
      match List.find rels ~f:(fun (_, fs') -> Set.is_subset fs ~of_:fs') with
      | Some (r, _) -> Some (r, p)
      | None -> None)
  |> Map.of_alist_multi (module Relation)

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
  | Int x -> Int.to_string x
  | Bool x -> Bool.to_string x
  | String x -> String.escaped x
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
  | Project (fs, r) ->
    sprintf "Project([%s], %s)"
      (List.map fs ~f:(fun f -> f.name) |> String.concat ~sep:", ") (to_string r)
  | Count r -> sprintf "Count(%s)" (to_string r)
  | Filter (p, r) -> sprintf "Filter(%s, %s)" (pred_to_string p) (to_string r)
  | EqJoin (f1, f2, r1, r2) ->
    sprintf "EqJoin(%s, %s, %s, %s)" f1.name f2.name (to_string r1) (to_string r2)
  | Scan l -> sprintf "Scan(%s)"
                (Type.of_layout_exn l |> [%sexp_of:Type.t] |> Sexp.to_string_hum)
  | Concat rs ->
    List.map rs ~f:to_string |> String.concat ~sep:", " |> sprintf "Concat(%s)"
  | Relation r -> r.name

let of_string_exn : string -> (string * string, string, 'l) Ralgebra0.t = fun s ->
  let lexbuf = Lexing.from_string s in
  try Ralgebra_parser.ralgebra_eof Ralgebra_lexer.token lexbuf with
  | Ralgebra0.ParseError (msg, line, col) as e ->
    Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col);
    raise e

let rec layouts : t -> Layout.t list = function
  | Project (_, r)
  | Filter (_, r)
  | Count r -> layouts r
  | EqJoin (_,_, r1, r2) -> layouts r1 @ layouts r2
  | Scan l -> [l]
  | Concat rs -> List.map rs ~f:layouts |> List.concat
  | Relation _ -> []

let rec to_schema : t -> Schema.t = function
  | Project (fs, r) ->
    to_schema r |> List.filter ~f:(List.mem fs ~equal:Field.(=))
  | Count _ -> [Field.{ name = "count"; dtype = DInt }]
  | Filter (_, r) -> to_schema r
  | EqJoin (_, _, r1, r2) -> to_schema r1 @ to_schema r2
  | Scan l -> Layout.to_schema_exn l
  | Concat rs -> List.concat_map rs ~f:to_schema
  | Relation r -> Schema.of_relation r

let rec flatten : t -> t =
  function
  | Filter (_, r) | Project (_, r) as x -> begin match flatten r with
      | Scan { node = Empty } -> Scan Layout.empty
      | _ -> x
    end
  | Count r as x -> begin match flatten r with
      | Scan { node = Empty } ->
        Scan Layout.(int 0 (scalar Relation.dummy Field.dummy))
      | _ -> x
    end
  | EqJoin (_, _, r1, r2) as x -> begin match flatten r1, flatten r2 with
      | Scan { node = Empty }, _ | _, Scan { node = Empty } -> Scan Layout.empty
      | _ -> x
    end
  | Concat rs -> Concat (List.filter_map rs ~f:(fun r -> match flatten r with
      | Scan { node = Empty } -> None
      | r' -> Some r' ))
  | Scan l -> Scan (Layout.flatten l)
  | Relation _ as x -> x

let rec params : t -> Set.M(TypedName).t =
  let empty = Set.empty (module TypedName) in
  let union_list = Set.union_list (module TypedName) in
  function
  | Relation _ -> empty
  | Scan l -> Layout.params l
  | Project (_, r) | Count r -> params r
  | Filter (p, r) ->
    let open Ralgebra0 in
    Set.union (pred_params p) (params r)
  | EqJoin (_, _, r1, r2) -> Set.union (params r1) (params r2)
  | Concat rs -> List.map ~f:params rs |> union_list

let replace_relation : Relation.t -> Relation.t -> t -> t = fun r1 r2 ->
  let open Ralgebra0 in
  let rec rep = function
    | Project (x, r) -> Project (x, rep r)
    | Filter (x, r) -> Filter (x, rep r)
    | EqJoin (x, y, r, r') -> EqJoin (x, y, rep r, rep r')
    | Scan _ as x -> x
    | Concat rs -> Concat (List.map ~f:rep rs)
    | Relation r as x -> if Relation.(r = r1) then Relation r2 else x
    | Count r -> Count (rep r)
  in
  rep

let rec relations : t -> Relation.t list =
  let rec f =
    let open Ralgebra0 in
    function
    | Scan _ -> []
    | Project (_, r)
    | Filter (_, r)
    | Count r -> f r
    | EqJoin (_, _, r, r') -> f r @ f r'
    | Concat rs -> List.concat_map ~f:f rs
    | Relation r -> [r]
  in
  fun r -> f r |> List.dedup_and_sort ~compare:Relation.compare

let rec layouts : t -> Layout.t list =
  let rec f =
    let open Ralgebra0 in
    function
    | Scan l -> [l]
    | Project (_, r)
    | Filter (_, r)
    | Count r -> f r
    | EqJoin (_, _, r, r') -> f r @ f r'
    | Concat rs -> List.concat_map ~f:f rs
    | Relation _ -> []
  in
  fun r -> f r

let intro_project : t -> t = fun r ->
  let open Ralgebra0 in
  let rec f fields = function
    | Scan l -> Scan (Layout.project (Set.to_list fields) l)
    | Project (fs, r) ->
      let fields = Set.of_list (module Field) fs in
      Project (fs, f fields r)
    | Filter (p, r) ->
      let fields =
        Set.union fields (Set.of_list (module Field) (pred_fields p))
      in
      Filter (p, f fields r)
    | Count r -> Count (f (Set.empty (module Field)) r)
    | EqJoin (f1, f2, r1, r2) ->
      EqJoin (f1, f2, f (Set.add fields f1) r1, f (Set.add fields f2) r2)
    | Concat rs -> Concat (List.map rs ~f:(f fields))
    | Relation _ as r -> r
  in
  f (Set.of_list (module Field) (to_schema r)) r

let push_filter : t -> t =
  let open Ralgebra0 in
  let rec f = function
    | Filter (p, EqJoin (f1, f2, r1, r2)) ->
      let fields = Set.of_list (module Field) (pred_fields p) in
      let r1, pushed_r1 =
        if Set.is_subset fields ~of_:(Set.of_list (module Field) (to_schema r1)) then
          f (Filter (p, r1)), true else f r1, false
      in
      let r2, pushed_r2 =
        if Set.is_subset fields ~of_:(Set.of_list (module Field) (to_schema r2)) then
          f (Filter (p, r2)), true else f r2, false
      in
      if pushed_r1 || pushed_r2 then EqJoin (f1, f2, r1, r2) else
        Filter (p, EqJoin (f1, f2, r1, r2))
    | Filter (p, Project (fs, r)) -> Project (fs, f (Filter (p, r)))
    | Filter (p, Concat rs) -> Concat (List.map rs ~f:(fun r -> f (Filter (p, r))))
    | Filter (p, r) -> Filter (p, f r)
    | Count r -> Count (f r)
    | Project (fs, r) -> Project (fs, f r)
    | EqJoin (f1, f2, r1, r2) -> EqJoin (f1, f2, f r1, f r2)
    | Concat rs -> Concat (List.map rs ~f)
    | Scan _ | Relation _ as r -> r
  in
  f

let hoist_filter : t -> t = fun r ->
  let open Ralgebra0 in
  let rec f = function
    | Scan _ | Relation _ as r -> r
    | Count r -> Count (f r)
    | EqJoin (f1, f2, r1, r2) ->
      begin match f r1, f r2 with
        | Filter (p1, r1), Filter (p2, r2) ->
          Filter (Binop (And, p1, p2), EqJoin(f1, f2, r1, r2))
        | Filter (p, r1), r2 -> Filter (p, EqJoin(f1, f2, r1, r2))
        | r1, Filter (p, r2) -> Filter (p, EqJoin(f1, f2, r1, r2))
        | r1, r2 -> EqJoin(f1, f2, r1, r2)
      end
    | Project (fs, r) ->
      begin match f r with
        | Filter (p, r) ->
          let fs = Set.union
              (Set.of_list (module Field) (pred_fields p))
              (Set.of_list (module Field) (fs))
                   |> Set.to_list
          in
          Filter (p, Project (fs, r))
        | r -> Project (fs, r)
      end
    | Filter (p, r) ->
      begin match f r with
        | Filter (p', r) -> Filter (Binop (And, p, p'), r)
        | r -> Filter (p, r)
      end
    | Concat rs ->
      let ps, rs = List.map rs ~f:(fun r -> match f r with
          | Filter (p, r) -> (Some p, r)
          | r -> (None, r))
                   |> List.unzip
      in
      match List.filter_map ps ~f:(fun x -> x) with
      | [] -> Concat rs
      | p::ps ->
        let pred = List.fold_left ~init:p ps ~f:(fun p p' -> Binop (And, p, p')) in
        Filter (pred, Concat rs)
  in
  f r

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
