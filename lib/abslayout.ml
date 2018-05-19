open Base
open Stdio
open Printf
open Expect_test_helpers_kernel

open Collections
open Db
open Layout

type op =
  | Eq
  | Lt
  | Le
  | Gt
  | Ge
  | And
  | Or
  | Add
  | Sub
  | Mul
  | Div
  | Mod

and 'f agg =
  | Count
  | Key of 'f
  | Sum of 'f
  | Avg of 'f
  | Min of 'f
  | Max of 'f

and 'f pred =
  | Var of Type.TypedName.t
  | Field of 'f
  | Null
  | Int of int
  | Bool of bool
  | String of string
  | Binop of (op * 'f pred * 'f pred)
  | Varop of (op * 'f pred list)

and ('f, 'l) ralgebra =
  | Project of 'f list * ('f, 'l) ralgebra
  | Filter of 'f pred * ('f, 'l) ralgebra
  | EqJoin of 'f * 'f * ('f, 'l) ralgebra * ('f, 'l) ralgebra
  | Scan of 'l
  | Agg of 'f agg list * 'f list * ('f, 'l) ralgebra
  | Dedup of ('f, 'l) ralgebra
[@@deriving visitors { variety = "endo" },
            visitors { variety = "map" },
            sexp]

let pred_of_value : Db.primvalue -> 'a pred = function
  | `Bool x -> Bool x
  | `String x -> String x
  | `Int x -> Int x
  | `Null -> Null
  | `Unknown x -> String x

let rec eval_pred : PredCtx.t -> Field.t pred -> primvalue =
  let raise = Error.raise in
  fun ctx -> function
    | Null -> `Null
    | Int x -> `Int x
    | String x -> `String x
    | Bool x -> `Bool x
    | Var (n, _) ->
      begin match PredCtx.find_var ctx n with
        | Some v -> v
        | None -> Error.create "Unbound variable." (n, ctx)
                    [%sexp_of:string * PredCtx.t] |> raise
      end
    | Field f ->
      begin match PredCtx.find_field ctx f with
        | Some v -> v
        | None -> Error.create "Unbound variable." (f, ctx)
                    [%sexp_of:Field.t * PredCtx.t] |> raise
      end
    | Binop (op, p1, p2) ->
      let v1 = eval_pred ctx p1 in
      let v2 = eval_pred ctx p2 in
      begin match op, v1, v2 with
        | Eq, `Null, _ | Eq, _, `Null -> `Bool false
        | Eq, `Bool x1, `Bool x2 -> `Bool (Bool.(x1 = x2))
        | Eq, `Int x1, `Int x2 -> `Bool (Int.(x1 = x2))
        | Eq, `String x1, `String x2 -> `Bool (String.(x1 = x2))
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
        | _ -> Error.create "Unexpected argument types." (op, v1, v2)
                 [%sexp_of:op * primvalue * primvalue]
               |> raise
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
        | _ -> Error.create "Unexpected argument types." (op, vs)
                 [%sexp_of:op * primvalue list] |> raise
      end

let subst : PredCtx.t -> (Field.t, 'a) ralgebra -> (Field.t, 'a) ralgebra =
  fun ctx ->
    let v = object
      inherit [_] endo as super

      method! visit_Var _ this v =
        match Map.find ctx (Var v) with
        | Some x -> pred_of_value x
        | None -> this

      method! visit_Field _ this f =
        match Map.find ctx (Field f) with
        | Some x -> pred_of_value x
        | None -> this

      method visit_t _ x = x
      method visit_'l _ x = x
      method visit_'f _ x = x
    end in
    v#visit_ralgebra ()

let%expect_test "subst" =
  let f = Field.of_name "f" in
  let g = Field.of_name "g" in
  let r = Relation.of_name "r" in
  let ctx = Map.of_alist_exn (module PredCtx.Key)
      [Field f, `Int 1; Field g, `Int 2] in
  let r = Filter (Binop (Eq, Field f, Field g), Project ([f; g], Scan r)) in
  print_s ([%sexp_of:(Field.t, Relation.t) ralgebra] (subst ctx r));
  [%expect {|
    (Filter
      (Binop (
        Eq
        (Int 1)
        (Int 2)))
      (Project
        (((name  f)
          (dtype DBool)
          (relation ((name "") (fields ()))))
         ((name  g)
          (dtype DBool)
          (relation ((name "") (fields ())))))
        (Scan ((name r) (fields ()))))) |}]

type 'a lambda = string * 'a

type hash_idx = {
  lookup : Field.t pred;
}

type ordered_idx = {
  lookup_low : Field.t pred;
  lookup_high : Field.t pred;
  order : Field.t pred;
}

type t =
  | AEmpty
  | AScalar of Field.t pred
  | AList of (Field.t, Relation.t) ralgebra * t lambda
  | ATuple of [`Cross | `Zip] * t list
  | AHashIdx of (Field.t, Relation.t) ralgebra * t lambda * hash_idx
  | AOrderedIdx of (Field.t, Relation.t) ralgebra * t lambda * ordered_idx

let rec pred_to_sql : string pred -> string = function
  | Var _ as x -> Error.create "Unsupported." x [%sexp_of:string pred] |> Error.raise
  | Field f -> sprintf "%s" f
  | Int x -> Int.to_string x
  | Bool true -> "true"
  | Bool false -> "false"
  | String s -> sprintf "'%s'" s
  | Null -> "null"
  | Binop (op, p1, p2) ->
    let s1 = sprintf "(%s)" (pred_to_sql p1) in
    let s2 = sprintf "(%s)" (pred_to_sql p2) in
    begin match op with
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
      | Mod -> sprintf "%s %% %s" s1 s2
    end
  | Varop (op, ps) ->
    let ss = List.map ps ~f:(fun p -> sprintf "(%s)" (pred_to_sql p)) in
    begin match op with
      | And -> String.concat ss ~sep:" and "
      | Or -> String.concat ss ~sep:" or "
      | _ -> failwith "Unsupported op."
    end

let ralgebra_to_sql : (Field.t, Relation.t) ralgebra -> string = fun r ->
  let fresh =
    let x = ref 0 in
    fun () -> incr x; !x
  in
  let rec f = function
    | Project ([], r) ->
      let table = fresh () in
      sprintf "select top 0 from (%s) as t%d" (f r) table
    | Project (fs, r) ->
      let table = fresh () in
      let fields =
        List.map fs ~f:(fun (f : Field.t) -> sprintf "t%d.\"%s\"" table f.name)
        |> String.concat ~sep:","
      in
      sprintf "select %s from (%s) as t%d" fields (f r) table
    | Scan r -> sprintf "select * from %s" r.name
    | Filter (pred, r) ->
      let table = fresh () in
      let pred =
        let f = object
          inherit [_] map as super
          method visit_Field _ (f : Field.t) =
            Field (sprintf "t%d.\"%s\"" table f.name)
          method visit_t = failwith "Unused"
          method visit_'l = failwith "Unused"
          method visit_'f = failwith "Unused"
        end in
        f#visit_pred () pred
      in
      sprintf "select * from (%s) as t%d where %s" (f r) table (pred_to_sql pred)
    | EqJoin (f1, f2, r1, r2) ->
      let t1 = fresh () in
      let t2 = fresh () in
      sprintf "select * from (%s) as t%d, (%s) as t%d where t%d.\"%s\" = t%d.\"%s\""
        (f r1) t1 (f r2) t2 t1 f1.name t2 f2.name
    | Agg (aggs, key, r) ->
      let table = fresh () in
      let aggs = List.map aggs ~f:(function
          | Count -> "count(*)"
          | Key f -> sprintf "t%d.\"%s\"" table f.name
          | Sum f -> sprintf "sum(t%d.\"%s\")" table f.name
          | Avg f -> sprintf "avg(t%d.\"%s\")" table f.name
          | Min f -> sprintf "min(t%d.\"%s\")" table f.name
          | Max f -> sprintf "max(t%d.\"%s\")" table f.name)
                 |> String.concat ~sep:", "
      in
      let key =
        List.map key ~f:(fun f -> sprintf "t%d.\"%s\"" table f.name)
        |> String.concat ~sep:", "
      in
      sprintf "select %s from (%s) as t%d group by (%s)" aggs (f r) table key
    | Dedup r ->
      let table = fresh () in
      sprintf "select distinct * from (%s) as t%d" (f r) table
  in
  f r

let%expect_test "project-empty" =
  let r = Project ([], Scan (Relation.of_name "r")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select top 0 from (select * from r) as t1 |}]

let%expect_test "project" =
  let r = Project ([Field.of_name "f"], Scan (Relation.of_name "r")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select t1."f" from (select * from r) as t1 |}]

let%expect_test "filter" =
  let r = Filter (Binop (Eq, Field (Field.of_name "f"), Field (Field.of_name "g")),
                  Scan (Relation.of_name "r")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select * from (select * from r) as t1 where (t1."f") = (t1."g") |}]

let%expect_test "eqjoin" =
  let r = EqJoin (Field.of_name "f", Field.of_name "g",
                  Scan (Relation.of_name "r"), Scan (Relation.of_name "s")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select * from (select * from r) as t1, (select * from s) as t2 where t1."f" = t2."g" |}]

let%expect_test "agg" =
  let f = Field.of_name "f" in
  let g = Field.of_name "g" in
  let r = Agg ([Sum g], [f; g], Scan (Relation.of_name "r")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select sum(t1."g") from (select * from r) as t1 group by (t1."f", t1."g") |}]

let%expect_test "agg" =
  let r = Filter (Binop (Eq, (Field (Field.of_name "sm_carrier")), String "GERMA"), Scan (Relation.of_name "ship_mode")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select * from (select * from ship_mode) as t1 where (t1."sm_carrier") = ('GERMA') |}]

module Config = struct
  module type S = sig
    include Eval.Config.S
  end
end

module Make (Config : Config.S) = struct
  open Config

  let rec materialize : ?ctx:PredCtx.t -> t -> Layout.t =
    fun ?(ctx = Map.empty (module PredCtx.Key)) -> function
      | AEmpty -> empty
      | AScalar e ->
        Layout.of_value {
          value = eval_pred ctx e; rel = Relation.dummy; field = Field.dummy
        }
      | AList (q, (n, l)) ->
        Db.exec_cursor conn (ralgebra_to_sql (subst ctx q)) (fun tups ->
            Seq.map tups ~f:(fun t ->
                let ctx =
                  Map.fold t ~init:ctx ~f:(fun ~key ~data ctx ->
                      let field = Field.of_name (sprintf "%s.%s" n key) in
                      Map.set ctx ~key:(Field field) ~data)
                in
                materialize ~ctx l)
            |> Seq.to_list)
        |> unordered_list
      | ATuple (`Zip, ls) -> zip_tuple (List.map ~f:(materialize ~ctx) ls)
      | ATuple (`Cross, ls) -> cross_tuple (List.map ~f:(materialize ~ctx) ls)
      | AHashIdx (q, (n, l), h) ->
        let out = ref (Map.empty (module ValueMap.Elem)) in
        Db.exec_cursor conn (ralgebra_to_sql (subst ctx q)) (fun keys ->
            Seq.iter keys ~f:(fun k ->
                let [(name, key)] = Map.to_alist k in
                let field = Field.of_name (sprintf "%s.%s" n name) in
                let ctx = Map.set ctx ~key:(Field field) ~data:key in
                let value = materialize ~ctx l in
                out := Map.set !out ~key:(Value.of_primvalue key) ~data:value
              ));
        table !out { field = Field.of_name "fixme"; lookup = PredCtx.Key.Field (Field.of_name "fixme") }
      | AOrderedIdx (q, (n, l), o) -> failwith ""
      (* let out = ref [] in
       * Db.exec_cursor conn (ralgebra_to_sql (subst ctx q)) (fun keys ->
       *     Seq.iter keys ~f:(fun k ->
       *         let [(name, key)] = Map.to_alist k in
       *         let field = Field.of_name (sprintf "%s.%s" n name) in
       *         let ctx = Map.set ctx ~key:(Field field) ~data:key in
       *         let value = materialize ctx l in
       *         out := cross_tuple [of_value (Value.of_primvalue key); value] :: !out
       *       ));
       * ordered_list !out { field = Field.dummy; order = `Asc; lookup = } *)
end

let%expect_test "mat-col" = 
  let conn = new Postgresql.connection ~dbname:"tpcds1" () in
  let layout = AList (Filter (Binop (Eq, (Field (Field.of_name "sm_carrier")), String "GERMA"), Scan (Relation.of_name "ship_mode")), ("t", AScalar (Field (Field.of_name "t.sm_carrier"))))
  in
  let module M = Make(struct let conn = conn end) in
  M.materialize layout |> [%sexp_of:Layout.t] |> print_s;
  [%expect {|
    (UnorderedList ((
      String "GERMA               " (
        (rel ((name "") (fields ())))
        (field (
          (name  "")
          (dtype DBool)
          (relation ((name "") (fields ()))))))))) |}]

let%expect_test "mat-hidx" = 
  let conn = new Postgresql.connection ~dbname:"tpcds1" () in
  let layout = AHashIdx (
      Dedup (Project ([Field.of_name "sm_type"], (Filter ((Binop (Eq, Field (Field.of_name "sm_type"), String "LIBRARY")), Scan (Relation.of_name "ship_mode"))))),
      ("t", AList (
          Filter (Binop (Eq, Field (Field.of_name "t.sm_type"),
                         Field (Field.of_name "sm_type")),
                  Scan (Relation.of_name "ship_mode")),
          ("t", AScalar (Field (Field.of_name "t.sm_code"))))), { lookup = Null })
  in
  let module M = Make(struct let conn = conn end) in
  M.materialize layout |> [%sexp_of:Layout.t] |> print_s;
  [%expect {|
    WARNING:  there is already a transaction in progress
    WARNING:  there is no transaction in progress
    (Table
      ((
        ((rel ((name "") (fields ())))
         (field (
           (name  "")
           (dtype DBool)
           (relation ((name "") (fields ())))))
         (value (Unknown "LIBRARY                       ")))
        (UnorderedList (
          (String "AIR       " (
            (rel ((name "") (fields ())))
            (field (
              (name  "")
              (dtype DBool)
              (relation ((name "") (fields ())))))))
          (String "SURFACE   " (
            (rel ((name "") (fields ())))
            (field (
              (name  "")
              (dtype DBool)
              (relation ((name "") (fields ())))))))
          (String "SEA       " (
            (rel ((name "") (fields ())))
            (field (
              (name  "")
              (dtype DBool)
              (relation ((name "") (fields ())))))))))))
      ((field (
         (name  fixme)
         (dtype DBool)
         (relation ((name "") (fields ())))))
       (lookup (
         Field (
           (name  fixme)
           (dtype DBool)
           (relation ((name "") (fields ())))))))) |}]
