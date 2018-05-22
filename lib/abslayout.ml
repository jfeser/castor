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
  | Select of 'f pred list * ('f, 'l) ralgebra
  | Filter of 'f pred * ('f, 'l) ralgebra
  | EqJoin of 'f * 'f * ('f, 'l) ralgebra * ('f, 'l) ralgebra
  | Scan of 'l
  | Agg of 'f Ralgebra0.agg list * 'f list * ('f, 'l) ralgebra
  | Dedup of ('f, 'l) ralgebra
[@@deriving visitors { variety = "endo"; name = "ralgebra_endo" },
            visitors { variety = "map"; name = "ralgebra_map" },
            visitors { variety = "iter"; name = "ralgebra_iter" },
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
      inherit [_] ralgebra_endo as super

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
      method visit_agg _ _ x = x
    end in
    v#visit_ralgebra ()

let%expect_test "subst" =
  let f = Field.of_name "f" in
  let g = Field.of_name "g" in
  let r = Relation.of_name "r" in
  let ctx = Map.of_alist_exn (module PredCtx.Key)
      [Field f, `Int 1; Field g, `Int 2] in
  let r = Filter (Binop (Eq, Field f, Field g), Select ([Field f; Field g], Scan r)) in
  print_s ([%sexp_of:(Field.t, Relation.t) ralgebra] (subst ctx r));
  [%expect {|
    (Filter
      (Binop (
        Eq
        (Int 1)
        (Int 2)))
      (Select
        ((Int 1)
         (Int 2))
        (Scan ((name r) (fields ()))))) |}]

type lambda = string * t

and hash_idx = {
  lookup : Field.t pred;
}

and ordered_idx = {
  lookup_low : Field.t pred;
  lookup_high : Field.t pred;
  order : Field.t pred;
}

and tuple = Cross | Zip
and t =
  | AEmpty
  | AScalar of Field.t pred
  | AList of (Field.t, Relation.t) ralgebra * lambda
  | ATuple of t list * tuple
  | AHashIdx of (Field.t, Relation.t) ralgebra * lambda * hash_idx
  | AOrderedIdx of (Field.t, Relation.t) ralgebra * lambda * ordered_idx
[@@deriving visitors { variety = "fold" },
            visitors { variety = "endo" },
            sexp]

let pred_relations : Field.t pred -> Relation.t list = fun p ->
  let rels = ref [] in
  let f = object
    inherit [_] ralgebra_iter as super
      method visit_Field () f = rels := f.Db.relation::!rels
      method visit_t () x = ()
      method visit_'l () x = ()
      method visit_'f () x = ()
      method visit_agg _ _ x = ()
    end in
  f#visit_pred () p; !rels

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
  let subst_table table =
    let f = object
      inherit [_] ralgebra_map as super
      method visit_Field _ (f : Field.t) =
        Field (sprintf "t%d.\"%s\"" table f.name)
      method visit_t = failwith "Unused"
      method visit_'l = failwith "Unused"
      method visit_'f = failwith "Unused"
      method visit_agg = failwith "Unused"
    end in
    f#visit_pred ()
  in
  let rec f = function
    | Select ([], r) ->
      let table = fresh () in
      sprintf "select top 0 from (%s) as t%d" (f r) table
    | Select (fs, r) ->
      let table = fresh () in
      let fields =
        List.map fs ~f:(fun p -> subst_table table p |> pred_to_sql)
        |> String.concat ~sep:","
      in
      sprintf "select %s from (%s) as t%d" fields (f r) table
    | Scan r -> sprintf "select * from %s" r.name
    | Filter (pred, r) ->
      let table = fresh () in
      let pred = subst_table table pred in
      sprintf "select * from (%s) as t%d where %s" (f r) table
        (pred_to_sql pred)
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
  let r = Select ([], Scan (Relation.of_name "r")) in
  print_endline (ralgebra_to_sql r);
  [%expect {| select top 0 from (select * from r) as t1 |}]

let%expect_test "project" =
  let r = Select ([Field (Field.of_name "f")], Scan (Relation.of_name "r")) in
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
  module type S_db = sig
    val conn : Postgresql.connection
  end

  module type S = sig
    val eval : PredCtx.t -> (Field.t, Relation.t) ralgebra -> (primvalue Map.M(String).t Seq.t -> 'a) -> 'a
  end
end

module Make (Config : Config.S) () = struct
  open Config

  let fresh = Fresh.create ()

  let partition : part:Field.t pred -> lookup:Field.t pred -> t -> t =
    fun ~part:p ~lookup l ->
      let rel = match pred_relations p with
        | [r] -> r
        | rs -> Error.create "Unexpected number of relations." rs
                  [%sexp_of:Relation.t list] |> Error.raise
      in
      let domain = Dedup (Select ([p], Scan rel)) in
      let name = Fresh.name fresh "x%d" in
      let pred =
        let subst = object
          inherit [_] ralgebra_endo as super
          method visit_Field _ _ (f: Field.t) =
            Field { f with name = sprintf "%s.%s" name f.name }
          method visit_'f _ x = x
          method visit_'l _ x = x
          method visit_t _ x = x
          method visit_agg _ _ x = x
        end in
        subst#visit_pred () p
      in
      let layout =
        let subst_ralgebra = object
          inherit [_] ralgebra_endo as super
          method visit_Scan () r rel' =
            if Relation.(rel = rel') then Filter (Binop (Eq, p, pred), r) else r
          method visit_'f _ x = x
          method visit_'l _ x = x
          method visit_t _ x = x
          method visit_agg _ _ x = x
        end in
        let subst = object
          inherit [_] endo as super
          method visit_ralgebra _ _ () r = subst_ralgebra#visit_ralgebra () r
          method visit_pred _ () x = x
        end in
        subst#visit_t () l
      in
      AHashIdx (domain, (name, layout), { lookup })

  let rec materialize : ?ctx:PredCtx.t -> t -> Layout.t =
    fun ?(ctx = Map.empty (module PredCtx.Key)) -> function
      | AEmpty -> empty
      | AScalar e ->
        Layout.of_value {
          value = eval_pred ctx e; rel = Relation.dummy; field = Field.dummy
        }
      | AList (q, (n, l)) ->
        eval ctx q (fun tups ->
            Seq.map tups ~f:(fun t ->
                let ctx =
                  Map.fold t ~init:ctx ~f:(fun ~key ~data ctx ->
                      let field = Field.of_name (sprintf "%s.%s" n key) in
                      Map.set ctx ~key:(Field field) ~data)
                in
                materialize ~ctx l)
            |> Seq.to_list)
        |> unordered_list
      | ATuple (ls, Zip) -> zip_tuple (List.map ~f:(materialize ~ctx) ls)
      | ATuple (ls, Cross) -> cross_tuple (List.map ~f:(materialize ~ctx) ls)
      | AHashIdx (q, (n, l), h) ->
        let out = ref (Map.empty (module ValueMap.Elem)) in
        eval ctx q (fun keys ->
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

module Make_db (Config_db : Config.S_db) () = struct
  module Config = struct
    let eval ctx query process =
      let sql = ralgebra_to_sql (subst ctx query) in
      Db.exec_cursor Config_db.conn sql process
  end
  include Make (Config) ()
end

module Test = struct
  let%expect_test "mat-col" =
    let conn = new Postgresql.connection ~dbname:"tpcds1" () in
    let layout = AList (Filter (Binop (Eq, (Field (Field.of_name "sm_carrier")), String "GERMA"), Scan (Relation.of_name "ship_mode")), ("t", AScalar (Field (Field.of_name "t.sm_carrier"))))
    in
    let module M = Make_db(struct let conn = conn end) () in
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
        Dedup (Select ([Field (Field.of_name "sm_type")], (Filter ((Binop (Eq, Field (Field.of_name "sm_type"), String "LIBRARY")), Scan (Relation.of_name "ship_mode"))))),
        ("t", AList (
            Filter (Binop (Eq, Field (Field.of_name "t.sm_type"),
                           Field (Field.of_name "sm_type")),
                    Scan (Relation.of_name "ship_mode")),
            ("t", AScalar (Field (Field.of_name "t.sm_code"))))), { lookup = Null })
    in
    let module M = Make_db(struct let conn = conn end) () in
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

  let rels = Hashtbl.create (module String)

  module Eval = struct
    let eval_pred0 = eval_pred

    include Eval.Make_relation(struct
      type relation = Relation.t [@@deriving compare, sexp]
      let eval_relation r = Hashtbl.find_exn rels r.name |> Seq.of_list
    end)

    let eval_filter ctx p seq =
      Seq.filter seq ~f:(fun t ->
          let ctx = Map.merge_right ctx (PredCtx.of_tuple t) in
          match eval_pred0 ctx p with
          | `Bool x -> x
          | _ -> failwith "Expected a boolean.")

    let eval_dedup seq =
      let set = Hash_set.create (module Tuple) in
      Seq.iter seq ~f:(Hash_set.add set);
      Hash_set.to_list set |> Seq.of_list

    let eval_select ctx out seq = 
      Seq.map seq ~f:(fun t ->
          let ctx = Map.merge_right ctx (PredCtx.of_tuple t) in
          List.map out ~f:(function
              | Field f ->
                let pv = Option.value_exn (PredCtx.find_field ctx f) in
                Value.({ value = pv; field = f; rel = Relation.dummy })
              | e -> eval_pred0 ctx e |> Value.of_primvalue))

    let eval : PredCtx.t -> (Field.t, Relation.t) ralgebra -> Tuple.t Seq.t = fun ctx r ->
      let rec eval = function
        | Scan r -> eval_relation r
        | Filter (p, r) -> eval_filter ctx p (eval r)
        | EqJoin (f1, f2, r1, r2) -> eval_eqjoin f1 f2 (eval r1) (eval r2)
        | Agg (output, key, r) -> eval_agg output key (eval r)
        | Dedup r -> eval_dedup (eval r)
        | Select (out, r) -> eval_select ctx out (eval r)
      in
      eval r
  end

  let create : string -> string list -> int list list -> Relation.t * Field.t list = fun name fs xs ->
    let data =
      List.map xs ~f:(fun data ->
          List.map2_exn fs data ~f:(fun name value : Value.t ->
              { field = Field.of_name name; value = `Int value;
                rel = Relation.dummy }))
    in
    Hashtbl.set rels ~key:name ~data;
    let rel = Relation.of_name name in
    rel, List.map fs ~f:(fun f : Field.t -> { name = f; relation = rel; dtype = DInt })

  module M = Make(struct
      let eval ctx query process =
        let results =
          Eval.eval ctx query
          |> Seq.map ~f:(fun (t: Tuple.t) ->
              List.map t ~f:(fun (v: Value.t) -> v.field.name, v.value)
              |> Map.of_alist_exn (module String))
        in
        process results
    end) ()

  let r1, [f; g] = create "r1" ["f"; "g"] [[1;2]; [1;3]; [2;1]; [2;2]; [3;4]]

  let%expect_test "part-list" =
    let layout = AList (Scan r1, ("x", ATuple ([AScalar (Field { f with name = "x.f" }); AScalar (Field { f with name = "x.g" })], Cross)))
    in
    let part_layout = M.partition ~part:(Field f) ~lookup:(Field f) layout in
    [%sexp_of:t] part_layout |> print_s;
    let mat_layout = M.materialize part_layout in
    [%sexp_of:Layout.t] mat_layout |> print_s
end
