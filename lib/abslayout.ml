open Base
open Stdio
open Printf
open Expect_test_helpers_kernel

open Collections
open Db
open Layout

module No_config = struct
  include Abslayout0

  let of_lexbuf_exn : Lexing.lexbuf -> (string * string, (string * string, string) Abslayout0.layout) ralgebra = fun lexbuf ->
    try Ralgebra_parser.abs_ralgebra_eof Ralgebra_lexer.token lexbuf with
    | Ralgebra0.ParseError (msg, line, col) as e ->
      Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col);
      raise e

  let of_channel_exn : In_channel.t -> (string * string, (string * string, string) Abslayout0.layout) ralgebra = fun ch -> of_lexbuf_exn (Lexing.from_channel ch)

  let of_string_exn : string -> (string * string, (string * string, string) Abslayout0.layout) ralgebra = fun s -> of_lexbuf_exn (Lexing.from_string s)

  let relations : (_, (_, 'r) Abslayout0.layout) ralgebra -> 'r list = fun r ->
    let inner_ralgebra_relations = object (self)
      inherit [_] ralgebra_reduce as super
      method zero = []
      method plus = (@)
      method visit_'l () l = [l]
      method visit_'f _ _ = self#zero
    end in
    let layout_relations = object (self)
      inherit [_] reduce as super
      method zero = []
      method plus = (@)
      method visit_ralgebra _ _ () r =
        inner_ralgebra_relations#visit_ralgebra () r
      method visit_pred _ _ _ = self#zero
      method visit_'r _ _ = self#zero
      method visit_'f _ _ = self#zero
    end in
    let ralgebra_relations = object (self)
      inherit [_] ralgebra_reduce as super
      method zero = []
      method plus = (@)
      method visit_'l () l = layout_relations#visit_layout () l
      method visit_'f _ _ = self#zero
    end in
    ralgebra_relations#visit_ralgebra () r

  let params : (_, (_, _) Abslayout0.layout) ralgebra -> Set.M(Type0.TypedName).t =
    fun r ->
      let params = object (self)
        inherit [_] ralgebra_reduce as super
        method zero = Set.empty (module Type0.TypedName)
        method plus = Set.union
        method visit_Var () n = Set.singleton (module Type0.TypedName) n
        method visit_'l () l = self#zero
        method visit_'f _ _ = self#zero
      end in
      params#visit_ralgebra () r

  let resolve :
    Postgresql.connection
    -> (string * string, (string * string, string) Abslayout0.layout) ralgebra
    -> (Field.t, (Field.t, Relation.t) Abslayout0.layout) ralgebra =
    fun conn r ->
      let resolve_relation ctx r = match Hashtbl.find ctx r with
        | Some r' -> r'
        | None -> Error.create "Relation not found." r [%sexp_of:string] |> Error.raise
      in
      let resolve_field _ = Field.of_name in
      let inner_resolver = object
        inherit [_] ralgebra_map as super
        method visit_'l = resolve_relation
        method visit_'f ctx (r, f) =
          resolve_field (resolve_relation ctx r) f
      end in
      let layout_resolver = object
        inherit [_] map as super
        method visit_'f ctx (r, f) =
          resolve_field (resolve_relation ctx r) f
        method visit_'r = resolve_relation
        method visit_ralgebra _ _ = inner_resolver#visit_ralgebra
        method visit_pred _ = inner_resolver#visit_pred
      end in
      let resolver = object
        inherit [_] ralgebra_map as super
        method visit_'l = layout_resolver#visit_layout
        method visit_'f ctx (r, f) =
          resolve_field (resolve_relation ctx r) f
      end in
      let ctx =
        relations r
        |> List.map ~f:(fun r -> (r, Relation.from_db conn r))
        |> Hashtbl.of_alist_exn (module String)
      in
      resolver#visit_ralgebra ctx r

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
                   [%sexp_of:Ralgebra0.op * primvalue * primvalue]
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
                   [%sexp_of:Ralgebra0.op * primvalue list] |> raise
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

        method visit_'l _ x = x
        method visit_'f _ x = x
        method visit_agg _ x = x
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

  let pred_relations : Field.t pred -> Relation.t list = fun p ->
    let rels = ref [] in
    let f = object
      inherit [_] ralgebra_iter as super
      method visit_Field () f = rels := f.Db.relation::!rels
      method visit_'l () x = ()
      method visit_'f () x = ()
      method visit_agg () x = ()
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
      fun () -> Caml.incr x; !x
    in
    let subst_table table =
      let f = object
        inherit [_] ralgebra_map as super
        method visit_Field _ (f : Field.t) =
          Field (sprintf "t%d.\"%s\"" table f.name)
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
end
include No_config

module Config = struct
  module type S_db = sig
    val conn : Postgresql.connection
  end

  module type S = sig
    val eval : PredCtx.t -> (Field.t, Relation.t) ralgebra -> primvalue Map.M(String).t Seq.t
  end
end

module Make (Config : Config.S) () = struct
  open Config

  include No_config

  let fresh = Fresh.create ()

  let partition : part:Field.t pred -> lookup:Field.t pred
    -> (Field.t, Relation.t) layout -> (Field.t, Relation.t) layout =
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
          method visit_agg _ x = x
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
          method visit_agg _ x = x
        end in
        let subst = object
          inherit [_] endo as super
          method visit_ralgebra _ _ () r = subst_ralgebra#visit_ralgebra () r
          method visit_pred _ () x = x
          method visit_'f _ () x = x
          method visit_'r _ () x = x
        end in
        subst#visit_layout () l
      in
      AHashIdx (domain, name, layout, { lookup })

  class virtual ['self] material_fold = object (self : 'self)
    method virtual build_AList : _
    method virtual build_ATuple : _
    method virtual build_AHashIdx : _
    method virtual build_AOrderedIdx : _
    method virtual build_AEmpty : _
    method virtual build_AScalar : _

    method visit_AList = fun ctx q n l ->
      let ls =
        eval ctx q |> Seq.map ~f:(fun t ->
            let ctx =
              Map.fold t ~init:ctx ~f:(fun ~key ~data ctx ->
                  let field = Field.of_name (sprintf "%s.%s" n key) in
                  Map.set ctx ~key:(Field field) ~data)
            in
            self#visit_t ctx l)
      in
      self#build_AList ls

    method visit_ATuple ctx ls kind =
      self#build_ATuple (List.map ~f:(self#visit_t ctx) ls) kind

    method visit_AHashIdx ctx q n l h =
      let kv =
        eval ctx q |> Seq.map ~f:(fun k ->
            let [(name, key)] = Map.to_alist k in
            let field = Field.of_name (sprintf "%s.%s" n name) in
            let ctx = Map.set ctx ~key:(Field field) ~data:key in
            let value = self#visit_t ctx l in
            (Value.of_primvalue key, value))
      in
      self#build_AHashIdx kv h

    method visit_AEmpty ctx = self#build_AEmpty

    method visit_AScalar ctx e =
      let l = Layout.of_value {
          value = eval_pred ctx e; rel = Relation.dummy; field = Field.dummy
        } in
      self#build_AScalar l

    method visit_AOrderedIdx ctx q n l o = failwith ""

    method visit_t ctx = function
      | AEmpty -> self#visit_AEmpty ctx
      | AScalar e -> self#visit_AScalar ctx e
      | AList (r, n, a) -> self#visit_AList ctx r n a
      | ATuple (a, k) -> self#visit_ATuple ctx a k
      | AHashIdx (r, n, a, t) -> self#visit_AHashIdx ctx r n a t
      | AOrderedIdx (_, _, _, _) -> failwith ""
  end

  let materialize : ?ctx:PredCtx.t -> (Field.t, Relation.t) layout -> Layout.t =
    fun ?(ctx = Map.empty (module PredCtx.Key)) l ->
      let f = object
        inherit [_] material_fold as super

        method build_AEmpty = empty
        method build_AList ls = Seq.to_list ls |> unordered_list
        method build_ATuple ls kind =
          match kind with
          | Zip -> zip_tuple ls
          | Cross -> cross_tuple ls
        method build_AHashIdx kv h =
          let m = Seq.to_list kv |> Map.of_alist_exn (module ValueMap.Elem) in
          table m { field = Field.of_name "fixme";
                    lookup = PredCtx.Key.Field (Field.of_name "fixme") }
        method build_AScalar l = l
        method build_AOrderedIdx ls o = failwith ""
      end in
      f#visit_t ctx l

  module TF = struct
    open Type
    class ['self] type_fold =
      object (self : 'self)
        inherit [_] material_fold as super

        method build_AEmpty = EmptyT
        method build_AScalar l = of_layout_exn l
        method build_AList ls =
          let (t, c) = Seq.fold ls ~init:(EmptyT, AbsCount.zero) ~f:(fun (t, c) t' ->
              (unify_exn t t', AbsCount.(c + count t')))
          in
          UnorderedListT (t, { count = c })
        method build_ATuple ls kind =
          let counts = List.map ls ~f:count in
          match kind with
          | Zip -> ZipTupleT (ls, { count = List.fold_left1_exn ~f:AbsCount.unify counts })
          | Cross -> CrossTupleT (ls, { count = List.fold_left1_exn ~f:AbsCount.( * ) counts })
        method build_AHashIdx kv h =
          let kt, vt = Seq.fold kv ~init:(EmptyT, EmptyT) ~f:(fun (kt, vt1) (kv, vt2) ->
              (unify_exn kt (Layout.of_value kv |> of_layout_exn),
               unify_exn vt1 vt2)) in
          TableT (kt, vt, { count = None; field = Field.of_name "fixme";
                            lookup = PredCtx.Key.Field (Field.of_name "fixme") })
        method build_AOrderedIdx ls o = failwith ""
      end

    class ['self] count_fold =
      object (self : 'self)
        inherit [_] material_fold as super

        method build_AEmpty = AbsCount.zero
        method build_AScalar _ = AbsCount.abstract 1
        method build_AList ls = Seq.fold ls ~init:AbsCount.zero ~f:AbsCount.(+)
        method build_ATuple ls = function
          | Zip -> List.fold_left1_exn ls ~f:AbsCount.unify
          | Cross -> List.fold_left ls ~init:(AbsCount.abstract 1) ~f:AbsCount.( * )
        method build_AHashIdx kv _ =
          Seq.map kv ~f:(fun (_, c) -> c)
          |> Seq.fold1_exn ~f:AbsCount.unify
        method build_AOrderedIdx ls o = failwith ""
      end

  end
  include TF

  let to_type : ?ctx:PredCtx.t -> (Field.t, Relation.t) layout -> Type.t =
    fun ?(ctx = Map.empty (module PredCtx.Key)) l -> (new type_fold)#visit_t ctx l

  module S = struct
    open Bitstring
    open Serialize
    open Type

    class virtual ['self] typed_counted_material_fold =
      let counter = new count_fold in
      object (self : 'self)
        method virtual build_AList : _
        method virtual build_ATuple : _
        method virtual build_AHashIdx : _
        method virtual build_AOrderedIdx : _
        method virtual build_AEmpty : _
        method virtual build_AScalar : _

        method visit_AList = fun ctx type_ q n elem_layout ->
          match type_ with
          | UnorderedListT (elem_t, _) ->
            let elems, counts =
              eval ctx q
              |> Seq.map ~f:(fun t ->
                  let ctx =
                    Map.fold t ~init:ctx ~f:(fun ~key ~data ctx ->
                        let field = Field.of_name (sprintf "%s.%s" n key) in
                        Map.set ctx ~key:(Field field) ~data)
                  in
                  self#visit_t ctx elem_t elem_layout)
              |> Seq.unzip
            in
            let count = counter#build_AList counts in
            (self#build_AList type_ count elems, count)
          | t -> Error.(create "Unexpected layout type." t [%sexp_of:Type.t] |> raise)

        method visit_ATuple ctx type_ elem_layouts kind =
          match type_ with
          | CrossTupleT (elem_ts, _) | ZipTupleT (elem_ts, _) ->
            let elems, counts =
              List.map2_exn ~f:(fun t l -> self#visit_t ctx t l) elem_ts elem_layouts
              |> List.unzip
            in
            let count = counter#build_ATuple counts kind in
            (self#build_ATuple type_ count elems kind, count)
          | t -> Error.(create "Unexpected layout type." t [%sexp_of:Type.t] |> raise)

        method visit_AHashIdx ctx type_ q n l h =
          match type_ with
          | TableT (_, t, _) ->
            let kv_elems, kv_counts =
              eval ctx q |> Seq.map ~f:(fun k ->
                  let [(name, key)] = Map.to_alist k in
                  let field = Field.of_name (sprintf "%s.%s" n name) in
                  let ctx = Map.set ctx ~key:(Field field) ~data:key in
                  let value = self#visit_t ctx t l in
                  (Value.of_primvalue key, value))
              |> Seq.map ~f:(fun (k, (v, c)) -> ((k, v), (k, c)))
              |> Seq.unzip
            in
            let count = counter#build_AHashIdx kv_counts h in
            (self#build_AHashIdx type_ count kv_elems h, count)
          | t -> Error.(create "Unexpected layout type." t [%sexp_of:Type.t] |> raise)

        method visit_AEmpty ctx type_ =
          let count = counter#build_AEmpty in
          (self#build_AEmpty type_ count, count)

        method visit_AScalar ctx type_ e =
          let l = Layout.of_value {
              value = eval_pred ctx e; rel = Relation.dummy; field = Field.dummy
            } in
          let count = counter#build_AScalar l in
          (self#build_AScalar type_ count l, count)

        method visit_AOrderedIdx ctx type_ q n l o = failwith ""

        method visit_t ctx type_ = function
          | AEmpty -> self#visit_AEmpty ctx type_
          | AScalar e -> self#visit_AScalar ctx type_ e
          | AList (r, n, a) -> self#visit_AList ctx type_ r n a
          | ATuple (a, k) -> self#visit_ATuple ctx type_ a k
          | AHashIdx (r, n, a, t) -> self#visit_AHashIdx ctx type_ r n a t
          | AOrderedIdx (_, _, _, _) -> failwith ""
      end

    class ['self] serialize_fold =
      object (self : 'self)
        inherit [_] typed_counted_material_fold as super

        method build_AEmpty type_ _ = empty

        method build_AScalar type_ _ l =
          match l.node with
          | Null s -> serialize_null type_ l s
          | Int (x, s) -> serialize_int type_ l x s
          | Bool (x, s) -> serialize_bool type_ l x s
          | String (x, s) -> serialize_string type_ l x s

        method build_ATuple type_ count ls kind =
          match type_ with
          | CrossTupleT (ts, { count }) | ZipTupleT (ts, { count }) ->
            let body = ls |> concat |> label "Tuple body" in
            let len = byte_length body in
            let len_str = of_int ~width:64 len |> label "Tuple len" in
            concat [len_str; body] |> label "Tuple"
          | t -> Error.(create "Unexpected layout type." t [%sexp_of:Type.t] |> raise)

        method build_AList type_ count ls =
          match type_ with
          | OrderedListT (t, { count }) | UnorderedListT (t, { count }) ->
            let elems = Seq.to_list ls in
            let count = List.length elems in
            let body = elems |> concat |> label "List body" in
            let len = byte_length body in
            let len_str = of_int ~width:64 len |> label "List len" in
            let ct_str = of_int ~width:64 count |> label "List count" in
            concat [ct_str; len_str; body] |> label "List"
          | t -> Error.(create "Unexpected layout type." t [%sexp_of:Type.t] |> raise)

        method build_AHashIdx type_ _ kv h =
          match type_ with
          | TableT (key_t, value_t, _) ->
            let keys =
              Seq.map kv ~f:(fun (k, v) -> k, serialize key_t (Layout.of_value k), v)
              |> Seq.to_list
            in
            Logs.debug (fun m -> m "Generating hash for %d keys." (List.length keys));
            let hash = Cmph.(List.map keys ~f:(fun (_, b, _) -> to_string b)
                             |> KeySet.of_fixed_width
                             |> Config.create ~seed:0 ~algo:`Chd |> Hash.of_config)
            in
            let keys =
              List.map keys ~f:(fun (k, b, v) ->
                  let h = Cmph.Hash.hash hash (to_string b) in
                  (k, b, h, v))
            in
            Out_channel.with_file "hashes.txt" ~f:(fun ch ->
                List.iter keys ~f:(fun (k, b, h, v) ->
                    Out_channel.fprintf ch "%s -> %d\n" (Bitstring.to_string b) h));
            let hash_body = Cmph.Hash.to_packed hash |> Bytes.of_string |> align isize in
            let hash_body_b = of_bytes hash_body in
            let hash_len = byte_length hash_body_b in

            let table_size =
              List.fold_left keys ~f:(fun m (_, _, h, _) -> Int.max m h) ~init:0
              |> fun m -> m + 1
            in
            let hash_table = Array.create ~len:table_size (0xDEADBEEF) in

            let values = empty in
            let offset = isize * table_size in
            let offset, values = List.fold_left keys ~init:(offset, values)
                ~f:(fun (offset, values) (k, b, h, vb) ->
                    hash_table.(h) <- offset;
                    let values = concat
                        [values; b |> label "Table key"; vb |> label "Table value"]
                    in
                    let offset = offset + byte_length b + byte_length vb in
                    (offset, values))
            in

            let hash_table_b =
              Array.map hash_table ~f:(of_int ~width:64) |> Array.to_list |> concat
            in

            let body =
              concat [
                of_int ~width:64 hash_len |> label "Cmph data len";
                hash_body_b |> label "Cmph data";
                hash_table_b |> label "Table mapping";
                values |> label "Table values";
              ]
            in
            concat [
              of_int ~width:64 (byte_length body) |> label "Table len";
              body;
            ]
          | t -> Error.(create "Unexpected layout type." t [%sexp_of:Type.t] |> raise)

        method build_AOrderedIdx = failwith ""
      end
  end
  include S

  let serialize : ?ctx:PredCtx.t -> Type.t -> (Field.t, Relation.t) layout -> Bitstring.t =
    fun ?(ctx = Map.empty (module PredCtx.Key)) t l ->
      let (s, _) = (new serialize_fold)#visit_t ctx t l in s

  type schema = (string * Type.PrimType.t) list

  let rec pred_to_schema_exn : Field.t pred -> (string * Type.PrimType.t) =
    function
    | Var (n, t) -> (n, t)
    | Field f -> f.name, Type.PrimType.of_dtype f.dtype
    | Int _ -> "", IntT
    | Bool _ -> "", BoolT
    | String _ -> "", StringT
    | Null -> failwith ""
    | Binop (op, _, _) | Varop (op, _) -> begin match op with
        | Eq | Lt | Le | Gt | Ge | And | Or -> "", BoolT
        | Add | Sub | Mul | Div | Mod -> "", IntT
      end

  let ralgebra_to_schema_exn : ('a -> schema) -> (Field.t, 'a) ralgebra -> schema =
    fun layout_to_schema_exn ->
      let rec ralgebra_to_schema_exn = function
        | Select (exprs, r) -> List.map exprs ~f:(pred_to_schema_exn)
        | Filter (_, r) | Dedup r -> ralgebra_to_schema_exn r
        | EqJoin (_, _, r1, r2) ->
          ralgebra_to_schema_exn r1 @ ralgebra_to_schema_exn r2
        | Scan r -> layout_to_schema_exn r
        | Agg (out, _, _) ->
          List.map out ~f:(function
              | Count -> "count", Type.PrimType.IntT
              | Key (f: Field.t) -> f.name, Type.PrimType.of_dtype f.dtype
              | Sum _ | Min _ | Max _ as a ->
                Ralgebra.agg_to_string a, Type.PrimType.IntT
              | Avg _ -> failwith "unsupported")
      in
      ralgebra_to_schema_exn

  let rec layout_to_schema_exn : (Field.t, Relation.t) layout -> (string * Type.PrimType.t) list =
    function
    | AEmpty -> []
    | AScalar p -> [pred_to_schema_exn p]
    | AHashIdx (_, _, l, _)
    | AOrderedIdx (_, _, l, _)
    | AList (_, _, l) -> layout_to_schema_exn l
    | ATuple (ls, _) -> List.concat_map ~f:layout_to_schema_exn ls

  let to_schema_exn : t -> _ list = fun r ->
    ralgebra_to_schema_exn layout_to_schema_exn r
end

module Make_db (Config_db : Config.S_db) () = struct
  module Config = struct
    let eval ctx query =
      let sql = ralgebra_to_sql (subst ctx query) in
      Db.exec_cursor Config_db.conn sql
  end
  include Make (Config) ()
end

module Test = struct
  let%expect_test "mat-col" =
    let conn = new Postgresql.connection ~dbname:"tpcds1" () in
    let layout = AList (Filter (Binop (Eq, (Field (Field.of_name "sm_carrier")), String "GERMA"), Scan (Relation.of_name "ship_mode")), "t", AScalar (Field (Field.of_name "t.sm_carrier")))
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
        "t", AList (
          Filter (Binop (Eq, Field (Field.of_name "t.sm_type"),
                         Field (Field.of_name "sm_type")),
                  Scan (Relation.of_name "ship_mode")),
          "t", AScalar (Field (Field.of_name "t.sm_code"))), { lookup = Null })
    in
    let module M = Make_db(struct let conn = conn end) () in
    M.materialize layout |> [%sexp_of:Layout.t] |> print_s;
    [%expect {|
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
      let eval ctx query =
        Eval.eval ctx query
        |> Seq.map ~f:(fun (t: Tuple.t) ->
            List.map t ~f:(fun (v: Value.t) -> v.field.name, v.value)
            |> Map.of_alist_exn (module String))
    end) ()

  let r1, [f; g] = create "r1" ["f"; "g"] [[1;2]; [1;3]; [2;1]; [2;2]; [3;4]]

  let%expect_test "part-list" =
    let layout = AList (Scan r1, "x", ATuple ([AScalar (Field { f with name = "x.f" }); AScalar (Field { f with name = "x.g" })], Cross))
    in
    let part_layout = M.partition ~part:(Field f) ~lookup:(Field f) layout in
    [%sexp_of:(Field.t, Relation.t) layout] part_layout |> print_s;
    [%sexp_of:Type.t] (M.to_type part_layout) |> print_s;
    let mat_layout = M.materialize part_layout in
    [%sexp_of:Layout.t] mat_layout |> print_s;
    [%expect {|
      (AHashIdx
        (Dedup (
          Select
          ((
            Field (
              (name  f)
              (dtype DInt)
              (relation ((name r1) (fields ()))))))
          (Scan ((name r1) (fields ())))))
        x0
        (AList
          (Filter
            (Binop (
              Eq
              (Field (
                (name  f)
                (dtype DInt)
                (relation ((name r1) (fields ())))))
              (Field (
                (name  x0.f)
                (dtype DInt)
                (relation ((name r1) (fields ())))))))
            (Scan ((name r1) (fields ()))))
          x
          (ATuple
            ((AScalar (
               Field (
                 (name  x.f)
                 (dtype DInt)
                 (relation ((name r1) (fields ()))))))
             (AScalar (
               Field (
                 (name  x.g)
                 (dtype DInt)
                 (relation ((name r1) (fields ())))))))
            Cross))
        ((
          lookup (
            Field (
              (name  f)
              (dtype DInt)
              (relation ((name r1) (fields ()))))))))
      (TableT
        (IntT (
          (range (1 3))
          (nullable false)
          (field (
            (name  "")
            (dtype DBool)
            (relation ((name "") (fields ())))))))
        (UnorderedListT
          (CrossTupleT
            ((IntT (
               (range (1 3))
               (nullable false)
               (field (
                 (name  "")
                 (dtype DBool)
                 (relation ((name "") (fields ())))))))
             (IntT (
               (range (1 4))
               (nullable false)
               (field (
                 (name  "")
                 (dtype DBool)
                 (relation ((name "") (fields ()))))))))
            ((count ((1 1)))))
          ((count ((1 2)))))
        ((count ())
         (field (
           (name  fixme)
           (dtype DBool)
           (relation ((name "") (fields ())))))
         (lookup (
           Field (
             (name  fixme)
             (dtype DBool)
             (relation ((name "") (fields ()))))))))
      (Table
        ((((rel ((name "") (fields ())))
           (field (
             (name  "")
             (dtype DBool)
             (relation ((name "") (fields ())))))
           (value (Int 1)))
          (UnorderedList (
            (CrossTuple (
              (Int 1 (
                (rel ((name "") (fields ())))
                (field (
                  (name  "")
                  (dtype DBool)
                  (relation ((name "") (fields ())))))))
              (Int 2 (
                (rel ((name "") (fields ())))
                (field (
                  (name  "")
                  (dtype DBool)
                  (relation ((name "") (fields ())))))))))
            (CrossTuple (
              (Int 1 (
                (rel ((name "") (fields ())))
                (field (
                  (name  "")
                  (dtype DBool)
                  (relation ((name "") (fields ())))))))
              (Int 3 (
                (rel ((name "") (fields ())))
                (field (
                  (name  "")
                  (dtype DBool)
                  (relation ((name "") (fields ()))))))))))))
         (((rel ((name "") (fields ())))
           (field (
             (name  "")
             (dtype DBool)
             (relation ((name "") (fields ())))))
           (value (Int 2)))
          (UnorderedList (
            (CrossTuple (
              (Int 2 (
                (rel ((name "") (fields ())))
                (field (
                  (name  "")
                  (dtype DBool)
                  (relation ((name "") (fields ())))))))
              (Int 1 (
                (rel ((name "") (fields ())))
                (field (
                  (name  "")
                  (dtype DBool)
                  (relation ((name "") (fields ())))))))))
            (CrossTuple (
              (Int 2 (
                (rel ((name "") (fields ())))
                (field (
                  (name  "")
                  (dtype DBool)
                  (relation ((name "") (fields ())))))))
              (Int 2 (
                (rel ((name "") (fields ())))
                (field (
                  (name  "")
                  (dtype DBool)
                  (relation ((name "") (fields ()))))))))))))
         (((rel ((name "") (fields ())))
           (field (
             (name  "")
             (dtype DBool)
             (relation ((name "") (fields ())))))
           (value (Int 3)))
          (UnorderedList ((
            CrossTuple (
              (Int 3 (
                (rel ((name "") (fields ())))
                (field (
                  (name  "")
                  (dtype DBool)
                  (relation ((name "") (fields ())))))))
              (Int 4 (
                (rel ((name "") (fields ())))
                (field (
                  (name  "")
                  (dtype DBool)
                  (relation ((name "") (fields ())))))))))))))
        ((field (
           (name  fixme)
           (dtype DBool)
           (relation ((name "") (fields ())))))
         (lookup (
           Field (
             (name  fixme)
             (dtype DBool)
             (relation ((name "") (fields ())))))))) |}]
end
