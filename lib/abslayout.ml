open Base
open Stdio
open Printf
open Expect_test_helpers_kernel

open Collections
open Db
open Layout

module No_config = struct
  include Abslayout0

  module Name = struct
    module T = struct
      type t = name = {
        relation : string option;
        name : string;
        type_ : Type.PrimType.t option [@compare.ignore];
      } [@@deriving compare, hash, sexp]

      let module_name = "Dblayout.Abslayout.Name"

      let to_string : t -> string = fun { relation; name; type_ } ->
        let out = match relation with
          | Some r -> r ^ "."
          | None -> ""
        in
        let out = out ^ name in
        let out = match type_ with
          | Some t -> failwith ""
          | None -> out
        in
        out

      let of_string : string -> t = fun s ->
        let lexbuf = Lexing.from_string s in
        try Ralgebra_parser.name_eof Ralgebra_lexer.token lexbuf with
        | Ralgebra0.ParseError (msg, line, col) as e ->
          Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col);
          raise e

      let to_typed_name : t -> Type.TypedName.t = function
        | { name; type_ = Some t } -> (name, t)
        | { type_ = None } -> failwith "Missing type."
    end
    include T
    include Identifiable.Make(T)

    let to_sql = fun { relation; name } -> match relation with
      | Some r -> sprintf "%s.\"%s\"" r name
      | None -> sprintf "\"%s\"" name
  end

  module Ctx = struct
    type t = primvalue Map.M(Name).t [@@deriving compare, sexp]

    let of_tuple : Tuple.t -> t = fun t ->
      List.fold_left t ~init:(Map.empty (module Name)) ~f:(fun m v ->
          let n = {
            relation = Some v.rel.name;
            name = v.field.name;
            type_ = Some (Type.PrimType.of_primvalue v.value)
          } in
          Map.set m ~key:n ~data:v.value)

    (* let of_vars : (string * primvalue) list -> t = fun l ->
     *   List.fold_left l ~init:(Map.empty (module Key)) ~f:(fun m (k, v) ->
     *       Map.set m ~key:(Var (k, PrimType.of_primvalue v))
     *         ~data:v) *)
  end


  let of_lexbuf_exn : Lexing.lexbuf -> (name, (name, string) Abslayout0.layout) ralgebra = fun lexbuf ->
    try Ralgebra_parser.abs_ralgebra_eof Ralgebra_lexer.token lexbuf with
    | Ralgebra0.ParseError (msg, line, col) as e ->
      Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col);
      raise e

  let of_channel_exn : In_channel.t -> (name, (name, string) Abslayout0.layout) ralgebra = fun ch -> of_lexbuf_exn (Lexing.from_channel ch)

  let of_string_exn : string -> (name, (name, string) Abslayout0.layout) ralgebra = fun s -> of_lexbuf_exn (Lexing.from_string s)

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
        method zero = Set.empty (module Type.TypedName)
        method plus = Set.union
        method visit_Name () n =
          if Option.is_none n.relation then
            Set.singleton (module Type.TypedName) (Name.to_typed_name n)
          else self#zero
        method visit_'l () l = self#zero
        method visit_'f _ _ = self#zero
      end in
      params#visit_ralgebra () r

  let resolve :
    Postgresql.connection
    -> ctx:Set.M(Name).t
    -> (name, (name, string) Abslayout0.layout) ralgebra
    -> (name, (name, string) Abslayout0.layout) ralgebra =
    fun conn ~ctx r ->
      let param_ctx = ctx in

      let resolve_relation r_name =
        let r = Relation.from_db conn r_name in
        let ctx =
          List.map r.fields ~f:(fun f -> {
                relation = Some r.name;
                name = f.name;
                type_ = Some (Type.PrimType.of_dtype f.Db.dtype)
              })
          |> Set.of_list (module Name)
          |> Set.union param_ctx
        in
        Logs.debug (fun m -> m "Relation %s: %s" r_name ([%sexp_of:Set.M(Name).t] ctx |> Sexp.to_string_hum));
        Logs.debug (fun m -> m "%s" ([%sexp_of:Field.t list] r.fields |> Sexp.to_string_hum));
        r_name, ctx
      in

      let rename name = Set.map (module Name) ~f:(fun n ->
          { n with relation = Option.map n.relation ~f:(fun _ -> name) })
      in

      let ralgebra_resolver = object
          inherit [_] ralgebra_map as super
          method visit_'f ctx n =
            match Set.find ctx ~f:(fun n' -> Name.(n = n')) with
            | Some n -> n
            | None -> Error.create "Could not resolve." (n, ctx)
                        [%sexp_of:name * Set.M(Name).t] |> Error.raise
          method visit_'l _ _ = failwith ""
        end in
      let resolve_agg ctx = ralgebra_resolver#visit_agg ctx in
      let resolve_pred ctx = ralgebra_resolver#visit_pred ctx in

      let rec resolve_ralgebra_inner ctx = function
        | Select (preds, r) ->
          let r, ctx = resolve_ralgebra_inner ctx r in
          let preds = List.map preds ~f:(resolve_pred ctx) in
          let ctx =
            List.filter_map preds ~f:(function Name n -> Some n | _ -> None)
            |> Set.of_list (module Name)
          in
          Select (preds, r), ctx
        | Filter (pred, r) ->
          let r, ctx = resolve_ralgebra_inner ctx r in
          let pred = resolve_pred ctx pred in
          Filter (pred, r), ctx
        | Join ({ pred; r1_name; r1; r2_name; r2 } as join) ->
          let r1, ctx1 = resolve_ralgebra_inner ctx r1 in
          let r2, ctx2 = resolve_ralgebra_inner ctx1 r2 in
          let ctx = Set.union (rename r1_name ctx1) (rename r2_name ctx2) in
          let pred = resolve_pred ctx pred in
          Join { join with pred; r1; r2 }, ctx
        | Scan l ->
          let l, ctx = resolve_relation l in
          Scan l, ctx
        | Agg (aggs, key, r) ->
          let r, ctx = resolve_ralgebra_inner ctx r in
          let aggs = List.map ~f:(resolve_agg ctx) aggs in
          let key = List.map key ~f:(fun n ->
              Option.value_exn (Set.find ctx ~f:(fun n' -> Name.(n = n'))))
          in
          Agg (aggs, key, r), param_ctx
        | Dedup r ->
          let r, ctx = resolve_ralgebra_inner ctx r in
          Dedup r, ctx
      in

      let rec resolve_layout runtime_ctx compile_ctx =
        let resolve_r ctx = resolve_ralgebra_inner ctx in
        function
        | AEmpty -> AEmpty, Set.empty (module Name)
        | AScalar p ->
          let p = resolve_pred compile_ctx p in
          let ctx = match p with
            | Name n -> Set.singleton (module Name) n
            | _ -> Set.empty (module Name)
          in
          AScalar p, ctx
        | AList (r, n, l) ->
          let r, compile_ctx = resolve_r compile_ctx r in
          let compile_ctx = rename n compile_ctx in
          let l, ctx = resolve_layout runtime_ctx compile_ctx l in
          AList (r, n, l), ctx
        | ATuple (ls, t) ->
          let (ls, ctxs) =
            List.map ls ~f:(resolve_layout runtime_ctx compile_ctx)
            |> List.unzip
          in
          let ctx = Set.union_list (module Name) ctxs in
          ATuple (ls, t), ctx
        | AHashIdx (r, n, l, m) ->
          let r, compile_ctx = resolve_r compile_ctx r in
          let compile_ctx = rename n compile_ctx in
          let l, ctx = resolve_layout runtime_ctx compile_ctx l in
          let m = (object
            inherit [_] map as super
            method visit_'f _ _ = failwith ""
            method visit_'r _ _ = failwith ""
            method visit_ralgebra _ _ = failwith ""
            method visit_pred _ = resolve_pred
          end)#visit_hash_idx runtime_ctx m
          in
          AHashIdx (r, n, l, m), ctx
        | AOrderedIdx (r, n, l, m) ->
          let r, compile_ctx = resolve_r compile_ctx r in
          let compile_ctx = rename n compile_ctx in
          let l, ctx = resolve_layout runtime_ctx compile_ctx l in
          let m = (object
            inherit [_] map as super
            method visit_'f _ _ = failwith ""
            method visit_'r _ _ = failwith ""
            method visit_ralgebra _ _ = failwith ""
            method visit_pred _ = resolve_pred
          end)#visit_ordered_idx runtime_ctx m
          in
          AOrderedIdx (r, n, l, m), ctx
      in

      let rec resolve_ralgebra ctx = function
        | Select (preds, r) ->
          let r, ctx = resolve_ralgebra ctx r in
          let ctx = Set.union param_ctx ctx in
          let preds = List.map preds ~f:(resolve_pred ctx) in
          let ctx =
            List.filter_map preds ~f:(function Name n -> Some n | _ -> None)
            |> Set.of_list (module Name)
          in
          Select (preds, r), ctx
        | Filter (pred, r) ->
          let r, ctx = resolve_ralgebra ctx r in
          let ctx = Set.union param_ctx ctx in
          let pred = resolve_pred ctx pred in
          Filter (pred, r), ctx
        | Join ({ pred; r1_name; r1; r2_name; r2 } as join) ->
          let r1, ctx1 = resolve_ralgebra ctx r1 in
          let ctx1 = Set.union param_ctx ctx1 in
          let r2, ctx2 = resolve_ralgebra ctx1 r2 in
          let ctx2 = Set.union param_ctx ctx2 in
          let ctx = Set.union (rename r1_name ctx1) (rename r2_name ctx2) in
          let pred = resolve_pred ctx pred in
          Join { join with pred; r1; r2 }, ctx
        | Scan l ->
          let l, ctx = resolve_layout ctx (Set.empty (module Name)) l in
          let ctx = Set.union param_ctx ctx in
          Scan l, ctx
        | Agg (aggs, key, r) ->
          let r, ctx = resolve_ralgebra ctx r in
          let ctx = Set.union param_ctx ctx in
          let aggs = List.map ~f:(resolve_agg ctx) aggs in
          let key = List.map key ~f:(fun n ->
              Option.value_exn (Set.find ctx ~f:(fun n' -> Name.(n = n'))))
          in
          Agg (aggs, key, r), param_ctx
        | Dedup r ->
          let r, ctx = resolve_ralgebra ctx r in
          let ctx = Set.union param_ctx ctx in
          Dedup r, ctx
      in
      let (r, _) = resolve_ralgebra param_ctx r in
      r

  let pred_of_value : Db.primvalue -> 'a pred = function
    | `Bool x -> Bool x
    | `String x -> String x
    | `Int x -> Int x
    | `Null -> Null
    | `Unknown x -> String x

  let rec eval_pred : Ctx.t -> name pred -> primvalue =
    let raise = Error.raise in
    fun ctx -> function
      | Null -> `Null
      | Int x -> `Int x
      | String x -> `String x
      | Bool x -> `Bool x
      | Name n ->
        begin match Map.find ctx n with
          | Some v -> v
          | None -> Error.create "Unbound variable." (n, ctx)
                      [%sexp_of:name * Ctx.t] |> raise
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

  let subst : Ctx.t -> (name, 'a) ralgebra -> (name, 'a) ralgebra =
    fun ctx ->
      let v = object
        inherit [_] ralgebra_endo as super

        method! visit_Name _ this v =
          match Map.find ctx v with
          | Some x -> pred_of_value x
          | None -> this

        method visit_'l _ x = x
        method visit_'f _ x = x
        method visit_agg _ x = x
      end in
      v#visit_ralgebra ()

  let%expect_test "subst" =
    let f = Name.of_string "f" in
    let g = Name.of_string "g" in
    let r = "r" in
    let ctx = Map.of_alist_exn (module Name) [f, `Int 1; g, `Int 2] in
    let r = Filter (Binop (Eq, Name f, Name g), Select ([Name f; Name g], Scan r)) in
    print_s ([%sexp_of:(name, string) ralgebra] (subst ctx r));
    [%expect {|
    (Filter
      (Binop (
        Eq
        (Int 1)
        (Int 2)))
      (Select
        ((Int 1)
         (Int 2))
        (Scan r))) |}]

  let pred_relations : name pred -> string list = fun p ->
    let rels = ref [] in
    let f = object
      inherit [_] ralgebra_iter as super
      method visit_Name () = function
        | { relation = Some r } -> rels := r::!rels
        | { relation = None } -> ()
      method visit_'l () x = ()
      method visit_'f () x = ()
      method visit_agg () x = ()
    end in
    f#visit_pred () p; !rels

  let rec pred_to_sql : _ pred -> string = function
    | Name n -> sprintf "%s" (Name.to_sql n)
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

  let ralgebra_to_sql : (_, string) ralgebra -> string = fun r ->
    let fresh =
      let x = ref 0 in
      fun () -> Caml.incr x; !x
    in
    let subst_table table =
      let f = object
        inherit [_] ralgebra_map as super
        method visit_Name _ n =
          let r' = sprintf "t%d" table in
          Name { n with relation = Option.map n.relation ~f:(fun _ -> r') }
        method visit_'l = failwith "Unused"
        method visit_'f = failwith "Unused"
        method visit_agg = failwith "Unused"
      end in
      f#visit_pred ()
    in
    let subst_table_prefix subs =
      let f = object
        inherit [_] ralgebra_map as super
        method visit_Name _ n =
          Name { n with relation = Option.map n.relation ~f:(fun tname ->
              let (_, tbl) = List.find_exn subs ~f:(fun (tname', tbl) ->
                  String.(tname = tname'))
              in
              sprintf "t%d" tbl )
            }
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
      | Scan r -> sprintf "select * from %s" r
      | Filter (pred, r) ->
        let table = fresh () in
        let pred = subst_table table pred in
        sprintf "select * from (%s) as t%d where %s" (f r) table
          (pred_to_sql pred)
      | Join { pred = p; r1_name; r1; r2_name; r2 } ->
        let t1 = fresh () in
        let t2 = fresh () in
        let pred = subst_table_prefix [r1_name, t1; r2_name, t2] p in
        sprintf "select * from (%s) as t%d, (%s) as t%d where %s"
          (f r1) t1 (f r2) t2 (pred_to_sql pred)
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

  let n s = Name (Name.of_string s)
  let nn r s = Name { relation = Some r; name = s; type_ = None }
  let%expect_test "project-empty" =
    let r = Select ([], Scan ("r")) in
    print_endline (ralgebra_to_sql r);
    [%expect {| select top 0 from (select * from r) as t1 |}]

  let%expect_test "project" =
    let r = Select ([nn "r" "f"], Scan ("r")) in
    print_endline (ralgebra_to_sql r);
    [%expect {| select t1."f" from (select * from r) as t1 |}]

  let%expect_test "filter" =
    let r = Filter (Binop (Eq, nn "r" "f", nn "r" "g"), Scan ("r")) in
    print_endline (ralgebra_to_sql r);
    [%expect {| select * from (select * from r) as t1 where (t1."f") = (t1."g") |}]

  let%expect_test "eqjoin" =
    let r = Join ({ pred = Binop (Eq, nn "r" "f", nn "s" "g");
                    r1_name = "r"; r1 = Scan ("r");
                    r2_name = "s"; r2 = Scan ("s")
                  })
    in
    print_endline (ralgebra_to_sql r);
    [%expect {| select * from (select * from r) as t1, (select * from s) as t2 where (t1."f") = (t2."g") |}]

  let%expect_test "agg" =
    let f = Name.of_string "f" in
    let g = Name.of_string "g" in
    let r = Agg ([Sum g], [f; g], Scan ("r")) in
    print_endline (ralgebra_to_sql r);
    [%expect {| select sum(t1."g") from (select * from r) as t1 group by (t1."f", t1."g") |}]

  let%expect_test "agg" =
    let r = Filter (Binop (Eq, nn "ship_mode" "sm_carrier", String "GERMA"), Scan ("ship_mode")) in
    print_endline (ralgebra_to_sql r);
    [%expect {| select * from (select * from ship_mode) as t1 where (t1."sm_carrier") = ('GERMA') |}]
end
include No_config

module Config = struct
  module type S_shared = sig
    val layout_map : bool
  end

  module type S_db = sig
    include S_shared
    val conn : Postgresql.connection
  end

  module type S = sig
    include S_shared
    val eval : Ctx.t -> (name, string) ralgebra -> primvalue Map.M(String).t Seq.t
  end
end

module Make (Config : Config.S) () = struct
  open Config

  include No_config

  let fresh = Fresh.create ()

  let partition : part:name pred -> lookup:name pred -> (name, string) layout -> (name, string) layout =
    fun ~part:p ~lookup l ->
      let rel = match pred_relations p with
        | [r] -> r
        | rs -> Error.create "Unexpected number of relations." rs
                  [%sexp_of:string list] |> Error.raise
      in
      let domain = Dedup (Select ([p], Scan rel)) in
      let name = Fresh.name fresh "x%d" in
      let pred =
        let subst = object
          inherit [_] ralgebra_endo as super
          method visit_Name _ _ n =
            match n.relation with
            | Some r when String.(rel = r) ->
              Name { n with relation = Some name }
            | _ -> Name n
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
            if String.(rel = rel') then
              Filter (Binop (Eq, p, pred), r) else r
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
                  let field = { relation = Some n; name = key; type_ = None } in
                  Map.set ctx ~key:field ~data)
            in
            self#visit_t ctx l)
      in
      self#build_AList ls

    method visit_ATuple ctx ls kind =
      self#build_ATuple (List.map ~f:(self#visit_t ctx) ls) kind

    method visit_AHashIdx ctx q n l h =
      let kv =
        eval ctx q |> Seq.map ~f:(fun k ->
            let (name, key) = match Map.to_alist k with
              | [(name, key)] -> (name, key)
              | _ -> failwith "Unexpected key tuple shape."
            in
            let field = { relation = Some n; name; type_ = None } in
            let ctx = Map.set ctx ~key:field ~data:key in
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

  let materialize : ?ctx:Ctx.t -> (name, string) layout -> Layout.t =
    fun ?(ctx = Map.empty (module Name)) l ->
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

  let to_type : ?ctx:Ctx.t -> (name, string) layout -> Type.t =
    fun ?(ctx = Map.empty (module Name)) l ->
      Logs.debug (fun m -> m "Computing type of abstract layout: %s"
                     (Sexp.to_string_hum ([%sexp_of:(name, string) layout] l)));
      let type_ = (new type_fold)#visit_t ctx l in
      Logs.debug (fun m -> m "The type is: %s"
                     (Sexp.to_string_hum ([%sexp_of:Type.t] type_)));
      type_

  module S = struct
    open Bitstring
    open Serialize
    open Type

    class ['self] serialize_fold log_ch writer =
      let ctr = ref 0 in
      let labels = ref [] in
      let log_start lbl =
        if Config.layout_map then begin
          labels := (lbl, !ctr, Writer.pos writer) :: !labels;
          Int.incr ctr
        end
      in
      let log_end () = if Config.layout_map then begin
          match !labels with
          | (lbl, ctr, start_pos)::ls ->
            labels := ls;
            let prefix = String.make (List.length !labels) ' ' in
            let end_ = Writer.Pos.to_bits (Writer.pos writer) in
            let start = Writer.Pos.to_bits start_pos in
            let byte_start = Int64.(start / of_int 8) in
            let byte_len = Int64.((end_ - start) / of_int 8) in
            let aligned = if Int64.(start % of_int 8 = of_int 0) then "=" else "~" in
            let out = sprintf "%d %s+ %s [%Ldb %s%LdB (%Ld bytes)]\n"
                ctr prefix lbl start aligned byte_start byte_len
            in
            Out_channel.output_string log_ch out
          | [] -> Logs.warn (fun m -> m "Unexpected log_end.")
        end
      in

      object (self : 'self)
        method visit_AList ctx type_ q n elem_layout =
          match type_ with
          | UnorderedListT (elem_t, { count }) ->
            (* Reserve space for list header. *)
            let header_pos = Writer.pos writer in

            log_start "List count";
            Writer.write_bytes writer (Bytes.make 8 '\x00');
            log_end ();

            log_start "List len";
            Writer.write_bytes writer (Bytes.make 8 '\x00');
            log_end ();

            (* Serialize list body. *)
            log_start "List body";
            let count = ref 0 in
            eval ctx q |> Seq.iter ~f:(fun t ->
                let ctx = Map.fold t ~init:ctx ~f:(fun ~key ~data ctx -> 
                    let field = { relation = Some n; name = key; type_ = None } in
                    Map.set ctx ~key:field ~data)
                in
                Caml.incr count;
                self#visit_t ctx elem_t elem_layout);
            let end_pos = Writer.pos writer in
            log_end ();

            (* Serialize list header. *)
            Writer.seek writer header_pos;
            let len = Writer.Pos.(header_pos - end_pos |> to_bytes_exn) |> Int64.to_int_exn in
            Writer.write writer (of_int ~width:64 !count);
            Writer.write writer (of_int ~width:64 len);

            Writer.seek writer end_pos

          | t -> Error.(create "Unexpected layout type." t [%sexp_of:Type.t] |> raise)

        method visit_ATuple ctx type_ elem_layouts kind =
          match type_ with
          | CrossTupleT (elem_ts, _) | ZipTupleT (elem_ts, _) ->
            (* Reserve space for header. *)
            let header_pos = Writer.pos writer in

            log_start "Tuple len";
            Writer.write_bytes writer (Bytes.make 8 '\x00');
            log_end ();

            (* Serialize body *)
            log_start "Tuple body";
            List.iter2_exn ~f:(fun t l -> self#visit_t ctx t l) elem_ts elem_layouts;
            log_end ();

            let end_pos = Writer.pos writer in

            (* Serialize header. *)
            Writer.seek writer header_pos;
            let len = Writer.Pos.(header_pos - end_pos |> to_bytes_exn) |> Int64.to_int_exn in
            Writer.write writer (of_int ~width:64 len);
            Writer.seek writer end_pos
          | t -> Error.(create "Unexpected layout type." t [%sexp_of:Type.t] |> raise)

        method visit_AHashIdx ctx type_ q n l h =
          match type_ with
          | TableT (key_t, value_t, _) ->
            let key_name = ref "" in
            let keys =
              eval ctx q
              |> Seq.map ~f:(fun k ->
                  let (name, key) = match Map.to_alist k with
                    | [(name, key)] -> (name, key)
                    | _ -> failwith "Unexpected key tuple shape."
                  in
                  key_name := name;
                  key)
              |> Seq.map ~f:(fun k -> k, serialize key_t (Layout.of_value (Value.of_primvalue k)))
              |> Seq.to_list
            in
            Logs.debug (fun m -> m "Generating hash for %d keys." (List.length keys));
            let hash = Cmph.(List.map keys ~f:(fun (_, b) -> to_string b)
                             |> KeySet.of_fixed_width
                             |> Config.create ~seed:0 ~algo:`Chd |> Hash.of_config)
            in
            let keys = List.map keys ~f:(fun (k, b) ->
                (k, b, Cmph.Hash.hash hash (to_string b)))
            in

            let hash_body = Cmph.Hash.to_packed hash |> Bytes.of_string |> align isize in
            let hash_len = Bytes.length hash_body in

            let table_size =
              List.fold_left keys ~f:(fun m (_, _, h) -> Int.max m h) ~init:0
              |> fun m -> m + 1
            in

            let header_pos = Writer.pos writer in

            log_start "Table len";
            Writer.write_bytes writer (Bytes.make 8 '\x00');
            log_end ();

            log_start "Table hash len";
            Writer.write_bytes writer (Bytes.make 8 '\x00');
            log_end ();

            log_start "Table hash";
            Writer.write_bytes writer (Bytes.make hash_len '\x00');
            log_end ();

            log_start "Table key map";
            Writer.write_bytes writer (Bytes.make (8 * table_size) '\x00');
            log_end ();

            let hash_table = Array.create ~len:table_size (0xDEADBEEF) in

            log_start "Table values";
            List.iter keys ~f:(fun (k, b, h) ->
                let field = { relation = Some n; name = !key_name; type_ = None } in
                let ctx = Map.set ctx ~key:field ~data:k in
                Writer.write writer b;
                self#visit_t ctx value_t l);
            log_end ();

            let end_pos = Writer.pos writer in

            Writer.seek writer header_pos;
            let len = Writer.Pos.(header_pos - end_pos |> to_bytes_exn) in
            Writer.write writer (of_int ~width:64 (Int64.to_int_exn len));
            Writer.write writer (of_int ~width:64 hash_len);
            Writer.write_bytes writer hash_body;
            Array.iter hash_table ~f:(fun x -> Writer.write writer (of_int ~width:64 x));

            Writer.seek writer end_pos

          | t -> Error.(create "Unexpected layout type." t [%sexp_of:Type.t] |> raise)

        method visit_AEmpty _ _ = ()

        method visit_AScalar ctx type_ e =
          let l = Layout.of_value {
              value = eval_pred ctx e; rel = Relation.dummy; field = Field.dummy
            } in
          let label, bstr = match l.node with
            | Null s -> "Null", serialize_null type_ l s
            | Int (x, s) -> "Int", serialize_int type_ l x s
            | Bool (x, s) -> "Bool", serialize_bool type_ l x s
            | String (x, s) -> "String", serialize_string type_ l x s
            | _ -> failwith "Expected a scalar."
          in
          log_start label;
          Writer.write writer bstr;
          log_end ();

        method visit_AOrderedIdx ctx type_ q n l o = failwith ""

        method visit_t ctx type_ = function
          | AEmpty -> self#visit_AEmpty ctx type_
          | AScalar e -> self#visit_AScalar ctx type_ e
          | AList (r, n, a) -> self#visit_AList ctx type_ r n a
          | ATuple (a, k) -> self#visit_ATuple ctx type_ a k
          | AHashIdx (r, n, a, t) -> self#visit_AHashIdx ctx type_ r n a t
          | AOrderedIdx (_, _, _, _) -> failwith ""
      end
  end
  include S

  let serialize : ?ctx:Ctx.t -> Bitstring.Writer.t -> Type.t -> (name, string) layout -> int =
    fun ?(ctx = Map.empty (module Name)) writer t l ->
      Logs.debug (fun m -> m "Serializing abstract layout: %s"
                     (Sexp.to_string_hum ([%sexp_of:(name, string) layout] l)));
      let open Bitstring in
      let begin_pos = Writer.pos writer in

      let fn = Caml.Filename.temp_file "buf" ".txt" in
      if Config.layout_map then Logs.info (fun m -> m "Outputting layout map to %s." fn);
      Out_channel.with_file fn ~f:(fun ch ->
          (new serialize_fold ch writer)#visit_t ctx t l);
      let end_pos = Writer.pos writer in
      let len = Writer.Pos.(begin_pos - end_pos |> to_bytes_exn) |> Int64.to_int_exn in
      len

  type schema = (string * Type.PrimType.t) list

  let rec pred_to_schema_exn : name pred -> (string * Type.PrimType.t) =
    function
    | Name { name; type_ = Some t } -> (name, t)
    | Name { type_ = None } -> failwith "Missing type."
    | Int _ -> "", IntT
    | Bool _ -> "", BoolT
    | String _ -> "", StringT
    | Null -> failwith ""
    | Binop (op, _, _) | Varop (op, _) -> begin match op with
        | Eq | Lt | Le | Gt | Ge | And | Or -> "", BoolT
        | Add | Sub | Mul | Div | Mod -> "", IntT
      end

  let agg_to_string : name agg -> string = function
    | Count -> "Count(*)"
    | Min f -> sprintf "Min(%s)" (Name.to_string f)
    | Max f -> sprintf "Max(%s)" (Name.to_string f)
    | Sum f -> sprintf "Sum(%s)" (Name.to_string f)
    | Avg f -> sprintf "Avg(%s)" (Name.to_string f)
    | Key f -> Name.to_string f

  let ralgebra_to_schema_exn : ('a -> schema) -> (name, 'a) ralgebra -> schema =
    fun layout_to_schema_exn ->
      let rec ralgebra_to_schema_exn = function
        | Select (exprs, r) -> List.map exprs ~f:(pred_to_schema_exn)
        | Filter (_, r) | Dedup r -> ralgebra_to_schema_exn r
        | Join {r1; r2} ->
          ralgebra_to_schema_exn r1 @ ralgebra_to_schema_exn r2
        | Scan r -> layout_to_schema_exn r
        | Agg (out, _, _) ->
          List.map out ~f:(function
              | Count -> "count", Type.PrimType.IntT
              | Key { name; type_ = Some t } -> name, t
              | Key { type_ = None } -> failwith "Missing type."
              | Sum _ | Min _ | Max _ as a ->
                agg_to_string a, Type.PrimType.IntT
              | Avg _ -> failwith "unsupported")
      in
      ralgebra_to_schema_exn

  let rec layout_to_schema_exn : (name, string) layout -> (string * Type.PrimType.t) list =
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
    let layout_map = Config_db.layout_map
  end
  include Make (Config) ()
end

module Test = struct
  let%expect_test "mat-col" =
    let conn = new Postgresql.connection ~dbname:"tpcds1" () in
    let layout = AList (Filter (Binop (Eq, n "sm_carrier", String "GERMA"), Scan "ship_mode"), "t", AScalar (nn "t" "sm_carrier"))
    in
    let module M = Make_db(struct
        let conn = conn
        let layout_map = false
      end) () in
    M.materialize layout |> [%sexp_of:Layout.t] |> print_s;
    [%expect {|
    (UnorderedList ((
      String "GERMA               " (
        (rel "")
        (field (
          (name  "")
          (dtype DBool))))))) |}]

  let%expect_test "mat-hidx" =
    let conn = new Postgresql.connection ~dbname:"tpcds1" () in
    let layout = AHashIdx (
        Dedup (Select ([n "sm_type"], (Filter ((Binop (Eq, n "sm_type", String "LIBRARY")), Scan "ship_mode")))),
        "t", AList (
          Filter (Binop (Eq, nn "t" "sm_type", n "sm_type"), Scan "ship_mode"),
          "t", AScalar (nn "t" "sm_code")), { lookup = Null })
    in
    let module M = Make_db(struct
        let conn = conn
        let layout_map = false 
      end) () in
    M.materialize layout |> [%sexp_of:Layout.t] |> print_s;
    [%expect {|
    (Table
      ((
        ((rel "")
         (field (
           (name  "")
           (dtype DBool)))
         (value (Unknown "LIBRARY                       ")))
        (UnorderedList (
          (String "AIR       " (
            (rel "")
            (field (
              (name  "")
              (dtype DBool)))))
          (String "SURFACE   " (
            (rel "")
            (field (
              (name  "")
              (dtype DBool)))))
          (String "SEA       " (
            (rel "")
            (field (
              (name  "")
              (dtype DBool)))))))))
      ((field (
         (name  fixme)
         (dtype DBool)))
       (lookup (
         Field (
           (name  fixme)
           (dtype DBool)))))) |}]

  let rels = Hashtbl.create (module String)

  module Eval = struct
    include Eval.Make_relation(struct
        type relation = string [@@deriving compare, sexp]
        let eval_relation r =
          Hashtbl.find_exn rels r |> Seq.of_list
          |> Seq.map ~f:(fun t ->
              List.map t ~f:(fun (n, v) -> {
                    rel = Relation.of_name r;
                    field = Field.of_name n;
                    Value.value = v
                  }))
      end)

    let rec eval_pred : Ctx.t -> name pred -> primvalue =
      fun ctx -> function
        | Null -> `Null
        | Int x -> `Int x
        | String x -> `String x
        | Bool x -> `Bool x
        | Name n ->
          begin match Map.find ctx n with
            | Some v -> v
            | None -> Error.create "Unbound variable." (n, ctx)
                        [%sexp_of:name * Ctx.t] |> Error.raise
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
                     [%sexp_of:Ralgebra.op * primvalue * primvalue]
                   |> Error.raise
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
                     [%sexp_of:Ralgebra.op * primvalue list] |> Error.raise
          end

    let eval_join ctx p r1_name r2_name r1 r2 =
      Seq.concat_map r1 ~f:(fun t1 ->
          Seq.filter r2 ~f:(fun t2 ->
              let ctx =
                ctx
                |> Map.merge_right (Ctx.of_tuple t1)
                |> Map.merge_right (Ctx.of_tuple t2)
              in
              match eval_pred ctx p with
              | `Bool x -> x
              | _ -> failwith "Expected a boolean."))

    let eval_filter ctx p seq =
      Seq.filter seq ~f:(fun t ->
          let ctx = Map.merge_right ctx (Ctx.of_tuple t) in
          match eval_pred ctx p with
          | `Bool x -> x
          | _ -> failwith "Expected a boolean.")

    let eval_dedup seq =
      let set = Hash_set.create (module Tuple) in
      Seq.iter seq ~f:(Hash_set.add set);
      Hash_set.to_list set |> Seq.of_list

    let eval_select ctx out seq =
      Seq.map seq ~f:(fun t ->
          let ctx = Map.merge_right ctx (Ctx.of_tuple t) in
          List.map out ~f:(fun e ->
              let v = eval_pred ctx e |> Value.of_primvalue in
              match e with
              | Name n -> { v with field = Field.of_name n.name }
              | e -> v))

    let eval : Ctx.t -> (name, string) ralgebra -> Tuple.t Seq.t = fun ctx r ->
      let rec eval = function
        | Scan r -> eval_relation r
        | Filter (p, r) -> eval_filter ctx p (eval r)
        | Join {pred = p; r1_name; r1; r2_name; r2} ->
          eval_join ctx p r1_name r2_name (eval r1) (eval r2)
        | Dedup r -> eval_dedup (eval r)
        | Select (out, r) -> eval_select ctx out (eval r)
        | Agg _ -> failwith ""
      in
      eval r
  end

  let create : string -> string list -> int list list -> string * name list = fun name fs xs ->
    let data =
      List.map xs ~f:(fun data ->
          List.map2_exn fs data ~f:(fun fname value ->
              (fname, `Int value)))
    in
    Hashtbl.set rels ~key:name ~data;
    name, List.map fs ~f:(fun f -> { name = f; relation = Some name; type_ = Some IntT })

  module M = Make(struct
      let layout_map = false
      let eval ctx query =
        Eval.eval ctx query
        |> Seq.map ~f:(fun (t: Tuple.t) ->
            List.map t ~f:(fun (v: Value.t) -> v.field.name, v.value)
            |> Map.of_alist_exn (module String))
    end) ()

  [@@@ warning "-8"]
  let r1, [f; g] = create "r1" ["f"; "g"] [[1;2]; [1;3]; [2;1]; [2;2]; [3;4]]
  [@@@ warning "+8"]

  let%expect_test "part-list" =
    let layout = AList (Scan r1, "x", ATuple ([AScalar (nn "x" "f"); AScalar (nn "x" "g")], Cross))
    in
    let part_layout = M.partition ~part:(Name f) ~lookup:(Name f) layout in
    [%sexp_of:(name, string) layout] part_layout |> print_s;
    [%expect {|
      (AHashIdx
        (Dedup (Select ((Name ((relation (r1)) (name f) (type_ (IntT))))) (Scan r1)))
        x0
        (AList
          (Filter
            (Binop (
              Eq
              (Name ((relation (r1)) (name f) (type_ (IntT))))
              (Name ((relation (x0)) (name f) (type_ (IntT))))))
            (Scan r1))
          x
          (ATuple
            ((AScalar (Name ((relation (x)) (name f) (type_ ()))))
             (AScalar (Name ((relation (x)) (name g) (type_ ())))))
            Cross))
        ((lookup (Name ((relation (r1)) (name f) (type_ (IntT))))))) |}];
    [%sexp_of:Type.t] (M.to_type part_layout) |> print_s;
    [%expect {|
      (TableT
        (IntT (
          (range (1 3))
          (nullable false)
          (field (
            (name  "")
            (dtype DBool)))))
        (UnorderedListT
          (CrossTupleT
            ((IntT (
               (range (1 3))
               (nullable false)
               (field (
                 (name  "")
                 (dtype DBool)))))
             (IntT (
               (range (1 4))
               (nullable false)
               (field (
                 (name  "")
                 (dtype DBool))))))
            ((count ((1 1)))))
          ((count ((1 2)))))
        ((count ())
         (field (
           (name  fixme)
           (dtype DBool)))
         (lookup (
           Field (
             (name  fixme)
             (dtype DBool)))))) |}];
    let mat_layout = M.materialize part_layout in
    [%sexp_of:Layout.t] mat_layout |> print_s;
    [%expect {|
      (Table
        ((((rel "")
           (field (
             (name  "")
             (dtype DBool)))
           (value (Int 1)))
          (UnorderedList (
            (CrossTuple (
              (Int 1 (
                (rel "")
                (field (
                  (name  "")
                  (dtype DBool)))))
              (Int 2 (
                (rel "")
                (field (
                  (name  "")
                  (dtype DBool)))))))
            (CrossTuple (
              (Int 1 (
                (rel "")
                (field (
                  (name  "")
                  (dtype DBool)))))
              (Int 3 (
                (rel "")
                (field (
                  (name  "")
                  (dtype DBool))))))))))
         (((rel "")
           (field (
             (name  "")
             (dtype DBool)))
           (value (Int 2)))
          (UnorderedList (
            (CrossTuple (
              (Int 2 (
                (rel "")
                (field (
                  (name  "")
                  (dtype DBool)))))
              (Int 1 (
                (rel "")
                (field (
                  (name  "")
                  (dtype DBool)))))))
            (CrossTuple (
              (Int 2 (
                (rel "")
                (field (
                  (name  "")
                  (dtype DBool)))))
              (Int 2 (
                (rel "")
                (field (
                  (name  "")
                  (dtype DBool))))))))))
         (((rel "")
           (field (
             (name  "")
             (dtype DBool)))
           (value (Int 3)))
          (UnorderedList ((
            CrossTuple (
              (Int 3 (
                (rel "")
                (field (
                  (name  "")
                  (dtype DBool)))))
              (Int 4 (
                (rel "")
                (field (
                  (name  "")
                  (dtype DBool)))))))))))
        ((field (
           (name  fixme)
           (dtype DBool)))
         (lookup (
           Field (
             (name  fixme)
             (dtype DBool)))))) |}]
end
