open Core
open Base
open Stdio
open Printf
open Expect_test_helpers_kernel
open Collections
open Db
open Layout

module No_config = struct
  include Abslayout0

  let select a b c = {node= Select (a, b, c); meta= Univ_map.empty}

  let filter a b c = {node= Filter (a, b, c); meta= Univ_map.empty}

  let agg a b c d = {node= Agg (a, b, c, d); meta= Univ_map.empty}

  let dedup a = {node= Dedup a; meta= Univ_map.empty}

  let scan a = {node= Scan a; meta= Univ_map.empty}

  let empty = {node= AEmpty; meta= Univ_map.empty}

  let scalar a = {node= AScalar a; meta= Univ_map.empty}

  let list a b c = {node= AList (a, b, c); meta= Univ_map.empty}

  let tuple a b = {node= ATuple (a, b); meta= Univ_map.empty}

  let hash_idx a b c d = {node= AHashIdx (a, b, c, d); meta= Univ_map.empty}

  let ordered_idx a b c d = {node= AOrderedIdx (a, b, c, d); meta= Univ_map.empty}

  module Name = struct
    module T = struct
      type t = Abslayout0.name =
        { relation: string option
        ; name: string
        ; type_: Type0.PrimType.t option [@compare.ignore] }
      [@@deriving compare, hash, sexp]
    end

    include T
    include Comparable.Make (T)

    let type_exn {type_; _} =
      match type_ with Some t -> t | None -> failwith "missing type"

    let to_typed_name ({name; _} as n) = (name, type_exn n)

    let to_sql {relation; name; _} =
      match relation with
      | Some r -> sprintf "%s.\"%s\"" r name
      | None -> sprintf "\"%s\"" name

    let of_lexbuf_exn lexbuf =
      try Ralgebra_parser.name_eof Ralgebra_lexer.token lexbuf
      with Ralgebra0.ParseError (msg, line, col) as e ->
        Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
        raise e

    let of_channel_exn ch = of_lexbuf_exn (Lexing.from_channel ch)

    let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

    let of_field ?rel f =
      {relation= rel; name= f.fname; type_= Some (Type.PrimType.of_dtype f.dtype)}
  end

  module Meta = struct
    type pos = Pos of int64 | Many_pos [@@deriving sexp]

    let schema = Univ_map.Key.create ~name:"schema" [%sexp_of : Name.t list]

    let pos = Univ_map.Key.create ~name:"pos" [%sexp_of : pos]

    let meta_mapper f =
      object
        inherit [_] map
        method visit_'m () meta = f meta
        method visit_'f () x = x
      end

    let map ~f r = (meta_mapper f)#visit_ralgebra () r

    let to_mutable = map ~f:ref

    let to_immutable = map ~f:( ! )

    let change r key ~f =
      (meta_mapper (fun m -> Univ_map.change m key ~f))#visit_ralgebra () r

    let init ~init r = map ~f:(fun _ -> init) r
  end

  module Ctx = struct
    type t = primvalue Map.M(Name).t [@@deriving sexp]

    let of_tuple : Tuple.t -> t =
     fun t ->
      List.fold_left t
        ~init:(Map.empty (module Name))
        ~f:(fun m v ->
          let n =
            { relation= Some v.rel.rname
            ; name= v.field.fname
            ; type_= Some (Type.PrimType.of_primvalue v.value) }
          in
          Map.set m ~key:n ~data:v.value )
  end

  let of_lexbuf_exn lexbuf =
    try Ralgebra_parser.abs_ralgebra_eof Ralgebra_lexer.token lexbuf
    with Ralgebra0.ParseError (msg, line, col) as e ->
      Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
      raise e

  let of_channel_exn ch = of_lexbuf_exn (Lexing.from_channel ch)

  let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

  let relations r =
    let ralgebra_relations =
      object (self)
        inherit [_] reduce
        method zero = []
        method plus = ( @ )
        method! visit_Scan _ r = [r]
        method visit_'f _ _ = self#zero
        method visit_'m _ _ = self#zero
      end
    in
    ralgebra_relations#visit_ralgebra () r

  let params r =
    let ralgebra_params =
      object (self)
        inherit [_] reduce
        method zero = Set.empty (module Type.TypedName)
        method plus = Set.union
        method! visit_Name () n =
          if Option.is_none n.relation then
            Set.singleton (module Type.TypedName) (Name.to_typed_name n)
          else self#zero
        method visit_'f _ _ = self#zero
        method visit_'m _ _ = self#zero
      end
    in
    ralgebra_params#visit_ralgebra () r

  (** Annotate names in an algebra expression with types. *)
  let resolve conn r =
    let resolve_relation r_name =
      let r = Relation.from_db conn r_name in
      List.map r.fields ~f:(fun f ->
          { relation= Some r.rname
          ; name= f.fname
          ; type_= Some (Type.PrimType.of_dtype f.Db.dtype) } )
      |> Set.of_list (module Name)
    in
    let rename name s =
      Set.map
        (module Name)
        s
        ~f:(fun n -> {n with relation= Option.map n.relation ~f:(fun _ -> name)})
    in
    let resolve_name ctx n =
      match n.type_ with
      | Some _ -> n
      | None ->
        match
          Set.find ctx ~f:(fun n' ->
              Polymorphic_compare.(n.name = n'.name && n.relation = n'.relation) )
        with
        | Some n -> n
        | None ->
            Error.create "Could not resolve." (n, ctx)
              [%sexp_of : Name.t * Set.M(Name).t]
            |> Error.raise
    in
    let ralgebra_resolver =
      object
        inherit [_] map
        method visit_'f = resolve_name
        method visit_'m _ x = x
      end
    in
    let empty_ctx = Set.empty (module Name) in
    let resolve_agg ctx = ralgebra_resolver#visit_agg ctx in
    let resolve_pred ctx = ralgebra_resolver#visit_pred ctx in
    let preds_to_names preds =
      List.filter_map preds ~f:(function Name n -> Some n | _ -> None)
      |> Set.of_list (module Name)
    in
    let aggs_to_names aggs =
      List.filter_map aggs ~f:(function Key n -> Some n | _ -> None)
      |> Set.of_list (module Name)
    in
    let rec resolve ctx {node; meta} =
      let node', ctx' =
        match node with
        | Select (rel, preds, r) ->
            let r, preds =
              let r, ctx = resolve ctx r in
              let pctx = rename rel ctx in
              (r, List.map preds ~f:(resolve_pred pctx))
            in
            (Select (rel, preds, r), preds_to_names preds)
        | Filter (rel, pred, r) ->
            let r, ctx = resolve ctx r in
            let pred = resolve_pred (rename rel ctx) pred in
            (Filter (rel, pred, r), ctx)
        | Join ({pred; r1_name; r1; r2_name; r2} as join) ->
            let r1, ctx1 = resolve ctx r1 in
            let r2, ctx2 = resolve ctx1 r2 in
            let ctx = Set.union (rename r1_name ctx1) (rename r2_name ctx2) in
            let pred = resolve_pred ctx pred in
            (Join {join with pred; r1; r2}, ctx)
        | Scan l -> (Scan l, resolve_relation l)
        | Agg (rel, aggs, key, r) ->
            let r, aggs, key =
              let r, ctx = resolve ctx r in
              let ctx = rename rel ctx in
              let aggs = List.map ~f:(resolve_agg ctx) aggs in
              let key = List.map key ~f:(ralgebra_resolver#visit_'f ctx) in
              (r, aggs, key)
            in
            (Agg (rel, aggs, key, r), aggs_to_names aggs)
        | Dedup r ->
            let r, ctx = resolve ctx r in
            (Dedup r, ctx)
        | AEmpty -> (AEmpty, empty_ctx)
        | AScalar p ->
            let p = resolve_pred ctx p in
            let ctx =
              match p with
              | Name n -> Set.singleton (module Name) n
              | _ -> Set.empty (module Name)
            in
            (AScalar p, ctx)
        | AList (r, n, l) ->
            let r, l, ctx =
              let r, ctx = resolve ctx r in
              let l, ctx = resolve (rename n ctx) l in
              (r, l, ctx)
            in
            (AList (r, n, l), ctx)
        | ATuple (ls, t) ->
            let ls, ctxs = List.map ls ~f:(resolve ctx) |> List.unzip in
            let ctx = Set.union_list (module Name) ctxs in
            (ATuple (ls, t), ctx)
        | AHashIdx (r, n, l, m) ->
            let r, ctx = resolve ctx r in
            let l, ctx = resolve (rename n ctx) l in
            let m =
              (object
                 inherit [_] map
                 method visit_'f _ _ = failwith ""
                 method visit_'m _ _ = failwith ""
                 method! visit_pred _ = resolve_pred ctx
              end)
                #visit_hash_idx () m
            in
            (AHashIdx (r, n, l, m), ctx)
        | AOrderedIdx (r, n, l, m) ->
            let r, ctx = resolve ctx r in
            let l, ctx = resolve (rename n ctx) l in
            let m =
              (object
                 inherit [_] map
                 method visit_'f _ _ = failwith ""
                 method visit_'m _ _ = failwith ""
                 method! visit_pred _ = resolve_pred ctx
              end)
                #visit_ordered_idx () m
            in
            (AOrderedIdx (r, n, l, m), ctx)
      in
      ({node= node'; meta}, ctx')
    in
    let r, _ = resolve (Set.empty (module Name)) r in
    r

  let pred_of_value : Db.primvalue -> 'a pred = function
    | `Bool x -> Bool x
    | `String x -> String x
    | `Int x -> Int x
    | `Null -> Null
    | `Unknown x -> String x

  let rec eval_pred =
    let raise = Error.raise in
    fun ctx -> function
      | Null -> `Null
      | Int x -> `Int x
      | String x -> `String x
      | Bool x -> `Bool x
      | Name n -> (
        match Map.find ctx n with
        | Some v -> v
        | None ->
            Error.create "Unbound variable." (n, ctx) [%sexp_of : Name.t * Ctx.t]
            |> raise )
      | Binop (op, p1, p2) -> (
          let v1 = eval_pred ctx p1 in
          let v2 = eval_pred ctx p2 in
          match (op, v1, v2) with
          | Eq, `Null, _ | Eq, _, `Null -> `Bool false
          | Eq, `Bool x1, `Bool x2 -> `Bool Bool.(x1 = x2)
          | Eq, `Int x1, `Int x2 -> `Bool Int.(x1 = x2)
          | Eq, `String x1, `String x2 -> `Bool String.(x1 = x2)
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
          | _ ->
              Error.create "Unexpected argument types." (op, v1, v2)
                [%sexp_of : Ralgebra0.op * primvalue * primvalue]
              |> raise )
      | Varop (op, ps) ->
          let vs = List.map ps ~f:(eval_pred ctx) in
          match op with
          | And ->
              List.for_all vs ~f:(function
                | `Bool x -> x
                | _ -> failwith "Unexpected argument type." )
              |> fun x -> `Bool x
          | Or ->
              List.exists vs ~f:(function
                | `Bool x -> x
                | _ -> failwith "Unexpected argument type." )
              |> fun x -> `Bool x
          | _ ->
              Error.create "Unexpected argument types." (op, vs)
                [%sexp_of : Ralgebra0.op * primvalue list]
              |> raise

  let subst ctx =
    let v =
      object
        inherit [_] endo
        method! visit_Name _ this v =
          match Map.find ctx v with Some x -> pred_of_value x | None -> this
        method visit_'f _ x = x
        method visit_'m _ x = x
      end
    in
    v#visit_ralgebra ()

  let%expect_test "subst" =
    let n = Name.of_string_exn in
    let f = n "r.f" in
    let g = n "r.g" in
    let ctx = Map.of_alist_exn (module Name) [(f, `Int 1); (g, `Int 2)] in
    let r = "Filter(r -> r.f = r.g, Select(r -> [r.f, r.g], r))" |> of_string_exn in
    print_s ([%sexp_of : (Name.t, Univ_map.t) ralgebra] (subst ctx r)) ;
    [%expect
      {|
    (Filter r
      (Binop (
        Eq
        (Int 1)
        (Int 2)))
      (Select r
        ((Int 1)
         (Int 2))
        (Scan r))) |}]

  let pred_relations p =
    let rels = ref [] in
    let f =
      object
        inherit [_] iter
        method! visit_Name () =
          function
          | {relation= Some r; _} -> rels := r :: !rels | {relation= None; _} -> ()
        method visit_'f () _ = ()
        method visit_'m () _ = ()
      end
    in
    f#visit_pred () p ; !rels

  let rec pred_to_sql = function
    | Name n -> sprintf "%s" (Name.to_sql n)
    | Int x -> Int.to_string x
    | Bool true -> "true"
    | Bool false -> "false"
    | String s -> sprintf "'%s'" s
    | Null -> "null"
    | Binop (op, p1, p2) -> (
        let s1 = sprintf "(%s)" (pred_to_sql p1) in
        let s2 = sprintf "(%s)" (pred_to_sql p2) in
        match op with
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
        | Mod -> sprintf "%s %% %s" s1 s2 )
    | Varop (op, ps) ->
        let ss = List.map ps ~f:(fun p -> sprintf "(%s)" (pred_to_sql p)) in
        match op with
        | And -> String.concat ss ~sep:" and "
        | Or -> String.concat ss ~sep:" or "
        | _ -> failwith "Unsupported op."

  let ralgebra_to_sql r =
    let rec f {node; _} =
      match node with
      | Select (rel, [], r) -> sprintf "select top 0 from (%s) as %s" (f r) rel
      | Select (rel, fs, r) ->
          let fields = List.map fs ~f:pred_to_sql |> String.concat ~sep:"," in
          sprintf "select %s from (%s) as %s" fields (f r) rel
      | Scan r -> sprintf "select * from %s" r
      | Filter (rel, pred, r) ->
          sprintf "select * from (%s) as %s where %s" (f r) rel (pred_to_sql pred)
      | Join {pred; r1_name; r1; r2_name; r2} ->
          sprintf "select * from (%s) as %s, (%s) as %s where %s" (f r1) r1_name (f r2)
            r2_name (pred_to_sql pred)
      | Agg (rel, aggs, key, r) ->
          let aggs =
            List.map aggs ~f:(function
              | Count -> "count(*)"
              | Key f -> sprintf "%s.\"%s\"" rel f.name
              | Sum f -> sprintf "sum(%s.\"%s\")" rel f.name
              | Avg f -> sprintf "avg(%s.\"%s\")" rel f.name
              | Min f -> sprintf "min(%s.\"%s\")" rel f.name
              | Max f -> sprintf "max(%s.\"%s\")" rel f.name )
            |> String.concat ~sep:", "
          in
          let key =
            List.map key ~f:(fun f -> sprintf "%s.\"%s\"" rel f.name)
            |> String.concat ~sep:", "
          in
          sprintf "select %s from (%s) as %s group by (%s)" aggs (f r) rel key
      | Dedup r -> sprintf "select distinct * from (%s) as t" (f r)
      | _ -> Error.of_string "Only relational algebra constructs allowed." |> Error.raise
    in
    f r

  let%expect_test "project-empty" =
    let r = of_string_exn "Select(r -> [], r)" in
    print_endline (ralgebra_to_sql r) ;
    [%expect {| select top 0 from (select * from r) as r |}]

  let%expect_test "project" =
    let r = of_string_exn "Select(r -> [r.r], r)" in
    print_endline (ralgebra_to_sql r) ;
    [%expect {| select r."r" from (select * from r) as r |}]

  let%expect_test "filter" =
    let r = of_string_exn "Filter(r -> r.f = r.g, r)" in
    print_endline (ralgebra_to_sql r) ;
    [%expect {| select * from (select * from r) as r where (r."f") = (r."g") |}]

  let%expect_test "eqjoin" =
    let r = of_string_exn "Join(r.f = s.g, r as r, s as s)" in
    print_endline (ralgebra_to_sql r) ;
    [%expect
      {| select * from (select * from r) as r, (select * from s) as s where (r."f") = (s."g") |}]

  let%expect_test "agg" =
    let r = of_string_exn "Agg(r -> [Sum(r.g)], [r.f, r.g], r)" in
    print_endline (ralgebra_to_sql r) ;
    [%expect {| select sum(r."g") from (select * from r) as r group by (r."f", r."g") |}]

  let%expect_test "agg" =
    let r =
      of_string_exn "Filter(ship_mode -> ship_mode.sm_carrier = \"GERMA\", ship_mode)"
    in
    print_endline (ralgebra_to_sql r) ;
    [%expect
      {| select * from (select * from ship_mode) as ship_mode where (ship_mode."sm_carrier") = ('GERMA') |}]
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

    val eval : Ctx.t -> (Name.t, 'a) ralgebra -> primvalue Map.M(String).t Seq.t
  end
end

module Make (Config : Config.S) () = struct
  open Config
  include No_config

  let fresh = Fresh.create ()

  let partition ~part:p ~lookup l =
    let rel =
      match pred_relations p with
      | [r] -> r
      | rs ->
          Error.create "Unexpected number of relations." rs [%sexp_of : string list]
          |> Error.raise
    in
    let domain = dedup (select rel [p] (scan rel)) in
    let name = Fresh.name fresh "x%d" in
    let pred =
      let subst =
        object
          inherit [_] endo
          method! visit_Name _ _ n =
            match n.relation with
            | Some r when String.(rel = r) -> Name {n with relation= Some name}
            | _ -> Name n
          method visit_'f _ x = x
          method visit_'m _ x = x
        end
      in
      subst#visit_pred () p
    in
    let layout =
      let subst =
        object
          inherit [_] endo
          method! visit_Scan () r rel' =
            if String.(rel = rel') then
              Filter (rel, Binop (Eq, p, pred), {node= r; meta= Univ_map.empty})
            else r
          method visit_'f _ x = x
          method visit_'m _ x = x
        end
      in
      subst#visit_ralgebra () l
    in
    hash_idx domain name layout {lookup}

  class virtual ['self] material_fold =
    object (self: 'self)
      method virtual build_AList : _
      method virtual build_ATuple : _
      method virtual build_AHashIdx : _
      method virtual build_AOrderedIdx : _
      method virtual build_AEmpty : _
      method virtual build_AScalar : _
      method virtual build_Select : _
      method virtual build_Filter : _
      method virtual build_Join : _
      method visit_AList ctx q n l =
        let ls =
          eval ctx q
          |> Seq.map ~f:(fun t ->
                 let ctx =
                   Map.fold t ~init:ctx ~f:(fun ~key ~data ctx ->
                       let field = {relation= Some n; name= key; type_= None} in
                       Map.set ctx ~key:field ~data )
                 in
                 self#visit_t ctx l )
        in
        self#build_AList ls
      method visit_ATuple ctx ls kind =
        self#build_ATuple (List.map ~f:(self#visit_t ctx) ls) kind
      method visit_AHashIdx ctx q n l (h: Name.t hash_idx) =
        let kv =
          eval ctx q
          |> Seq.map ~f:(fun k ->
                 let name, key =
                   match Map.to_alist k with
                   | [(name, key)] -> (name, key)
                   | _ -> failwith "Unexpected key tuple shape."
                 in
                 let field = {relation= Some n; name; type_= None} in
                 let ctx = Map.set ctx ~key:field ~data:key in
                 let value = self#visit_t ctx l in
                 (Value.of_primvalue key, value) )
        in
        self#build_AHashIdx kv h
      method visit_AEmpty _ = self#build_AEmpty
      method visit_AScalar ctx e =
        let l =
          Layout.of_value
            {value= eval_pred ctx e; rel= Relation.dummy; field= Field.dummy}
        in
        self#build_AScalar l
      method visit_AOrderedIdx _ _ _ _ _ = failwith "not implemented"
      method visit_Select ctx _ exprs r' = self#build_Select exprs (self#visit_t ctx r')
      method visit_Filter ctx _ _ r' = self#build_Filter (self#visit_t ctx r')
      method visit_Join ctx pred r1_name r1 r2_name r2 =
        self#build_Join ctx pred r1_name (self#visit_t ctx r1) r2_name
          (self#visit_t ctx r2)
      method visit_t ctx r =
        match r.node with
        | AEmpty -> self#visit_AEmpty ctx
        | AScalar e -> self#visit_AScalar ctx e
        | AList (r, n, a) -> self#visit_AList ctx r n a
        | ATuple (a, k) -> self#visit_ATuple ctx a k
        | AHashIdx (r, n, a, t) -> self#visit_AHashIdx ctx r n a t
        | AOrderedIdx (r, n, a, t) -> self#visit_AOrderedIdx ctx r n a t
        | Select (n, exprs, r') -> self#visit_Select ctx n exprs r'
        | Filter (n, pred, r') -> self#visit_Filter ctx n pred r'
        | Join {pred; r1_name; r1; r2_name; r2} ->
            self#visit_Join ctx pred r1_name r1 r2_name r2
        | Dedup _ | Agg _ | Scan _ ->
            Error.create "Wrong context." r [%sexp_of : (Name.t, Univ_map.t) ralgebra]
            |> Error.raise
    end
    

  let materialize ?(ctx= Map.empty (module Name)) l =
    let f =
      object
        inherit [_] material_fold
        method build_AEmpty = Layout.empty
        method build_AList ls = Seq.to_list ls |> unordered_list
        method build_ATuple ls kind =
          match kind with Zip -> zip_tuple ls | Cross -> cross_tuple ls
        method build_AHashIdx kv _ =
          let m = Seq.to_list kv |> Map.of_alist_exn (module ValueMap.Elem) in
          table m
            { field= Field.of_name "fixme"
            ; lookup= PredCtx.Key.Field (Field.of_name "fixme") }
        method build_AScalar l = l
        method build_AOrderedIdx _ _ = failwith ""
        method build_Select _ l = l
        method build_Filter l = l
        method build_Join _ _ _ _ _ _ = failwith ""
      end
    in
    f#visit_t ctx l

  module TF = struct
    open Type

    let type_of_scalar_layout (l: Layout.t) =
      match l.node with
      | Int (x, {node= {field; _}; _}) ->
          IntT {range= AbsInt.abstract x; nullable= false; field}
      | Bool (_, {node= {field; _}; _}) -> BoolT {nullable= false; field}
      | String (x, {node= {field; _}; _}) ->
          StringT {nchars= AbsInt.abstract (String.length x); nullable= false; field}
      | Null {node= {field; _}; _} -> NullT {field}
      | _ -> failwith "Not a scalar."

    class ['self] type_fold =
      object (_: 'self)
        inherit [_] material_fold
        method build_Select exprs t = FuncT ([t], List.length exprs)
        method build_Filter t = FuncT ([t], width t)
        method build_Join _ _ _ t1 _ t2 = FuncT ([t1; t2], width t1 + width t2)
        method build_AEmpty = EmptyT
        method build_AScalar = type_of_scalar_layout
        method build_AList ls =
          let t, c =
            Seq.fold ls ~init:(EmptyT, AbsCount.zero) ~f:(fun (t, c) t' ->
                (unify_exn t t', AbsCount.(c + count t')) )
          in
          UnorderedListT (t, {count= c})
        method build_ATuple ls kind =
          let counts = List.map ls ~f:count in
          match kind with
          | Zip -> ZipTupleT (ls, {count= List.fold_left1_exn ~f:AbsCount.unify counts})
          | Cross ->
              CrossTupleT (ls, {count= List.fold_left1_exn ~f:AbsCount.( * ) counts})
        method build_AHashIdx kv h =
          let kt, vt =
            Seq.fold kv ~init:(EmptyT, EmptyT) ~f:(fun (kt, vt1) (kv, vt2) ->
                ( unify_exn kt (Layout.of_value kv |> type_of_scalar_layout)
                , unify_exn vt1 vt2 ) )
          in
          TableT (kt, vt, {count= None; field= Field.of_name "fixme"; lookup= h.lookup})
        method build_AOrderedIdx _ _ = failwith ""
      end
      
  end

  include TF

  let to_type ?(ctx= Map.empty (module Name)) l =
    Logs.debug (fun m ->
        m "Computing type of abstract layout: %s"
          (Sexp.to_string_hum ([%sexp_of : (Name.t, Univ_map.t) ralgebra] l)) ) ;
    let type_ = (new type_fold)#visit_t ctx l in
    Logs.debug (fun m ->
        m "The type is: %s" (Sexp.to_string_hum ([%sexp_of : Type.t] type_)) ) ;
    type_

  module S = struct
    open Bitstring
    open Serialize
    open Type

    class ['self] serialize_fold log_ch writer =
      let ctr = ref 0 in
      let labels = ref [] in
      let log_start lbl =
        if Config.layout_map then (
          labels := (lbl, !ctr, Writer.pos writer) :: !labels ;
          Int.incr ctr )
      in
      let log_end () =
        if Config.layout_map then
          match !labels with
          | (lbl, ctr, start_pos) :: ls ->
              labels := ls ;
              let prefix = String.make (List.length !labels) ' ' in
              let end_ = Writer.Pos.to_bits (Writer.pos writer) in
              let start = Writer.Pos.to_bits start_pos in
              let byte_start = Int64.(start / of_int 8) in
              let byte_len = Int64.((end_ - start) / of_int 8) in
              let aligned = if Int64.(start % of_int 8 = of_int 0) then "=" else "~" in
              let out =
                sprintf "%d %s+ %s [%Ldb %s%LdB (%Ld bytes)]\n" ctr prefix lbl start
                  aligned byte_start byte_len
              in
              Out_channel.output_string log_ch out
          | [] -> Logs.warn (fun m -> m "Unexpected log_end.")
      in
      object (self: 'self)
        method visit_AList ctx type_ q n elem_layout =
          match type_ with
          | UnorderedListT (elem_t, _) ->
              (* Reserve space for list header. *)
              let header_pos = Writer.pos writer in
              log_start "List count" ;
              Writer.write_bytes writer (Bytes.make 8 '\x00') ;
              log_end () ;
              log_start "List len" ;
              Writer.write_bytes writer (Bytes.make 8 '\x00') ;
              log_end () ;
              (* Serialize list body. *)
              log_start "List body" ;
              let count = ref 0 in
              eval ctx q
              |> Seq.iter ~f:(fun t ->
                     let ctx =
                       Map.fold t ~init:ctx ~f:(fun ~key ~data ctx ->
                           let field = {relation= Some n; name= key; type_= None} in
                           Map.set ctx ~key:field ~data )
                     in
                     Caml.incr count ;
                     self#visit_t ctx elem_t elem_layout ) ;
              let end_pos = Writer.pos writer in
              log_end () ;
              (* Serialize list header. *)
              let len =
                Writer.Pos.(end_pos - header_pos |> to_bytes_exn) |> Int64.to_int_exn
              in
              Writer.seek writer header_pos ;
              Writer.write writer (of_int ~width:64 !count) ;
              Writer.write writer (of_int ~width:64 len) ;
              Writer.seek writer end_pos
          | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)
        method visit_ATuple ctx type_ elem_layouts _ =
          match type_ with
          | CrossTupleT (elem_ts, _) | ZipTupleT (elem_ts, _) ->
              (* Reserve space for header. *)
              let header_pos = Writer.pos writer in
              log_start "Tuple len" ;
              Writer.write_bytes writer (Bytes.make 8 '\x00') ;
              log_end () ;
              (* Serialize body *)
              log_start "Tuple body" ;
              List.iter2_exn ~f:(fun t l -> self#visit_t ctx t l) elem_ts elem_layouts ;
              log_end () ;
              let end_pos = Writer.pos writer in
              (* Serialize header. *)
              Writer.seek writer header_pos ;
              let len =
                Writer.Pos.(end_pos - header_pos |> to_bytes_exn) |> Int64.to_int_exn
              in
              Writer.write writer (of_int ~width:64 len) ;
              Writer.seek writer end_pos
          | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)
        method visit_AHashIdx ctx type_ q n l _ =
          match type_ with
          | TableT (key_t, value_t, _) ->
              let key_name = ref "" in
              let keys =
                eval ctx q
                |> Seq.map ~f:(fun k ->
                       let name, key =
                         match Map.to_alist k with
                         | [(name, key)] -> (name, key)
                         | _ -> failwith "Unexpected key tuple shape."
                       in
                       key_name := name ;
                       key )
                |> Seq.to_list
              in
              Logs.debug (fun m -> m "Generating hash for %d keys." (List.length keys)) ;
              let keys =
                List.map keys ~f:(fun k ->
                    let serialized =
                      Value.of_primvalue k |> Layout.of_value |> serialize key_t
                    in
                    let hash_key =
                      match k with
                      | `String s | `Unknown s -> s
                      | _ -> Bitstring.to_string serialized
                    in
                    (k, serialized, hash_key) )
              in
              Out_channel.with_file "keys.txt" ~f:(fun ch ->
                  List.iter keys ~f:(fun (_, _, x) -> Out_channel.fprintf ch "%s\n" x) ) ;
              let hash =
                let open Cmph in
                List.map keys ~f:(fun (_, _, x) -> x)
                |> KeySet.create
                |> Config.create ~verbose:true ~seed:0
                |> Hash.of_config
              in
              let keys =
                List.map keys ~f:(fun (k, b, x) -> (k, b, x, Cmph.Hash.hash hash x))
              in
              Out_channel.with_file "hashes.txt" ~f:(fun ch ->
                  List.iter keys ~f:(fun (_, _, x, h) ->
                      Out_channel.fprintf ch "%s -> %d\n" x h ) ) ;
              let hash_body =
                Cmph.Hash.to_packed hash |> Bytes.of_string |> align isize
              in
              let hash_len = Bytes.length hash_body in
              let table_size =
                List.fold_left keys ~f:(fun m (_, _, _, h) -> Int.max m h) ~init:0
                |> fun m -> m + 1
              in
              let header_pos = Writer.pos writer in
              log_start "Table len" ;
              Writer.write_bytes writer (Bytes.make 8 '\x00') ;
              log_end () ;
              log_start "Table hash len" ;
              Writer.write_bytes writer (Bytes.make 8 '\x00') ;
              log_end () ;
              log_start "Table hash" ;
              Writer.write_bytes writer (Bytes.make hash_len '\x00') ;
              log_end () ;
              log_start "Table key map" ;
              Writer.write_bytes writer (Bytes.make (8 * table_size) '\x00') ;
              log_end () ;
              let hash_table = Array.create ~len:table_size 0x0 in
              log_start "Table values" ;
              List.iter keys ~f:(fun (k, b, _, h) ->
                  let field = {relation= Some n; name= !key_name; type_= None} in
                  let ctx = Map.set ctx ~key:field ~data:k in
                  let value_pos = Writer.pos writer in
                  Writer.write writer b ;
                  self#visit_t ctx value_t l ;
                  hash_table.(h)
                  <- Writer.Pos.(value_pos |> to_bytes_exn |> Int64.to_int_exn) ) ;
              log_end () ;
              let end_pos = Writer.pos writer in
              Writer.seek writer header_pos ;
              let len = Writer.Pos.(end_pos - header_pos |> to_bytes_exn) in
              Writer.write writer (of_int ~width:64 (Int64.to_int_exn len)) ;
              Writer.write writer (of_int ~width:64 hash_len) ;
              Writer.write_bytes writer hash_body ;
              Array.iter hash_table ~f:(fun x -> Writer.write writer (of_int ~width:64 x)) ;
              Writer.seek writer end_pos
          | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)
        method visit_AEmpty _ _ = ()
        method visit_AScalar ctx type_ e =
          let l =
            Layout.of_value
              {value= eval_pred ctx e; rel= Relation.dummy; field= Field.dummy}
          in
          let label, bstr =
            match l.node with
            | Null s -> ("Null", serialize_null type_ l s)
            | Int (x, s) -> ("Int", serialize_int type_ l x s)
            | Bool (x, s) -> ("Bool", serialize_bool type_ l x s)
            | String (x, s) -> ("String", serialize_string type_ l x s)
            | _ -> failwith "Expected a scalar."
          in
          log_start label ; Writer.write writer bstr ; log_end ()
        method visit_AOrderedIdx _ _ _ _ _ _ = failwith ""
        method visit_t ctx type_ {node; meta} =
          meta :=
            Univ_map.update !meta Meta.pos ~f:(function
              | Some _ -> Many_pos
              | None -> Pos (Writer.pos writer |> Writer.Pos.to_bytes_exn) ) ;
          match node with
          | AEmpty -> self#visit_AEmpty ctx type_
          | AScalar e -> self#visit_AScalar ctx type_ e
          | AList (r, n, a) -> self#visit_AList ctx type_ r n a
          | ATuple (a, k) -> self#visit_ATuple ctx type_ a k
          | AHashIdx (r, n, a, t) -> self#visit_AHashIdx ctx type_ r n a t
          | AOrderedIdx (_, _, _, _) | _ -> failwith ""
      end
      
  end

  include S

  let serialize ?(ctx= Map.empty (module Name)) writer t l =
    Logs.debug (fun m ->
        m "Serializing abstract layout: %s"
          (Sexp.to_string_hum ([%sexp_of : (Name.t, Univ_map.t) ralgebra] l)) ) ;
    let open Bitstring in
    let l = Meta.to_mutable l in
    let begin_pos = Writer.pos writer in
    let fn = Caml.Filename.temp_file "buf" ".txt" in
    if Config.layout_map then Logs.info (fun m -> m "Outputting layout map to %s." fn) ;
    Out_channel.with_file fn ~f:(fun ch -> (new serialize_fold ch writer)#visit_t ctx t l) ;
    let end_pos = Writer.pos writer in
    let len = Writer.Pos.(end_pos - begin_pos |> to_bytes_exn) |> Int64.to_int_exn in
    let l = Meta.to_immutable l in
    (l, len)

  let unnamed t = {name= ""; relation= None; type_= Some t}

  let pred_to_schema_exn =
    let open Type0.PrimType in
    function
    | Name {type_= None; _} -> failwith "Missing type."
    | Name ({type_= Some _; _} as n) -> n
    | Int _ -> unnamed IntT
    | Bool _ -> unnamed BoolT
    | String _ -> unnamed StringT
    | Null -> failwith ""
    | Binop (op, _, _) | Varop (op, _) ->
      match op with
      | Eq | Lt | Le | Gt | Ge | And | Or -> unnamed BoolT
      | Add | Sub | Mul | Div | Mod -> unnamed IntT
end

module Make_db (Config_db : Config.S_db) () = struct
  module Config = struct
    include Config_db

    let eval ctx query =
      let sql = ralgebra_to_sql (subst ctx query) in
      Db.exec_cursor Config_db.conn sql

    let layout_map = Config_db.layout_map
  end

  include Make (Config) ()

  let rec annotate_schema r =
    let node', schema =
      match r.node with
      | Select (n, x, r) ->
          let r' = annotate_schema r in
          let schema = List.map x ~f:pred_to_schema_exn in
          (Select (n, x, r'), schema)
      | Filter (n, x, r) ->
          let r' = annotate_schema r in
          let schema = Univ_map.find_exn r'.meta Meta.schema in
          (Filter (n, x, r'), schema)
      | Join ({r1_name; r1; r2_name; r2; _} as j) ->
          let r1' = annotate_schema r1 in
          let r2' = annotate_schema r2 in
          let s1 = Univ_map.find_exn r1'.meta Meta.schema in
          let s2 = Univ_map.find_exn r2'.meta Meta.schema in
          let schema =
            List.map s1 ~f:(fun n -> {n with relation= Some r1_name})
            @ List.map s2 ~f:(fun n -> {n with relation= Some r2_name})
          in
          (Join {j with r1= r1'; r2= r2'}, schema)
      | Dedup r as x ->
          let r' = annotate_schema r in
          (x, Univ_map.find_exn r'.meta Meta.schema)
      | Scan table as x ->
          let schema =
            (Db.Relation.from_db Config.conn table).fields
            |> List.map ~f:(fun f -> Name.of_field ~rel:table f)
          in
          (x, schema)
      | Agg (_, _, _, _) -> failwith ""
      | AEmpty as x -> (x, [])
      | AScalar e as x ->
          let schema = [pred_to_schema_exn e] in
          (x, schema)
      | AList (n, x, r) ->
          let r' = annotate_schema r in
          let schema = Univ_map.find_exn r'.meta Meta.schema in
          (AList (n, x, r'), schema)
      | ATuple (rs, t) ->
          let rs' = List.map ~f:annotate_schema rs in
          let schema =
            List.concat_map ~f:(fun r' -> Univ_map.find_exn r'.meta Meta.schema) rs'
          in
          (ATuple (rs', t), schema)
      | AHashIdx (kr, n, vr, x) ->
          let kr' = annotate_schema kr in
          let vr' = annotate_schema vr in
          let schema =
            Univ_map.find_exn kr'.meta Meta.schema
            @ Univ_map.find_exn vr'.meta Meta.schema
          in
          (AHashIdx (kr', n, vr', x), schema)
      | AOrderedIdx (kr, n, vr, x) ->
          let kr' = annotate_schema kr in
          let vr' = annotate_schema vr in
          let schema =
            Univ_map.find_exn kr'.meta Meta.schema
            @ Univ_map.find_exn vr'.meta Meta.schema
          in
          (AOrderedIdx (kr', n, vr', x), schema)
    in
    {node= node'; meta= Univ_map.set r.meta Meta.schema schema}
end

module Test = struct
  let%expect_test "mat-col" =
    let conn = new Postgresql.connection ~dbname:"tpcds1" () in
    let layout =
      of_string_exn
        "AList(Filter(ship_mode -> ship_mode.sm_carrier = \"GERMA\", ship_mode), t -> \
         AScalar(t.sm_carrier))"
    in
    let module M =
      Make_db (struct
          let conn = conn

          let layout_map = false
        end)
        () in
    M.materialize layout |> [%sexp_of : Layout.t] |> print_s ;
    [%expect
      {|
    (UnorderedList ((
      String "GERMA               " (
        (rel "")
        (field (
          (fname "")
          (dtype DBool))))))) |}]

  let%expect_test "mat-hidx" =
    let conn = new Postgresql.connection ~dbname:"tpcds1" () in
    let layout =
      of_string_exn
        "AHashIdx(Dedup(Select(ship_mode -> [ship_mode.sm_type], Filter(ship_mode -> \
         ship_mode.sm_type = \"LIBRARY\", ship_mode))), t -> AList(Filter(ship_mode -> \
         t.sm_type = ship_mode.sm_type, ship_mode), t -> AScalar(t.sm_code)), null)"
    in
    let module M =
      Make_db (struct
          let conn = conn

          let layout_map = false
        end)
        () in
    M.materialize layout |> [%sexp_of : Layout.t] |> print_s ;
    [%expect
      {|
    (Table
      ((
        ((rel "")
         (field (
           (fname "")
           (dtype DBool)))
         (value (Unknown "LIBRARY                       ")))
        (UnorderedList (
          (String "AIR       " (
            (rel "")
            (field (
              (fname "")
              (dtype DBool)))))
          (String "SURFACE   " (
            (rel "")
            (field (
              (fname "")
              (dtype DBool)))))
          (String "SEA       " (
            (rel "")
            (field (
              (fname "")
              (dtype DBool)))))))))
      ((field (
         (fname fixme)
         (dtype DBool)))
       (lookup (
         Field (
           (fname fixme)
           (dtype DBool)))))) |}]

  let rels = Hashtbl.create (module String)

  module Eval = struct
    include Eval.Make_relation (struct
      type relation = string [@@deriving compare, sexp]

      let eval_relation r =
        Hashtbl.find_exn rels r |> Seq.of_list
        |> Seq.map ~f:(fun t ->
               List.map t ~f:(fun (n, v) ->
                   {rel= Relation.of_name r; field= Field.of_name n; Value.value= v} ) )
    end)

    let rec eval_pred ctx = function
      | Null -> `Null
      | Int x -> `Int x
      | String x -> `String x
      | Bool x -> `Bool x
      | Name n -> (
        match Map.find ctx n with
        | Some v -> v
        | None ->
            Error.create "Unbound variable." (n, ctx) [%sexp_of : Name.t * Ctx.t]
            |> Error.raise )
      | Binop (op, p1, p2) -> (
          let v1 = eval_pred ctx p1 in
          let v2 = eval_pred ctx p2 in
          match (op, v1, v2) with
          | Eq, `Null, _ | Eq, _, `Null -> `Bool false
          | Eq, `Bool x1, `Bool x2 -> `Bool Bool.(x1 = x2)
          | Eq, `Int x1, `Int x2 -> `Bool Int.(x1 = x2)
          | Eq, `String x1, `String x2 -> `Bool String.(x1 = x2)
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
          | _ ->
              Error.create "Unexpected argument types." (op, v1, v2)
                [%sexp_of : Ralgebra.op * primvalue * primvalue]
              |> Error.raise )
      | Varop (op, ps) ->
          let vs = List.map ps ~f:(eval_pred ctx) in
          match op with
          | And ->
              List.for_all vs ~f:(function
                | `Bool x -> x
                | _ -> failwith "Unexpected argument type." )
              |> fun x -> `Bool x
          | Or ->
              List.exists vs ~f:(function
                | `Bool x -> x
                | _ -> failwith "Unexpected argument type." )
              |> fun x -> `Bool x
          | _ ->
              Error.create "Unexpected argument types." (op, vs)
                [%sexp_of : Ralgebra.op * primvalue list]
              |> Error.raise

    let eval_join ctx p _ _ r1 r2 =
      Seq.concat_map r1 ~f:(fun t1 ->
          Seq.filter r2 ~f:(fun t2 ->
              let ctx =
                ctx
                |> Map.merge_right (Ctx.of_tuple t1)
                |> Map.merge_right (Ctx.of_tuple t2)
              in
              match eval_pred ctx p with
              | `Bool x -> x
              | _ -> failwith "Expected a boolean." ) )

    let eval_filter ctx p seq =
      Seq.filter seq ~f:(fun t ->
          let ctx = Map.merge_right ctx (Ctx.of_tuple t) in
          match eval_pred ctx p with `Bool x -> x | _ -> failwith "Expected a boolean."
      )

    let eval_dedup seq =
      let set = Hash_set.create (module Tuple) in
      Seq.iter seq ~f:(Hash_set.add set) ;
      Hash_set.to_list set |> Seq.of_list

    let eval_select ctx out seq =
      Seq.map seq ~f:(fun t ->
          let ctx = Map.merge_right ctx (Ctx.of_tuple t) in
          List.map out ~f:(fun e ->
              let v = eval_pred ctx e |> Value.of_primvalue in
              match e with Name n -> {v with field= Field.of_name n.name} | _ -> v ) )

    let eval ctx r =
      let rec eval {node; _} =
        match node with
        | Scan r -> eval_relation r
        | Filter (_, p, r) -> eval_filter ctx p (eval r)
        | Join {pred= p; r1_name; r1; r2_name; r2} ->
            eval_join ctx p r1_name r2_name (eval r1) (eval r2)
        | Dedup r -> eval_dedup (eval r)
        | Select (_, out, r) -> eval_select ctx out (eval r)
        | Agg _ | _ -> failwith ""
      in
      eval r
  end

  let create name fs xs =
    let data =
      List.map xs ~f:(fun data ->
          List.map2_exn fs data ~f:(fun fname value -> (fname, `Int value)) )
    in
    Hashtbl.set rels ~key:name ~data ;
    ( name
    , List.map fs ~f:(fun f ->
          {name= f; relation= Some name; type_= Some Type0.PrimType.IntT} ) )

  module M =
    Make (struct
        let layout_map = false

        let eval ctx query =
          Eval.eval ctx query
          |> Seq.map ~f:(fun (t: Tuple.t) ->
                 List.map t ~f:(fun (v: Value.t) -> (v.field.fname, v.value))
                 |> Map.of_alist_exn (module String) )
      end)
      ()

  [@@@warning "-8"]

  let _, [f; _] = create "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

  [@@@warning "+8"]

  let%expect_test "part-list" =
    let layout =
      of_string_exn "AList(r1, x -> ATuple([AScalar(x.f), AScalar(x.g)], Cross))"
    in
    let part_layout = M.partition ~part:(Name f) ~lookup:(Name f) layout in
    [%sexp_of : (Name.t, Univ_map.t) ralgebra] part_layout |> print_s ;
    [%expect
      {|
      (AHashIdx
        (Dedup (
          Select r1 ((Name ((relation (r1)) (name f) (type_ (IntT))))) (Scan r1)))
        x0
        (AList
          (Filter r1
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
        ((lookup (Name ((relation (r1)) (name f) (type_ (IntT))))))) |}] ;
    [%sexp_of : Type.t] (M.to_type part_layout) |> print_s ;
    [%expect
      {|
      (TableT
        (IntT (
          (range (1 3))
          (nullable false)
          (field (
            (fname "")
            (dtype DBool)))))
        (UnorderedListT
          (CrossTupleT
            ((IntT (
               (range (1 3))
               (nullable false)
               (field (
                 (fname "")
                 (dtype DBool)))))
             (IntT (
               (range (1 4))
               (nullable false)
               (field (
                 (fname "")
                 (dtype DBool))))))
            ((count ((1 1)))))
          ((count ((1 2)))))
        ((count ())
         (field (
           (fname fixme)
           (dtype DBool)))
         (lookup (Name ((relation (r1)) (name f) (type_ (IntT))))))) |}] ;
    let mat_layout = M.materialize part_layout in
    [%sexp_of : Layout.t] mat_layout |> print_s ;
    [%expect
      {|
      (Table
        ((((rel "")
           (field (
             (fname "")
             (dtype DBool)))
           (value (Int 1)))
          (UnorderedList (
            (CrossTuple (
              (Int 1 (
                (rel "")
                (field (
                  (fname "")
                  (dtype DBool)))))
              (Int 2 (
                (rel "")
                (field (
                  (fname "")
                  (dtype DBool)))))))
            (CrossTuple (
              (Int 1 (
                (rel "")
                (field (
                  (fname "")
                  (dtype DBool)))))
              (Int 3 (
                (rel "")
                (field (
                  (fname "")
                  (dtype DBool))))))))))
         (((rel "")
           (field (
             (fname "")
             (dtype DBool)))
           (value (Int 2)))
          (UnorderedList (
            (CrossTuple (
              (Int 2 (
                (rel "")
                (field (
                  (fname "")
                  (dtype DBool)))))
              (Int 1 (
                (rel "")
                (field (
                  (fname "")
                  (dtype DBool)))))))
            (CrossTuple (
              (Int 2 (
                (rel "")
                (field (
                  (fname "")
                  (dtype DBool)))))
              (Int 2 (
                (rel "")
                (field (
                  (fname "")
                  (dtype DBool))))))))))
         (((rel "")
           (field (
             (fname "")
             (dtype DBool)))
           (value (Int 3)))
          (UnorderedList ((
            CrossTuple (
              (Int 3 (
                (rel "")
                (field (
                  (fname "")
                  (dtype DBool)))))
              (Int 4 (
                (rel "")
                (field (
                  (fname "")
                  (dtype DBool)))))))))))
        ((field (
           (fname fixme)
           (dtype DBool)))
         (lookup (
           Field (
             (fname fixme)
             (dtype DBool)))))) |}]
end
