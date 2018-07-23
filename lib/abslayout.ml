open Base
open Stdio
open Printf
open Collections
open Db
open Layout

module No_config = struct
  include Abslayout0

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

    let create ?relation ?type_ name = {relation; name; type_}

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

    let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

    let of_field ?rel f =
      {relation= rel; name= f.fname; type_= Some (Type.PrimType.of_dtype f.dtype)}
  end

  module Meta = struct
    open Core

    type 'a key = 'a Univ_map.Key.t

    type pos = Pos of int64 | Many_pos [@@deriving sexp]

    let empty () = ref Univ_map.empty

    let schema = Univ_map.Key.create ~name:"schema" [%sexp_of : Name.t list]

    let pos = Univ_map.Key.create ~name:"pos" [%sexp_of : pos]

    let meta_iter f =
      object (self: 'self)
        inherit [_] iter
        method! visit_t () {node; meta} = self#visit_node () node ; f meta
      end

    let change_inplace r key ~f =
      (meta_iter (fun m -> m := Univ_map.change !m key ~f))#visit_t () r

    let find_exn ralgebra key =
      match Univ_map.find !(ralgebra.meta) key with
      | Some x -> x
      | None ->
          Error.create "Missing metadata."
            (Univ_map.Key.name key, ralgebra)
            [%sexp_of : string * t]
          |> Error.raise

    let set ralgebra k v =
      {ralgebra with meta= ref (Univ_map.set !(ralgebra.meta) k v)}
  end

  let select a b = {node= Select (a, b); meta= Meta.empty ()}

  let filter a b = {node= Filter (a, b); meta= Meta.empty ()}

  let agg a b c = {node= Agg (a, b, c); meta= Meta.empty ()}

  let dedup a = {node= Dedup a; meta= Meta.empty ()}

  let scan a = {node= Scan a; meta= Meta.empty ()}

  let empty = {node= AEmpty; meta= Meta.empty ()}

  let scalar a = {node= AScalar a; meta= Meta.empty ()}

  let list a b = {node= AList (a, b); meta= Meta.empty ()}

  let tuple a b = {node= ATuple (a, b); meta= Meta.empty ()}

  let hash_idx a b c = {node= AHashIdx (a, b, c); meta= Meta.empty ()}

  let ordered_idx a b c = {node= AOrderedIdx (a, b, c); meta= Meta.empty ()}

  let as_ a b = {node= As (a, b); meta= Meta.empty ()}

  let name r =
    match r.node with
    | Select _ -> "select"
    | Filter _ -> "filter"
    | Join _ -> "join"
    | Agg _ -> "agg"
    | Dedup _ -> "dedup"
    | Scan _ -> "scan"
    | AEmpty -> "empty"
    | AScalar _ -> "scalar"
    | AList _ -> "list"
    | ATuple _ -> "tuple"
    | AHashIdx _ -> "hash_idx"
    | AOrderedIdx _ -> "ordered_idx"
    | As _ -> "as"

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
        method visit_name _ _ = self#zero
      end
    in
    ralgebra_params#visit_t () r

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
    let empty_ctx = Set.empty (module Name) in
    let resolve_agg ctx = function
      | Count -> Count
      | Key n -> Key (resolve_name ctx n)
      | Sum n -> Sum (resolve_name ctx n)
      | Avg n -> Avg (resolve_name ctx n)
      | Min n -> Min (resolve_name ctx n)
      | Max n -> Max (resolve_name ctx n)
    in
    let resolve_pred ctx =
      (object
         inherit [_] map
         method! visit_Name ctx n = Name (resolve_name ctx n)
      end)
        #visit_pred ctx
    in
    let preds_to_names preds =
      List.filter_map preds ~f:(function Name n -> Some n | _ -> None)
      |> Set.of_list (module Name)
    in
    let aggs_to_names aggs =
      List.filter_map aggs ~f:(function Key n -> Some n | _ -> None)
      |> Set.of_list (module Name)
    in
    let rec resolve outer_ctx {node; meta} =
      let node', ctx' =
        match node with
        | Select (preds, r) ->
            let r, preds =
              let r, inner_ctx = resolve outer_ctx r in
              let ctx = Set.union outer_ctx inner_ctx in
              (r, List.map preds ~f:(resolve_pred ctx))
            in
            (Select (preds, r), preds_to_names preds)
        | Filter (pred, r) ->
            let r, inner_ctx = resolve outer_ctx r in
            let ctx = Set.union outer_ctx inner_ctx in
            let pred = resolve_pred ctx pred in
            (Filter (pred, r), inner_ctx)
        | Join {pred; r1; r2} ->
            let r1, inner_ctx1 = resolve outer_ctx r1 in
            let r2, inner_ctx2 = resolve outer_ctx r2 in
            let ctx =
              Set.union_list (module Name) [inner_ctx1; inner_ctx2; outer_ctx]
            in
            let pred = resolve_pred ctx pred in
            (Join {pred; r1; r2}, Set.union inner_ctx1 inner_ctx2)
        | Scan l -> (Scan l, resolve_relation l)
        | Agg (aggs, key, r) ->
            let r, inner_ctx = resolve outer_ctx r in
            let ctx = Set.union outer_ctx inner_ctx in
            let aggs = List.map ~f:(resolve_agg ctx) aggs in
            let key = List.map key ~f:(resolve_name ctx) in
            (Agg (aggs, key, r), aggs_to_names aggs)
        | Dedup r ->
            let r, inner_ctx = resolve outer_ctx r in
            (Dedup r, inner_ctx)
        | AEmpty -> (AEmpty, empty_ctx)
        | AScalar p ->
            let p = resolve_pred outer_ctx p in
            let ctx =
              match p with
              | Name n -> Set.singleton (module Name) n
              | _ -> Set.empty (module Name)
            in
            (AScalar p, ctx)
        | AList (r, l) ->
            let r, inner_ctx = resolve outer_ctx r in
            let ctx = Set.union inner_ctx outer_ctx in
            let l, ctx = resolve ctx l in
            (AList (r, l), ctx)
        | ATuple (ls, t) ->
            let ls, ctxs = List.map ls ~f:(resolve outer_ctx) |> List.unzip in
            let ctx = Set.union_list (module Name) ctxs in
            (ATuple (ls, t), ctx)
        | AHashIdx (r, l, m) ->
            let r, inner_ctx = resolve outer_ctx r in
            let ctx = Set.union inner_ctx outer_ctx in
            let l, ctx = resolve ctx l in
            let m =
              (object
                 inherit [_] map
                 method visit_name _ _ = failwith ""
                 method! visit_pred _ = resolve_pred ctx
              end)
                #visit_hash_idx () m
            in
            (AHashIdx (r, l, m), ctx)
        | AOrderedIdx (r, l, m) ->
            let r, inner_ctx = resolve outer_ctx r in
            let ctx = Set.union outer_ctx inner_ctx in
            let l, ctx = resolve ctx l in
            let m =
              (object
                 inherit [_] map
                 method visit_name _ _ = failwith ""
                 method! visit_pred _ = resolve_pred ctx
              end)
                #visit_ordered_idx () m
            in
            (AOrderedIdx (r, l, m), ctx)
        | As (n, r) ->
            let r, ctx = resolve outer_ctx r in
            let ctx = rename n ctx in
            (As (n, r), ctx)
      in
      ({node= node'; meta}, ctx')
    in
    let r, _ = resolve (Set.empty (module Name)) r in
    r

  let pred_of_value = function
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
        method visit_name _ x = x
      end
    in
    v#visit_t ()

  let pred_relations p =
    let rels = ref [] in
    let f =
      object
        inherit [_] iter
        method! visit_Name () =
          function
          | {relation= Some r; _} -> rels := r :: !rels | {relation= None; _} -> ()
        method visit_name () _ = ()
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

  (** Return the set of relations which have fields in the tuple produced by
     this expression. *)
  let relation r =
    let reducer =
      object
        inherit [_] reduce
        method zero = Set.empty (module String)
        method plus = Set.union
        method! visit_As _ n _ = Set.singleton (module String) n
        method! visit_Scan _ n = Set.singleton (module String) n
      end
    in
    reducer#visit_t () r

  let ralgebra_to_sql r =
    let rec f {node; _} =
      let relation_name r =
        let rs = relation r in
        if Set.length rs > 1 then
          Error.create
            "More than one relation name. Use AS to give this expression a name." r
            [%sexp_of : t]
          |> Error.raise
        else Set.choose_exn rs
      in
      match node with
      | Select ([], r) ->
          sprintf "select top 0 from (%s) as %s" (f r) (relation_name r)
      | Select (fs, r) ->
          let fields = List.map fs ~f:pred_to_sql |> String.concat ~sep:"," in
          sprintf "select %s from (%s) as %s" fields (f r) (relation_name r)
      | Scan r -> sprintf "select * from %s" r
      | Filter (pred, r) ->
          sprintf "select * from (%s) as %s where %s" (f r) (relation_name r)
            (pred_to_sql pred)
      | Join {pred; r1; r2} ->
          let r1_name = relation_name r1 in
          let r2_name = relation_name r2 in
          sprintf "select * from (%s) as %s, (%s) as %s where %s" (f r1) r1_name
            (f r2) r2_name (pred_to_sql pred)
      | Agg (aggs, key, r) ->
          let rel = relation_name r in
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
      | As (_, r) -> f r
      | _ ->
          Error.of_string "Only relational algebra constructs allowed."
          |> Error.raise
    in
    f r

  let unnamed t = {name= ""; relation= None; type_= Some t}

  let pred_to_schema_exn =
    let open Type0.PrimType in
    function
    | Name ({type_= None; _} as n) ->
        Error.create "Missing type." n [%sexp_of : Name.t] |> Error.raise
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

    val eval : Ctx.t -> t -> Ctx.t Seq.t
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
    let name = Fresh.name fresh "x%d" in
    let domain = as_ name (dedup (select [p] (scan rel))) in
    let pred =
      let subst =
        object
          inherit [_] endo
          method! visit_Name _ _ n =
            match n.relation with
            | Some r when String.(rel = r) -> Name {n with relation= Some name}
            | _ -> Name n
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
              Filter (Binop (Eq, p, pred), {node= r; meta= Meta.empty ()})
            else r
        end
      in
      subst#visit_t () l
    in
    hash_idx domain layout {lookup}

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
      method visit_As ctx _ r = self#visit_t ctx r
      method visit_AList ctx q l =
        eval ctx q
        |> Seq.map ~f:(fun ctx' -> self#visit_t (Map.merge_right ctx ctx') l)
        |> self#build_AList
      method visit_ATuple ctx ls kind =
        self#build_ATuple (List.map ~f:(self#visit_t ctx) ls) kind
      method visit_AHashIdx ctx q l (h: hash_idx) =
        let kv =
          eval ctx q
          |> Seq.map ~f:(fun key_ctx ->
                 let _, key =
                   match Map.to_alist key_ctx with
                   | [(name, key)] -> (name, key)
                   | _ -> failwith "Unexpected key tuple shape."
                 in
                 let value = self#visit_t (Map.merge_right ctx key_ctx) l in
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
      method visit_AOrderedIdx _ _ _ _ = failwith "not implemented"
      method visit_Select ctx exprs r' =
        self#build_Select exprs (self#visit_t ctx r')
      method visit_Filter ctx _ r' = self#build_Filter (self#visit_t ctx r')
      method visit_Join ctx pred r1 r2 =
        self#build_Join ctx pred (self#visit_t ctx r1) (self#visit_t ctx r2)
      method visit_t ctx r =
        match r.node with
        | AEmpty -> self#visit_AEmpty ctx
        | AScalar e -> self#visit_AScalar ctx e
        | AList (r, a) -> self#visit_AList ctx r a
        | ATuple (a, k) -> self#visit_ATuple ctx a k
        | AHashIdx (r, a, t) -> self#visit_AHashIdx ctx r a t
        | AOrderedIdx (r, a, t) -> self#visit_AOrderedIdx ctx r a t
        | Select (exprs, r') -> self#visit_Select ctx exprs r'
        | Filter (pred, r') -> self#visit_Filter ctx pred r'
        | Join {pred; r1; r2} -> self#visit_Join ctx pred r1 r2
        | As (n, r) -> self#visit_As ctx n r
        | Dedup _ | Agg _ | Scan _ ->
            Error.create "Wrong context." r [%sexp_of : t] |> Error.raise
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
        method build_Join _ _ _ _ = failwith ""
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
        method build_Join _ _ t1 t2 = FuncT ([t1; t2], width t1 + width t2)
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
          | Zip ->
              ZipTupleT (ls, {count= List.fold_left1_exn ~f:AbsCount.unify counts})
          | Cross ->
              CrossTupleT (ls, {count= List.fold_left1_exn ~f:AbsCount.( * ) counts})
        method build_AHashIdx kv h =
          let kt, vt =
            Seq.fold kv ~init:(EmptyT, EmptyT) ~f:(fun (kt, vt1) (kv, vt2) ->
                ( unify_exn kt (Layout.of_value kv |> type_of_scalar_layout)
                , unify_exn vt1 vt2 ) )
          in
          TableT
            (kt, vt, {count= None; field= Field.of_name "fixme"; lookup= h.lookup})
        method build_AOrderedIdx _ _ = failwith ""
      end
      
  end

  include TF

  let to_type ?(ctx= Map.empty (module Name)) l =
    Logs.debug (fun m ->
        m "Computing type of abstract layout: %s"
          (Sexp.to_string_hum ([%sexp_of : t] l)) ) ;
    let type_ = (new type_fold)#visit_t ctx l in
    Logs.debug (fun m ->
        m "The type is: %s" (Sexp.to_string_hum ([%sexp_of : Type.t] type_)) ) ;
    type_

  module S = struct
    class ['self] serialize_fold log_ch writer =
      let ctr = ref 0 in
      let labels = ref [] in
      let log_start lbl =
        let open Bitstring in
        if Config.layout_map then (
          labels := (lbl, !ctr, Writer.pos writer) :: !labels ;
          Int.incr ctr )
      in
      let log_end () =
        let open Bitstring in
        if Config.layout_map then
          match !labels with
          | (lbl, ctr, start_pos) :: ls ->
              labels := ls ;
              let prefix = String.make (List.length !labels) ' ' in
              let end_ = Writer.Pos.to_bits (Writer.pos writer) in
              let start = Writer.Pos.to_bits start_pos in
              let byte_start = Int64.(start / of_int 8) in
              let byte_len = Int64.((end_ - start) / of_int 8) in
              let aligned =
                if Int64.(start % of_int 8 = of_int 0) then "=" else "~"
              in
              let out =
                sprintf "%d %s+ %s [%Ldb %s%LdB (%Ld bytes)]\n" ctr prefix lbl start
                  aligned byte_start byte_len
              in
              Out_channel.output_string log_ch out
          | [] -> Logs.warn (fun m -> m "Unexpected log_end.")
      in
      object (self: 'self)
        method visit_AList ctx type_ q elem_layout =
          let open Bitstring in
          let open Type in
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
                     Caml.incr count ;
                     self#visit_t (Map.merge_right ctx t) elem_t elem_layout ) ;
              let end_pos = Writer.pos writer in
              log_end () ;
              (* Serialize list header. *)
              let len =
                Writer.Pos.(end_pos - header_pos |> to_bytes_exn)
                |> Int64.to_int_exn
              in
              Writer.seek writer header_pos ;
              Writer.write writer (of_int ~width:64 !count) ;
              Writer.write writer (of_int ~width:64 len) ;
              Writer.seek writer end_pos
          | t ->
              Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)
        method visit_ATuple ctx type_ elem_layouts _ =
          let open Bitstring in
          let open Type in
          match type_ with
          | CrossTupleT (elem_ts, _) | ZipTupleT (elem_ts, _) ->
              (* Reserve space for header. *)
              let header_pos = Writer.pos writer in
              log_start "Tuple len" ;
              Writer.write_bytes writer (Bytes.make 8 '\x00') ;
              log_end () ;
              (* Serialize body *)
              log_start "Tuple body" ;
              List.iter2_exn
                ~f:(fun t l -> self#visit_t ctx t l)
                elem_ts elem_layouts ;
              log_end () ;
              let end_pos = Writer.pos writer in
              (* Serialize header. *)
              Writer.seek writer header_pos ;
              let len =
                Writer.Pos.(end_pos - header_pos |> to_bytes_exn)
                |> Int64.to_int_exn
              in
              Writer.write writer (of_int ~width:64 len) ;
              Writer.seek writer end_pos
          | t ->
              Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)
        method visit_AHashIdx ctx type_ q l _ =
          let open Bitstring in
          let open Type in
          match type_ with
          | TableT (key_t, value_t, _) ->
              let key_name = ref (Name.of_string_exn "fixme") in
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
              Logs.debug (fun m ->
                  m "Generating hash for %d keys." (List.length keys) ) ;
              let keys =
                List.map keys ~f:(fun k ->
                    let serialized =
                      Value.of_primvalue k |> Layout.of_value
                      |> Serialize.serialize key_t
                    in
                    let hash_key =
                      match k with
                      | `String s | `Unknown s -> s
                      | _ -> Bitstring.to_string serialized
                    in
                    (k, serialized, hash_key) )
              in
              Out_channel.with_file "keys.txt" ~f:(fun ch ->
                  List.iter keys ~f:(fun (_, _, x) ->
                      Out_channel.fprintf ch "%s\n" x ) ) ;
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
                Cmph.Hash.to_packed hash |> Bytes.of_string
                |> Serialize.(align isize)
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
                  let ctx = Map.set ctx ~key:!key_name ~data:k in
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
              Array.iter hash_table ~f:(fun x ->
                  Writer.write writer (of_int ~width:64 x) ) ;
              Writer.seek writer end_pos
          | t ->
              Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)
        method visit_AEmpty _ _ = ()
        method visit_AScalar ctx type_ e =
          let open Serialize in
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
          log_start label ;
          Bitstring.Writer.write writer bstr ;
          log_end ()
        method visit_AOrderedIdx _ _ _ _ _ = failwith ""
        method visit_func ctx type_ rs =
          let open Type in
          match type_ with
          | FuncT (ts, _) -> List.iter2_exn ts rs ~f:(self#visit_t ctx)
          | _ ->
              Error.create "Expected a function type." type_ [%sexp_of : Type.t]
              |> Error.raise
        method visit_t ctx type_ ({node; _} as r) =
          let open Bitstring in
          Meta.change_inplace r Meta.pos ~f:(function
            | Some _ -> Some Many_pos
            | None -> Some (Pos (Writer.pos writer |> Writer.Pos.to_bytes_exn)) ) ;
          match node with
          | AEmpty -> self#visit_AEmpty ctx type_
          | AScalar e -> self#visit_AScalar ctx type_ e
          | AList (r, a) -> self#visit_AList ctx type_ r a
          | ATuple (a, k) -> self#visit_ATuple ctx type_ a k
          | AHashIdx (r, a, t) -> self#visit_AHashIdx ctx type_ r a t
          | AOrderedIdx (r, a, t) -> self#visit_AOrderedIdx ctx type_ r a t
          | Select (_, r) | Filter (_, r) | Agg (_, _, r) | Dedup r ->
              self#visit_func ctx type_ [r]
          | As (_, r) -> self#visit_t ctx type_ r
          | Join {r1; r2; _} -> self#visit_func ctx type_ [r1; r2]
          | Scan _ ->
              Error.create "Cannot serialize." r [%sexp_of : t] |> Error.raise
      end
      
  end

  include S

  let serialize ?(ctx= Map.empty (module Name)) writer t l =
    Logs.debug (fun m ->
        m "Serializing abstract layout: %s" (Sexp.to_string_hum ([%sexp_of : t] l))
    ) ;
    let open Bitstring in
    let begin_pos = Writer.pos writer in
    let fn = Caml.Filename.temp_file "buf" ".txt" in
    if Config.layout_map then
      Logs.info (fun m -> m "Outputting layout map to %s." fn) ;
    Out_channel.with_file fn ~f:(fun ch ->
        (new serialize_fold ch writer)#visit_t ctx t l ) ;
    let end_pos = Writer.pos writer in
    let len =
      Writer.Pos.(end_pos - begin_pos |> to_bytes_exn) |> Int64.to_int_exn
    in
    (l, len)
end

module Make_db (Config_db : Config.S_db) () = struct
  module Config = struct
    include Config_db

    let eval ctx query =
      let sql = ralgebra_to_sql (subst ctx query) in
      let schema = Meta.(find_exn query schema) in
      Db.exec_cursor Config_db.conn sql
      |> Seq.map ~f:(fun t ->
             List.map schema ~f:(fun n ->
                 match Map.find t n.name with
                 | Some v -> (n, v)
                 | None ->
                     Error.create "Mismatched tuple." (t, schema)
                       [%sexp_of : Db.primvalue Map.M(String).t * Name.t list]
                     |> Error.raise )
             |> Map.of_alist_exn (module Name) )

    let layout_map = Config_db.layout_map
  end

  (** Add a schema field to each metadata node. Variables must first be
     annotated with type information. *)
  let annotate_schema =
    let mapper =
      object (self: 'self)
        inherit [_] map
        method! visit_t () {node; meta} =
          let node' = self#visit_node () node in
          let schema =
            match node' with
            | Select (x, _) -> List.map x ~f:pred_to_schema_exn
            | Filter (_, r) | Dedup r | AList (_, r) -> Meta.(find_exn r schema)
            | Join {r1; r2; _} | AOrderedIdx (r1, r2, _) | AHashIdx (r1, r2, _) ->
                Meta.(find_exn r1 schema) @ Meta.(find_exn r2 schema)
            | Agg (_, _, _) -> failwith ""
            | AEmpty -> []
            | AScalar e -> [pred_to_schema_exn e]
            | ATuple (rs, _) ->
                List.concat_map ~f:(fun r -> Meta.(find_exn r schema)) rs
            | As (n, r) ->
                Meta.(find_exn r schema)
                |> List.map ~f:(fun x -> {x with relation= Some n})
            | Scan table ->
                (Db.Relation.from_db Config.conn table).fields
                |> List.map ~f:(fun f -> Name.of_field ~rel:table f)
          in
          Meta.set {node= node'; meta} Meta.schema schema
      end
    in
    mapper#visit_t ()

  include Make (Config) ()
end
