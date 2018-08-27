open Base
open Collections

module Make (Eval : Eval.S) = struct
  open Abslayout0
  open Abslayout

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
            if String.(rel = rel') then Filter (Binop (Eq, p, pred), scan rel)
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
        Eval.eval ctx q
        |> Seq.map ~f:(fun ctx' -> self#visit_t (Map.merge_right ctx ctx') l)
        |> self#build_AList
      method visit_ATuple ctx ls kind =
        self#build_ATuple (List.map ~f:(self#visit_t ctx) ls) kind
      method visit_AHashIdx ctx q l (h: hash_idx) =
        let kv =
          Eval.eval ctx q
          |> Seq.map ~f:(fun key_ctx ->
                 let _, key =
                   match Map.to_alist key_ctx with
                   | [(name, key)] -> (name, key)
                   | _ -> failwith "Unexpected key tuple shape."
                 in
                 let value = self#visit_t (Map.merge_right ctx key_ctx) l in
                 (Db.Value.of_primvalue key, value) )
        in
        self#build_AHashIdx kv h
      method visit_AEmpty _ = self#build_AEmpty
      method visit_AScalar ctx e =
        let l =
          Layout.of_value
            {value= eval_pred ctx e; rel= Db.Relation.dummy; field= Db.Field.dummy}
        in
        self#build_AScalar l
      method visit_AOrderedIdx ctx q l h =
        let kv =
          Eval.eval ctx q
          |> Seq.map ~f:(fun key_ctx ->
                 let _, key =
                   match Map.to_alist key_ctx with
                   | [(name, key)] -> (name, key)
                   | _ -> failwith "Unexpected key tuple shape."
                 in
                 let value = self#visit_t (Map.merge_right ctx key_ctx) l in
                 (Db.Value.of_primvalue key, value) )
        in
        self#build_AOrderedIdx kv h
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
        | Dedup _ | Agg _ | Scan _ | OrderBy _ ->
            Error.create "Wrong context." r [%sexp_of : t] |> Error.raise
    end

  let materialize ?(ctx= Map.empty (module Name)) l =
    let open Layout in
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
            { field= Db.Field.of_name "fixme"
            ; lookup= PredCtx.Key.Field (Db.Field.of_name "fixme") }
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
        method build_Select exprs t = FuncT ([t], `Width (List.length exprs))
        method build_Filter t = FuncT ([t], `Child_sum)
        method build_Join _ _ t1 t2 = FuncT ([t1; t2], `Child_sum)
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
        method build_AHashIdx kv _ =
          let kt, vt =
            Seq.fold kv ~init:(EmptyT, EmptyT) ~f:(fun (kt, vt1) (kv, vt2) ->
                ( unify_exn kt (Layout.of_value kv |> type_of_scalar_layout)
                , unify_exn vt1 vt2 ) )
          in
          TableT (kt, vt, {count= None})
        method build_AOrderedIdx kv _ =
          let kt, vt =
            Seq.fold kv ~init:(EmptyT, EmptyT) ~f:(fun (kt, vt1) (kv, vt2) ->
                ( unify_exn kt (Layout.of_value kv |> type_of_scalar_layout)
                , unify_exn vt1 vt2 ) )
          in
          OrderedIdxT (kt, vt, {count= None})
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
                (Eval.load_relation table).fields
                |> List.map ~f:(fun f -> Name.of_field ~rel:table f)
            | OrderBy _ -> failwith ""
          in
          Meta.set {node= node'; meta} Meta.schema schema
      end
    in
    mapper#visit_t ()

  (** Annotate names in an algebra expression with types. *)
  let resolve ?(params= Set.empty (module Name)) r =
    let resolve_relation r_name =
      let r = Eval.load_relation r_name in
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
          Set.find (Set.union params ctx) ~f:(fun n' ->
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
        | OrderBy _ -> failwith ""
      in
      ({node= node'; meta}, ctx')
    in
    let r, _ = resolve (Set.empty (module Name)) r in
    r
end
