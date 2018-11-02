open Base
open Collections

module type S = Abslayout_db_intf.S

module Make (Eval : Eval.S) = struct
  open Abslayout0
  open Abslayout

  let fresh = Fresh.create ()

  let partition ~part:p ~lookup l =
    let rel =
      match pred_relations p with
      | [r] -> r
      | rs ->
          Error.create "Unexpected number of relations." rs [%sexp_of: string list]
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
    hash_idx domain layout lookup

  type eval_ctx =
    [ `Eval of Ctx.t
    | `Consume_outer of (Ctx.t * Ctx.t Seq.t) Seq.t sexp_opaque
    | `Consume_inner of Ctx.t * Ctx.t Seq.t sexp_opaque ]
  [@@deriving sexp]

  class virtual ['self] material_fold =
    object (self : 'self)
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

      method visit_AList ctx elem_query elem_layout =
        ( match ctx with
        | `Eval ctx ->
            Eval.eval ctx elem_query
            |> Seq.map ~f:(fun ctx -> self#visit_t (`Eval ctx) elem_layout)
        | `Consume_outer ctxs ->
            Seq.map ctxs ~f:(fun ctx ->
                self#visit_t (`Consume_inner ctx) elem_layout )
        | `Consume_inner (_, ctxs) ->
            Seq.map ctxs ~f:(fun ctx -> self#visit_t (`Eval ctx) elem_layout) )
        |> self#build_AList

      method visit_ATuple ctx ls kind =
        ( match ctx with
        | `Eval _ -> List.map ls ~f:(fun l -> self#visit_t ctx l)
        | `Consume_outer _ -> failwith "Cannot consume."
        | `Consume_inner _ as ctx ->
            (* If the tuple is in the inner loop of a foreach we pass the inner
             loop sequence to the first layout that can consume it. Other
             layouts that can consume are expected to perform evaluation. *)
            let _, ret =
              List.fold_left ls ~init:(ctx, []) ~f:(fun (ctx, ret) l ->
                  match (ctx, next_inner_loop l) with
                  | `Consume_inner (ctx', _), Some _ ->
                      (`Eval ctx', ret @ [self#visit_t (ctx :> eval_ctx) l])
                  | `Consume_inner (ctx', _), None ->
                      (ctx, ret @ [self#visit_t (`Eval ctx') l])
                  | `Eval _, _ -> (ctx, ret @ [self#visit_t (ctx :> eval_ctx) l]) )
            in
            ret )
        |> fun ts -> self#build_ATuple ts kind

      method visit_AEmpty _ = self#build_AEmpty

      method visit_AScalar ctx e =
        match ctx with
        | `Eval ctx -> self#build_AScalar (Eval.eval_pred ctx e)
        | _ -> failwith "Cannot consume."

      method visit_Index ctx q key_l value_l =
        let contexts =
          match ctx with
          | `Eval ctx ->
              Eval.eval ctx q |> Seq.map ~f:(fun ctx -> (`Eval ctx, `Eval ctx))
          | `Consume_outer ctxs ->
              Seq.map ctxs ~f:(fun (ctx, child_ctxs) ->
                  (`Eval ctx, `Consume_inner (ctx, child_ctxs)) )
          | `Consume_inner (_, ctxs) ->
              Seq.map ctxs ~f:(fun ctx -> (`Eval ctx, `Eval ctx))
        in
        Seq.map contexts ~f:(fun (kctx, vctx) ->
            let key = self#visit_t kctx key_l in
            let value = self#visit_t vctx value_l in
            (key, value) )

      method visit_AHashIdx ctx q value_l (h : hash_idx) =
        let key_l =
          Option.value_exn
            ~error:(Error.create "Missing key layout." h [%sexp_of: hash_idx])
            h.hi_key_layout
        in
        self#build_AHashIdx (self#visit_Index ctx q key_l value_l) h

      method visit_AOrderedIdx ctx q value_l (h : ordered_idx) =
        let key_l =
          Option.value_exn
            ~error:(Error.create "Missing key layout." h [%sexp_of: ordered_idx])
            h.oi_key_layout
        in
        self#build_AOrderedIdx (self#visit_Index ctx q key_l value_l) h

      method visit_Select ctx exprs r' =
        self#build_Select exprs (self#visit_t ctx r')

      method visit_Filter ctx _ r' = self#build_Filter (self#visit_t ctx r')

      method visit_Join ctx pred r1 r2 =
        self#build_Join ctx pred (self#visit_t ctx r1) (self#visit_t ctx r2)

      method visit_t ctx r =
        match (r.node, ctx) with
        | AEmpty, _ -> self#visit_AEmpty ctx
        | AScalar e, _ -> self#visit_AScalar ctx e
        | AList (q, r'), `Eval ctx
          when Meta.(find r use_foreach |> Option.value ~default:true) -> (
          match next_inner_loop r' with
          | Some (_, q') ->
              let ctx = `Consume_outer (Eval.eval_foreach ctx q q') in
              self#visit_AList ctx q r'
          | None -> self#visit_AList (`Eval ctx) q r' )
        | AList (q, r'), _ -> self#visit_AList ctx q r'
        | AHashIdx (q, r', x), `Eval ctx
          when Meta.(find r use_foreach |> Option.value ~default:true) -> (
          match next_inner_loop r' with
          | Some (_, q') ->
              let ctx' = `Consume_outer (Eval.eval_foreach ctx q q') in
              self#visit_AHashIdx ctx' q r' x
          | None -> self#visit_AHashIdx (`Eval ctx) q r' x )
        | AHashIdx (r, a, t), _ -> self#visit_AHashIdx ctx r a t
        | AOrderedIdx (q, r', x), `Eval ctx
          when Meta.(find r use_foreach |> Option.value ~default:true) -> (
          match next_inner_loop r' with
          | Some (_, q') ->
              let ctx' = `Consume_outer (Eval.eval_foreach ctx q q') in
              self#visit_AOrderedIdx ctx' q r' x
          | None -> self#visit_AOrderedIdx (`Eval ctx) q r' x )
        | AOrderedIdx (r, a, t), _ -> self#visit_AOrderedIdx ctx r a t
        | ATuple (a, k), _ -> self#visit_ATuple ctx a k
        | Select (exprs, r'), _ -> self#visit_Select ctx exprs r'
        | Filter (pred, r'), _ -> self#visit_Filter ctx pred r'
        | Join {pred; r1; r2}, _ -> self#visit_Join ctx pred r1 r2
        | As (n, r), _ -> self#visit_As ctx n r
        | (Dedup _ | GroupBy _ | Scan _ | OrderBy _), _ ->
            Error.create "Wrong context." r [%sexp_of: t] |> Error.raise
    end

  module TF = struct
    open Type

    class ['self] type_fold =
      object (_ : 'self)
        inherit [_] material_fold

        method build_Select exprs t = FuncT ([t], `Width (List.length exprs))

        method build_Filter t = FuncT ([t], `Child_sum)

        method build_Join _ _ t1 t2 = FuncT ([t1; t2], `Child_sum)

        method build_AEmpty = EmptyT

        method build_AScalar =
          function
          | Int x -> IntT {range= AbsInt.abstract x; nullable= false}
          | Bool _ -> BoolT {nullable= false}
          | String x ->
              StringT {nchars= AbsInt.abstract (String.length x); nullable= false}
          | Null -> NullT
          | Fixed x -> FixedT {value= AbsFixed.of_fixed x; nullable= false}

        method build_AList ls =
          let t, c =
            Seq.fold ls ~init:(EmptyT, AbsInt.zero) ~f:(fun (t, c) t' ->
                (unify_exn t t', AbsInt.(c + abstract 1)) )
          in
          ListT (t, {count= c})

        method build_ATuple ls kind =
          let counts = List.map ls ~f:count in
          match kind with
          | Zip -> TupleT (ls, {count= List.fold_left1_exn ~f:AbsCount.unify counts})
          | Concat ->
              TupleT (ls, {count= List.fold_left1_exn ~f:AbsCount.( + ) counts})
          | Cross ->
              TupleT (ls, {count= List.fold_left1_exn ~f:AbsCount.( * ) counts})

        method build_AHashIdx kv _ =
          let kt, vt =
            Seq.fold kv ~init:(EmptyT, EmptyT) ~f:(fun (kt1, vt1) (kt2, vt2) ->
                (unify_exn kt1 kt2, unify_exn vt1 vt2) )
          in
          HashIdxT (kt, vt, {count= None})

        method build_AOrderedIdx kv _ =
          let kt, vt =
            Seq.fold kv ~init:(EmptyT, EmptyT) ~f:(fun (kt1, vt1) (kt2, vt2) ->
                (unify_exn kt1 kt2, unify_exn vt1 vt2) )
          in
          OrderedIdxT (kt, vt, {count= None})
      end
  end

  include TF

  let to_type l =
    Logs.info (fun m -> m "Computing type of abstract layout.") ;
    let type_ =
      (new type_fold)#visit_t (`Eval (Map.empty (module Name.Compare_no_type))) l
    in
    Logs.info (fun m ->
        m "The type is: %s" (Sexp.to_string_hum ([%sexp_of: Type.t] type_)) ) ;
    type_

  let annotate_subquery_types =
    let annotate_type r =
      let type_ = to_type r in
      Meta.(set_m r type_) type_
    in
    let visitor =
      object
        inherit runtime_subquery_visitor

        method visit_Subquery r = annotate_type r
      end
    in
    visitor#visit_t ()

  (* let schema_visitor =
   *   let unnamed t = {name= ""; relation= None; type_= Some t} in
   *   let int_t = unnamed (Type.PrimType.IntT {nullable= false}) in
   *   let bool_t = unnamed (Type.PrimType.BoolT {nullable= false}) in
   *   let fixed_t = unnamed (Type.PrimType.FixedT {nullable= false}) in
   *   let string_t = unnamed (Type.PrimType.StringT {nullable= false}) in
   *   let null_t = unnamed Type.PrimType.NullT in
   *   let unify s1 s2 =
   *     List.map2_exn s1 s2 ~f:(fun n1 n2 ->
   *         assert (String.(n1.name = "") && String.(n2.name = "")) ;
   *         unnamed (Type.PrimType.unify (Name.type_exn n1) (Name.type_exn n2)) )
   *   in
   *   object (self : 'a)
   *     inherit [_] reduce as super
   * 
   *     inherit [_] Util.list_monoid
   * 
   *     method! zero = failwith "Missing case."
   * 
   *     method! visit_As_pred () (p, n) =
   *       List.map (self#visit_pred () p) ~f:(fun s -> {s with relation= None; name= n}
   *       )
   * 
   *     method! visit_Name () n = [n]
   * 
   *     method! visit_Int () _ = [int_t]
   * 
   *     method! visit_Fixed () _ = [fixed_t]
   * 
   *     method! visit_Bool () _ = [bool_t]
   * 
   *     method! visit_Date () _ = [int_t]
   * 
   *     method! visit_String () _ = [string_t]
   * 
   *     method! visit_Null () = [null_t]
   * 
   *     method! visit_Unop () (op, _) =
   *       match op with
   *       | Year | Month | Day | Strlen | ExtractY | ExtractM | ExtractD -> [int_t]
   *       | Not -> [bool_t]
   * 
   *     method! visit_Binop () (op, p1, p2) =
   *       let s1 = self#visit_pred () p1 in
   *       let s2 = self#visit_pred () p2 in
   *       match op with
   *       | Add | Sub | Mul | Div | Mod -> unify s1 s2
   *       | Strpos -> [int_t]
   *       | Eq | Lt | Le | Gt | Ge | And | Or -> [bool_t]
   * 
   *     method! visit_Count () = [int_t]
   * 
   *     method! visit_Sum () p = self#visit_pred () p
   * 
   *     method! visit_Min () p = self#visit_pred () p
   * 
   *     method! visit_Max () p = self#visit_pred () p
   * 
   *     method! visit_Avg () _ = [fixed_t]
   * 
   *     method! visit_Exists () r = self#visit_t () r
   * 
   *     method! visit_Select () (ps, _) = List.concat_map ps ~f:(self#visit_pred ())
   * 
   *     method! visit_Filter () (_, r) = self#visit_t () r
   * 
   *     method! visit_Join () _ r1 r2 = self#visit_t () r1 @ self#visit_t () r2
   * 
   *     method! visit_GroupBy () ps _ _ = List.concat_map ps ~f:(self#visit_pred ())
   * 
   *     method! visit_OrderBy () _ _ r = self#visit_t () r
   * 
   *     method! visit_Dedup () r = self#visit_t () r
   * 
   *     method! visit_Scan () table =
   *       (Eval.load_relation table).fields
   *       |> List.map ~f:(fun f -> Name.of_field ~rel:table f)
   * 
   *     method! visit_AEmpty () = []
   * 
   *     method! visit_AScalar () p = self#visit_pred () p
   * 
   *     method! visit_AList () (_, r) = self#visit_t () r
   * 
   *     method! visit_ATuple () (rs, _) = List.concat_map rs ~f:(self#visit_t ())
   * 
   *     method! visit_AOrderedIdx () (r1, r2, _) =
   *       self#visit_t () r1 @ self#visit_t () r2
   * 
   *     method! visit_AHashIdx () (r1, r2, _) =
   *       self#visit_t () r1 @ self#visit_t () r2
   * 
   *     method! visit_As () n r =
   *       List.map (self#visit_t () r) ~f:(fun x -> {x with relation= Some n})
   *   end *)

  let rec pred_to_schema =
    let open Type.PrimType in
    let unnamed t = {name= ""; relation= None; type_= Some t} in
    function
    | As_pred (p, n) ->
        let schema = pred_to_schema p in
        {schema with relation= None; name= n}
    | Name n -> n
    | Int _ | Date _
     |Unop ((Year | Month | Day | Strlen | ExtractY | ExtractM | ExtractD), _)
     |Count ->
        unnamed (IntT {nullable= false})
    | Fixed _ | Avg _ -> unnamed (FixedT {nullable= false})
    | Bool _ | Exists _
     |Binop ((Eq | Lt | Le | Gt | Ge | And | Or), _, _)
     |Unop (Not, _) ->
        unnamed (BoolT {nullable= false})
    | String _ -> unnamed (StringT {nullable= false})
    | Null -> unnamed NullT
    | Binop ((Add | Sub | Mul | Div | Mod), p1, p2) ->
        let s1 = pred_to_schema p1 in
        let s2 = pred_to_schema p2 in
        unnamed (unify (Name.type_exn s1) (Name.type_exn s2))
    | Binop (Strpos, _, _) -> unnamed (IntT {nullable= false})
    | Sum p | Min p | Max p -> pred_to_schema p
    | If (_, p1, p2) ->
        let s1 = pred_to_schema p1 in
        let s2 = pred_to_schema p2 in
        Type.PrimType.unify (Name.type_exn s1) (Name.type_exn s2) |> ignore ;
        unnamed (Name.type_exn s1)
    | First r -> (
        annotate_schema r ;
        match Meta.(find_exn r schema) with
        | [n] -> n
        | [] -> failwith "Unexpected empty schema."
        | _ -> failwith "Too many fields." )
    | Substring _ -> unnamed (StringT {nullable= false})

  (** Add a schema field to each metadata node. Variables must first be
     annotated with type information. *)
  and annotate_schema r =
    let mapper =
      object (self : 'a)
        inherit [_] iter as super

        method! visit_t () r =
          super#visit_t () r ;
          let schema =
            match r.node with
            | Select (x, _) ->
                List.map x ~f:(fun p -> self#visit_pred () p ; pred_to_schema p)
            | Filter (_, r) | Dedup r | AList (_, r) | OrderBy {rel= r; _} ->
                Meta.(find_exn r schema)
            | Join {r1; r2; _} | AOrderedIdx (r1, r2, _) | AHashIdx (r1, r2, _) ->
                Meta.(find_exn r1 schema) @ Meta.(find_exn r2 schema)
            | GroupBy (x, _, _) -> List.map x ~f:pred_to_schema
            | AEmpty -> []
            | AScalar e -> self#visit_pred () e ; [pred_to_schema e]
            | ATuple (rs, _) ->
                List.concat_map ~f:(fun r -> Meta.(find_exn r schema)) rs
            | As (n, r) ->
                Meta.(find_exn r schema)
                |> List.map ~f:(fun x -> {x with relation= Some n})
            | Scan table ->
                (Eval.load_relation table).fields
                |> List.map ~f:(fun f -> Name.of_field ~rel:table f)
          in
          Meta.set_m r Meta.schema schema
      end
    in
    mapper#visit_t () r

  let to_schema r =
    annotate_schema r ;
    Meta.(find_exn r schema)

  let annotate_key_layouts =
    let key_layout schema =
      let layout =
        match List.map schema ~f:(fun n -> scalar (Name n)) with
        | [] -> failwith "empty schema"
        | [x] -> x
        | xs -> tuple xs Cross
      in
      annotate_schema layout ; layout
    in
    let annotator =
      object (self : 'a)
        inherit [_] map

        method! visit_AHashIdx () ((x, y, ({hi_key_layout; _} as m)) as r) =
          let x = self#visit_t () x in
          let y = self#visit_t () y in
          match hi_key_layout with
          | Some _ -> AHashIdx r
          | None ->
              let schema = Meta.find_exn x Meta.schema in
              AHashIdx (x, y, {m with hi_key_layout= Some (key_layout schema)})

        method! visit_AOrderedIdx () ((x, y, ({oi_key_layout; _} as m)) as r) =
          let x = self#visit_t () x in
          let y = self#visit_t () y in
          match oi_key_layout with
          | Some _ -> AOrderedIdx r
          | None ->
              let schema = Meta.find_exn x Meta.schema in
              AOrderedIdx (x, y, {m with oi_key_layout= Some (key_layout schema)})
      end
    in
    annotator#visit_t ()

  (** Annotate names in an algebra expression with types. *)
  let resolve ?(params = Set.empty (module Name.Compare_no_type)) r =
    let resolve_relation r_name =
      let r = Eval.load_relation r_name in
      List.map r.fields ~f:(fun f ->
          {relation= Some r.rname; name= f.fname; type_= Some f.type_} )
      |> Set.of_list (module Name.Compare_no_type)
    in
    let rename name s =
      Set.map
        (module Name.Compare_no_type)
        s
        ~f:(fun n -> {n with relation= Some name})
    in
    let resolve_name ctx n =
      let could_not_resolve =
        Error.create "Could not resolve." (n, ctx)
          [%sexp_of: Name.t * Set.M(Name).t]
      in
      match (n.type_, n.relation) with
      | Some _, Some _ -> n
      | Some _, None -> n
      | None, Some _ -> (
        match Set.find ctx ~f:(fun n' -> Name.Compare_no_type.(n' = n)) with
        | Some n' -> n'
        | None -> Error.raise could_not_resolve )
      | None, None -> (
          let matches =
            Set.to_list ctx
            |> List.filter ~f:(fun n' -> Name.Compare_name_only.(n = n'))
          in
          match matches with
          | [] -> Error.raise could_not_resolve
          | [n'] -> n'
          | n' :: n'' :: _ ->
              Error.create "Ambiguous name." (n, n', n'')
                [%sexp_of: Name.t * Name.t * Name.t]
              |> Error.raise )
    in
    let empty_ctx = Set.empty (module Name.Compare_no_type) in
    let preds_to_names preds =
      List.map preds ~f:pred_to_schema
      |> List.filter ~f:(fun n -> String.(n.name <> ""))
      |> Set.of_list (module Name.Compare_no_type)
    in
    let pred_to_name pred =
      let n = pred_to_schema pred in
      if String.(n.name = "") then None else Some n
    in
    let union c1 c2 = Set.union c1 c2 in
    let union_list =
      List.fold_left ~init:(Set.empty (module Name.Compare_no_type)) ~f:union
    in
    let rec resolve_pred ctx =
      let visitor =
        object
          inherit [_] endo

          method! visit_Name ctx _ n = Name (resolve_name ctx n)

          method! visit_Exists ctx _ r =
            let r', _ = resolve ctx r in
            Exists r'

          method! visit_First ctx _ r =
            let r', _ = resolve ctx r in
            First r'
        end
      in
      visitor#visit_pred ctx
    and resolve outer_ctx {node; meta} =
      let node', ctx' =
        match node with
        | Select (preds, r) ->
            let r, preds =
              let r, inner_ctx = resolve outer_ctx r in
              let ctx = union outer_ctx inner_ctx in
              (r, List.map preds ~f:(resolve_pred ctx))
            in
            (Select (preds, r), preds_to_names preds)
        | Filter (pred, r) ->
            let r, value_ctx = resolve outer_ctx r in
            let pred = resolve_pred (union outer_ctx value_ctx) pred in
            (Filter (pred, r), value_ctx)
        | Join {pred; r1; r2} ->
            let r1, inner_ctx1 = resolve outer_ctx r1 in
            let r2, inner_ctx2 = resolve outer_ctx r2 in
            let ctx = union_list [inner_ctx1; inner_ctx2; outer_ctx] in
            let pred = resolve_pred ctx pred in
            (Join {pred; r1; r2}, union inner_ctx1 inner_ctx2)
        | Scan l -> (Scan l, resolve_relation l)
        | GroupBy (aggs, key, r) ->
            let r, inner_ctx = resolve outer_ctx r in
            let ctx = union outer_ctx inner_ctx in
            let aggs = List.map ~f:(resolve_pred ctx) aggs in
            let key = List.map key ~f:(resolve_name ctx) in
            (GroupBy (aggs, key, r), preds_to_names aggs)
        | Dedup r ->
            let r, inner_ctx = resolve outer_ctx r in
            (Dedup r, inner_ctx)
        | AEmpty -> (AEmpty, empty_ctx)
        | AScalar p ->
            let p = resolve_pred outer_ctx p in
            let ctx =
              match pred_to_name p with
              | Some n -> Set.singleton (module Name.Compare_no_type) n
              | None -> empty_ctx
            in
            (AScalar p, ctx)
        | AList (r, l) ->
            let r, outer_ctx' = resolve outer_ctx r in
            let l, ctx = resolve (union outer_ctx' outer_ctx) l in
            (AList (r, l), ctx)
        | ATuple (ls, (Zip as t)) ->
            let ls, ctxs = List.map ls ~f:(resolve outer_ctx) |> List.unzip in
            let ctx = union_list ctxs in
            (ATuple (ls, t), ctx)
        | ATuple (ls, (Concat as t)) ->
            let ls, ctxs = List.map ls ~f:(resolve outer_ctx) |> List.unzip in
            let ctx =
              List.all_equal ~sexp_of_t:[%sexp_of: Set.M(Name.Compare_no_type).t]
                ctxs
              |> Or_error.ok_exn
            in
            (ATuple (ls, t), ctx)
        | ATuple (ls, (Cross as t)) ->
            let ls, ctx =
              List.fold_left ls ~init:([], empty_ctx) ~f:(fun (ls, ctx) l ->
                  let l, ctx' = resolve (union outer_ctx ctx) l in
                  (l :: ls, union ctx ctx') )
            in
            (ATuple (List.rev ls, t), ctx)
        | AHashIdx (r, l, m) ->
            let r, key_ctx = resolve outer_ctx r in
            let l, value_ctx = resolve (union outer_ctx key_ctx) l in
            let m =
              (object
                 inherit [_] map

                 method! visit_pred _ = resolve_pred outer_ctx
              end)
                #visit_hash_idx () m
            in
            (AHashIdx (r, l, m), value_ctx)
        | AOrderedIdx (r, l, m) ->
            let r, key_ctx = resolve outer_ctx r in
            let l, value_ctx = resolve (union outer_ctx key_ctx) l in
            let m =
              (object
                 inherit [_] map

                 method! visit_pred _ = resolve_pred outer_ctx
              end)
                #visit_ordered_idx () m
            in
            (AOrderedIdx (r, l, m), value_ctx)
        | As (n, r) ->
            let r, ctx = resolve outer_ctx r in
            let ctx = rename n ctx in
            (As (n, r), ctx)
        | OrderBy ({key; rel; _} as x) ->
            let rel, inner_ctx = resolve outer_ctx rel in
            let key = List.map key ~f:(resolve_pred inner_ctx) in
            (OrderBy {x with key; rel}, inner_ctx)
      in
      ({node= node'; meta}, ctx')
    in
    let r, _ = resolve params r in
    r
end
