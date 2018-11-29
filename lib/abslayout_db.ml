open Base
open Printf
open Collections

module type S = Abslayout_db_intf.S

module Config = struct
  module type S = sig
    val conn : Db.t
  end
end

module Make (Config : Config.S) = struct
  open Config
  open Abslayout0
  open Abslayout

  type eval_ctx =
    [ `Concat of eval_ctx Gen.t
    | `Empty
    | `For of (eval_ctx * eval_ctx) Gen.t
    | `Scalar of Value.t ]

  let rec gen_query q =
    match q.node with
    | AList (q1, q2) -> `For (q1, gen_query q2)
    | AHashIdx (q1, q2, _) | AOrderedIdx (q1, q2, _) ->
        `Concat [gen_query q1; `For (q1, gen_query q2)]
    | AEmpty -> `Empty
    | AScalar p -> `Scalar p
    | ATuple (ts, _) -> `Concat (List.map ts ~f:gen_query)
    | Select (_, q) | Filter (_, q) -> gen_query q
    | Scan _ | Dedup _ | OrderBy _ | GroupBy _ | Join _ | As _ ->
        failwith "Unsupported."

  let rec subst ctx = function
    | `For (q1, q2) -> `For (Abslayout.subst ctx q1, subst ctx q2)
    | `Concat qs -> `Concat (List.map ~f:(subst ctx) qs)
    | `Scalar p -> `Scalar (subst_pred ctx p)
    | `Empty -> `Empty

  let rec junk_all = function
    | `For cs -> Gen.iter cs ~f:(fun (c1, c2) -> junk_all c1 ; junk_all c2)
    | `Concat cs -> Gen.iter cs ~f:junk_all
    | `Empty | `Scalar _ -> ()

  let rec query_to_sql q =
    match q with
    | `For (q1, q2) ->
        let sql1, s1 = Sql.(to_subquery (of_ralgebra q1)) in
        let q2 =
          let ctx =
            List.zip_exn s1 Meta.(find_exn q1 schema)
            |> List.map ~f:(fun (n, n') -> (n, Name n'))
            |> Map.of_alist_exn (module Name.Compare_no_type)
          in
          subst ctx q2
        in
        let sql2, s2 = query_to_sql q2 |> Sql.to_subquery in
        let q1_fields = List.map s1 ~f:Name.to_sql in
        let q2_fields = List.map s2 ~f:Name.to_sql in
        let fields_str = String.concat ~sep:", " (q1_fields @ q2_fields) in
        let q1_fields_str = String.concat q1_fields ~sep:", " in
        { sql=
            `Subquery
              (sprintf "select %s from %s, lateral %s order by (%s)" fields_str sql1
                 sql2 q1_fields_str)
        ; schema= s1 @ s2 }
    | `Concat qs ->
        let queries = List.map qs ~f:query_to_sql in
        let queries, schemas = List.map queries ~f:Sql.to_subquery |> List.unzip in
        let queries =
          List.mapi queries ~f:(fun i q ->
              let select_list =
                List.mapi schemas ~f:(fun j ns ->
                    let names = List.map ns ~f:Name.to_sql in
                    if i = j then names
                    else List.map names ~f:(fun n -> sprintf "null as %s" n) )
                |> List.concat |> String.concat ~sep:", "
              in
              sprintf "select %d, %s from (%s)" i select_list q )
        in
        let query = String.concat ~sep:" union all " queries in
        let schema = List.concat schemas in
        {sql= `Subquery query; schema}
    | `Empty -> {sql= `Subquery "select limit 0"; schema= []}
    | `Scalar p ->
        { sql= `Subquery (sprintf "select %s as x" (Sql.pred_to_sql p))
        ; schema= [Name.create "x"] }

  let eval_query q =
    let sql = query_to_sql q |> Sql.to_query in
    let tups = Db.exec_cursor conn sql in
    let rec eval tups = function
      | `For (q1, q2) ->
          let extract_tup s t = List.take t (List.length s) in
          let outer_schema = Meta.(find_exn q1 schema) in
          let eq t1 t2 =
            [%compare.equal: string list]
              (extract_tup outer_schema t1)
              (extract_tup outer_schema t2)
          in
          `For
            ( Gen.group_lazy eq tups
            |> Gen.map ~f:(fun (t, ts) ->
                   let ctx : eval_ctx =
                     match (t, outer_schema) with
                     | [], _ -> failwith "Unexpected empty key."
                     | [v], [n] -> `Scalar (Db.load_value_exn (Name.type_exn n) v)
                     | _ ->
                         `Concat
                           (Gen.of_list
                              (List.map2_exn t outer_schema ~f:(fun x n ->
                                   `Scalar (Db.load_value_exn (Name.type_exn n) x)
                               )))
                   in
                   (ctx, eval ts q2) ) )
      | `Concat qs ->
          let eq t1 t2 = String.(List.hd_exn t1 = List.hd_exn t2) in
          let streams = Gen.group_lazy eq tups |> Gen.map ~f:(fun (_, ts) -> ts) in
          let streams = Gen.map2 streams (Gen.of_list qs) ~f:eval in
          `Concat streams
      | `Empty ->
          if Gen.is_empty tups then `Empty
          else failwith "Expected an empty generator."
      | `Scalar p -> (
        match Gen.next tups with
        | Some [x] ->
            `Scalar (Db.load_value_exn (Name.type_exn (pred_to_schema p)) x)
        | Some _ -> failwith "Unexpected tuple width."
        | None -> failwith "Expected a tuple." )
    in
    eval tups q

  class virtual ['ctx, 'a] material_fold =
    object (self)
      method virtual build_AList
          : 'ctx -> Meta.t -> t * t -> (eval_ctx * eval_ctx) Gen.t -> 'a

      method virtual build_ATuple : _

      method virtual build_AHashIdx
          :    'ctx
            -> Meta.t
            -> t * t * hash_idx
            -> eval_ctx
            -> (eval_ctx * eval_ctx) Gen.t
            -> 'a

      method virtual build_AOrderedIdx
          :    'ctx
            -> Meta.t
            -> t * t * ordered_idx
            -> eval_ctx
            -> (eval_ctx * eval_ctx) Gen.t
            -> 'a

      method virtual build_AEmpty : 'ctx -> Meta.t -> 'a

      method virtual build_AScalar : 'ctx -> Meta.t -> pred -> Value.t -> 'a

      method virtual build_Select
          : 'ctx -> Meta.t -> pred list * t -> eval_ctx -> 'a

      method virtual build_Filter : 'ctx -> Meta.t -> pred * t -> eval_ctx -> 'a

      method visit_t ctx eval_ctx r =
        match (r.node, eval_ctx) with
        | AEmpty, `Empty -> self#build_AEmpty ctx r.meta
        | AScalar x, `Scalar v -> self#build_AScalar ctx r.meta x v
        | AList x, `For vs -> self#build_AList ctx r.meta x vs
        | AHashIdx x, `Concat vs ->
            let key_ctx = Gen.get_exn vs in
            let value_gen =
              match Gen.get_exn vs with
              | `For g -> g
              | _ ->
                  Error.create "Bug: Mismatched context." r [%sexp_of: t]
                  |> Error.raise
            in
            self#build_AHashIdx ctx r.meta x key_ctx value_gen
        | AOrderedIdx x, `Concat vs ->
            let key_ctx = Gen.get_exn vs in
            let value_gen =
              match Gen.get_exn vs with
              | `For g -> g
              | _ ->
                  Error.create "Bug: Mismatched context." r [%sexp_of: t]
                  |> Error.raise
            in
            self#build_AOrderedIdx ctx r.meta x key_ctx value_gen
        | ATuple x, `Concat vs -> self#build_ATuple ctx r.meta x vs
        | Select x, _ -> self#build_Select ctx r.meta x eval_ctx
        | Filter x, _ -> self#build_Filter ctx r.meta x eval_ctx
        | (AEmpty | AScalar _ | AList _ | AHashIdx _ | AOrderedIdx _ | ATuple _), _
          ->
            Error.create "Bug: Mismatched context." r [%sexp_of: t] |> Error.raise
        | (Join _ | Dedup _ | OrderBy _ | Scan _ | GroupBy _ | As _), _ ->
            Error.create "Cannot materialize." r [%sexp_of: t] |> Error.raise
    end

  module TF = struct
    open Type

    class type_fold =
      object (self)
        inherit [_, _] material_fold

        method build_Select () _ (exprs, r) ctx =
          let t = self#visit_t () ctx r in
          FuncT ([t], `Width (List.length exprs))

        method build_Filter () _ (_, r) ctx =
          let t = self#visit_t () ctx r in
          FuncT ([t], `Child_sum)

        method build_AEmpty () _ = EmptyT

        method build_AScalar () _ _ v =
          match v with
          | Value.Date x ->
              let x = Date.to_int x in
              IntT {range= AbsInt.abstract x; nullable= false}
          | Int x -> IntT {range= AbsInt.abstract x; nullable= false}
          | Bool _ -> BoolT {nullable= false}
          | String x ->
              StringT {nchars= AbsInt.abstract (String.length x); nullable= false}
          | Null -> NullT
          | Fixed x -> FixedT {value= AbsFixed.of_fixed x; nullable= false}

        method build_AList () _ (_, elem_query) gen =
          let elem_type, num_elems =
            Gen.fold gen ~init:(EmptyT, 0) ~f:(fun (t, c) (_, ctx) ->
                let t' = self#visit_t () ctx elem_query in
                (unify_exn t t', c + 1) )
          in
          ListT (elem_type, {count= AbsInt.abstract num_elems})

        method build_ATuple () _ (ls, kind) gen =
          let elem_types =
            Gen.map2 gen (Gen.of_list ls) ~f:(self#visit_t ()) |> Gen.to_list
          in
          let counts = List.map elem_types ~f:count in
          match kind with
          | Zip ->
              TupleT
                (elem_types, {count= List.fold_left1_exn ~f:AbsCount.unify counts})
          | Concat ->
              TupleT
                (elem_types, {count= List.fold_left1_exn ~f:AbsCount.( + ) counts})
          | Cross ->
              TupleT
                (elem_types, {count= List.fold_left1_exn ~f:AbsCount.( * ) counts})

        method build_AHashIdx () _ (_, value_query, {hi_key_layout= m_key_query; _})
            key_ctx value_gen =
          let key_query = Option.value_exn m_key_query in
          junk_all key_ctx ;
          let kt, vt =
            Gen.fold value_gen ~init:(EmptyT, EmptyT)
              ~f:(fun (kt, vt) (kctx, vctx) ->
                let kt' = self#visit_t () kctx key_query in
                let vt' = self#visit_t () vctx value_query in
                (unify_exn kt kt', unify_exn vt vt') )
          in
          HashIdxT (kt, vt, {count= None})

        method build_AOrderedIdx () _
            (_, value_query, {oi_key_layout= m_key_query; _}) key_ctx value_gen =
          let key_query = Option.value_exn m_key_query in
          junk_all key_ctx ;
          let kt, vt =
            Gen.fold value_gen ~init:(EmptyT, EmptyT)
              ~f:(fun (kt, vt) (kctx, vctx) ->
                let kt' = self#visit_t () kctx key_query in
                let vt' = self#visit_t () vctx value_query in
                (unify_exn kt kt', unify_exn vt vt') )
          in
          OrderedIdxT (kt, vt, {count= None})
      end
  end

  include TF

  let to_type l =
    Logs.info (fun m -> m "Computing type of abstract layout.") ;
    let type_ = (new type_fold)#visit_t () (eval_query (gen_query l)) l in
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
            | ATuple (rs, (Cross | Zip)) ->
                List.concat_map ~f:(fun r -> Meta.(find_exn r schema)) rs
            | ATuple ([], Concat) -> []
            | ATuple (r :: _, Concat) -> Meta.(find_exn r schema)
            | As (n, r) ->
                Meta.(find_exn r schema)
                |> List.map ~f:(fun x -> {x with relation= Some n})
            | Scan table ->
                (Db.Relation.from_db conn table).fields
                |> List.map ~f:(fun f ->
                       Name.create ~relation:table ~type_:f.Db.Field.type_ f.fname
                   )
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
      let r = Db.Relation.from_db conn r_name in
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
            let ctx = List.hd_exn ctxs in
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
            (AHashIdx (r, l, m), union key_ctx value_ctx)
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
