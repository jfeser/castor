open Base
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
    [ `Concat of eval_ctx list
    | `Empty
    | `For of (Value.t list * eval_ctx) Gen.t
    | `Scalar of Value.t Lazy.t
    | `Query of Value.t list Gen.t ]

  let rec width = function
    | `Empty -> 0
    | `For (q1, q2) -> List.length Meta.(find_exn q1 schema) + width q2
    | `Scalar _ -> 1
    | `Concat qs -> 1 + List.sum (module Int) qs ~f:width
    | `Query q -> List.length Meta.(find_exn q schema)

  let rec pred_to_schema =
    let open Type.PrimType in
    let unnamed t = {name= ""; relation= None; type_= Some t} in
    function
    | As_pred (p, n) ->
        let schema = pred_to_schema p in
        {schema with relation= None; name= n}
    | Name n -> n
    | Date _ | Unop ((Year | Month | Day), _) -> unnamed (DateT {nullable= false})
    | Int _ | Unop ((Strlen | ExtractY | ExtractM | ExtractD), _) | Count ->
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

  let total_order_key q =
    let native_order = Abslayout.order_of q in
    let total_order =
      List.map Meta.(find_exn q schema) ~f:(fun n -> (Name n, Asc))
    in
    native_order @ total_order

  let rec gen_query q =
    (* TODO: Respect orderings from original queries. Needs a way to get the
       ordering, then the ordering has to be extended with the remaining fields
       so that it is total. *)
    match q.node with
    | AList (q1, q2) ->
        let q1 =
          let order_key = total_order_key q1 in
          let q1 = order_by order_key q1 in
          annotate_schema q1 ; q1
        in
        `For (q1, gen_query q2)
    | AHashIdx (q1, q2, _) | AOrderedIdx (q1, q2, _) ->
        let q1 =
          let order_key = total_order_key q1 in
          let q1 = order_by order_key (dedup q1) in
          annotate_schema q1 ; q1
        in
        `Concat [`Query q1; `For (q1, gen_query q2)]
    | AEmpty -> `Empty
    | AScalar p -> `Scalar p
    | ATuple (ts, _) -> `Concat (List.map ts ~f:gen_query)
    | Select (_, q) | Filter (_, q) | As (_, q) -> gen_query q
    | Scan _ | Dedup _ | OrderBy _ | GroupBy _ | Join _ -> failwith "Unsupported."

  let rec subst ctx = function
    | `For (q1, q2) -> `For (Abslayout.subst ctx q1, subst ctx q2)
    | `Concat qs -> `Concat (List.map ~f:(subst ctx) qs)
    | `Scalar p -> `Scalar (subst_pred ctx p)
    | `Empty -> `Empty
    | `Query q -> `Query (Abslayout.subst ctx q)

  let to_ctx : _ -> eval_ctx = function
    | [] -> failwith "unexpected empty tuple"
    | [v] -> `Scalar (lazy v)
    | vs -> `Concat (List.map vs ~f:(fun v -> `Scalar (lazy v)))

  let rec to_schema = function
    | `For (q1, q2) ->
        (Meta.(find_exn q1 schema) |> List.map ~f:Name.type_exn) @ to_schema q2
    | `Concat qs -> IntT {nullable= false} :: List.concat_map qs ~f:to_schema
    | `Scalar p -> [pred_to_schema p |> Name.type_exn]
    | `Query q -> Meta.(find_exn q schema) |> List.map ~f:Name.type_exn
    | `Empty -> []

  let query_to_sql fresh q =
    let ctx = Sql.create_ctx ~fresh () in
    let rec query_to_sql q =
      match q with
      | `For (q1, q2) ->
          let open Sql in
          let spj1 = of_ralgebra ctx q1 |> to_spj ctx in
          let sql1 = Query spj1 in
          let sql1_no_order = Query {spj1 with order= []} in
          let sql1_names = to_schema (Query spj1) in
          let q2 =
            let ctx =
              List.zip_exn Meta.(find_exn q1 schema) sql1_names
              |> List.map ~f:(fun (n, n') ->
                     (n, Name (Name.create ?type_:n.type_ n')) )
              |> Map.of_alist_exn (module Name.Compare_no_type)
            in
            subst ctx q2
          in
          let spj2 = query_to_sql q2 |> to_spj ctx in
          let sql2 = Query spj2 in
          let sql2_no_order = Query {spj2 with order= []} in
          let select_list =
            let sql2_names = to_schema sql2 in
            List.map (sql1_names @ sql2_names) ~f:(fun n ->
                add_pred_alias ctx (Name (Name.create n)) )
          in
          let order =
            (to_order sql1 |> Or_error.ok_exn) @ (to_order sql2 |> Or_error.ok_exn)
          in
          Query
            (create_query ~order
               ~relations:
                 [ (`Subquery (sql1_no_order, Fresh.name fresh "t%d"), `Left)
                 ; (`Subquery (sql2_no_order, Fresh.name fresh "t%d"), `Lateral) ]
               select_list)
      | `Concat qs ->
          let counter_name = Fresh.name fresh "counter%d" in
          let scalars, other_queries =
            List.partition_map qs ~f:(function
              | `Scalar p -> `Fst p
              | (`Concat _ | `Empty | `For _ | `Query _) as q -> `Snd q )
          in
          let scalar_query =
            let sql =
              Sql.create_query
                (List.map scalars ~f:(fun p -> (p, Fresh.name fresh "x%d", None)))
            in
            let types =
              List.map scalars ~f:(fun p -> p |> pred_to_schema |> Name.type_exn)
            in
            let names = Sql.(to_schema (Query sql)) in
            (sql, types, names)
          in
          let other_queries =
            List.map other_queries ~f:(fun q ->
                let sql = query_to_sql q |> Sql.to_spj ctx in
                let types = to_schema q in
                let names = Sql.(to_schema (Query sql)) in
                (sql, types, names) )
          in
          let queries = scalar_query :: other_queries in
          let queries =
            List.mapi queries ~f:(fun i (sql, _, _) ->
                let select_list =
                  (Int i, counter_name, None)
                  :: ( List.mapi queries ~f:(fun j (_, types, names) ->
                           if i = j then sql.select
                           else
                             List.map2_exn names types ~f:(fun n t ->
                                 (Null, n, Some t) ) )
                     |> List.concat )
                in
                {sql with select= select_list} )
          in
          let union =
            let spj =
              Sql.Union_all (List.map queries ~f:(fun q -> {q with order= []}))
              |> Sql.to_spj ctx
            in
            let order =
              (Name (Name.create counter_name), Asc)
              :: List.concat_map queries ~f:(fun q ->
                     Sql.to_order (Query q) |> Or_error.ok_exn )
            in
            {spj with order}
          in
          Query union
      | `Empty -> Query (Sql.create_query ~limit:0 [])
      | `Scalar p -> Query (Sql.create_query [(p, Fresh.name fresh "x%d", None)])
      | `Query q -> Sql.of_ralgebra ctx q
    in
    query_to_sql q

  let eval_query q =
    let fresh = Fresh.create () in
    let ctx = Sql.create_ctx ~fresh () in
    let sql = query_to_sql fresh q in
    let tups = Db.exec_cursor conn (to_schema q) (Sql.to_string_hum ctx sql) in
    let rec eval tups = function
      | `For (q1, q2) ->
          let extract_tup s t = List.take t (List.length s) in
          let outer_schema = Meta.(find_exn q1 schema) in
          let eq t1 t2 =
            [%compare.equal: Value.t list]
              (extract_tup outer_schema t1)
              (extract_tup outer_schema t2)
          in
          let outer_width = List.length outer_schema in
          `For
            ( Gen.group_lazy eq tups
            |> Gen.map ~f:(fun (t, ts) ->
                   let t = List.take t outer_width in
                   let ts = Gen.map ts ~f:(fun t -> List.drop t outer_width) in
                   (t, eval ts q2) ) )
      | `Concat qs ->
          let tups = ref tups in
          let put_back t = tups := Gen.append (Gen.singleton t) !tups in
          let take_while pred =
            let stop = ref false in
            let next () =
              if !stop then None
              else
                match Gen.get !tups with
                | Some x ->
                    if pred x then Some x
                    else (
                      stop := true ;
                      put_back x ;
                      None )
                | None -> None
            in
            next
          in
          let nscalars =
            List.sum (module Int) qs ~f:(function `Scalar _ -> 1 | _ -> 0)
          in
          let scalars =
            lazy
              ( match List.take (Gen.get_exn !tups) (nscalars + 1) with
              | Int ctr :: vals ->
                  if ctr = 0 then vals
                  else
                    Error.(
                      create "Unexpected counter value." ctr [%sexp_of: int]
                      |> raise)
              | _ -> failwith "No counter field." )
          in
          let widths =
            nscalars
            :: List.filter_map qs ~f:(function
                 | `Scalar _ -> None
                 | q -> Some (width q) )
          in
          let _, _, streams =
            List.fold_left qs ~init:(0, 1, []) ~f:(fun (sidx, oidx, ctxs) ->
              function
              | `Scalar _ ->
                  let ctx =
                    `Scalar (Lazy.map scalars ~f:(fun ss -> List.nth_exn ss sidx))
                  in
                  (sidx + 1, oidx, ctxs @ [ctx])
              | q ->
                  let take_ct = List.nth_exn widths oidx in
                  let drop_ct =
                    List.sum (module Int) (List.take widths oidx) ~f:(fun x -> x)
                    + 1
                  in
                  let strm =
                    take_while (fun t -> List.hd_exn t |> Value.to_int = oidx)
                    |> Gen.map ~f:(fun t ->
                           let t = List.drop t drop_ct in
                           List.take t take_ct )
                  in
                  let strm () =
                    Lazy.force scalars |> ignore ;
                    Gen.get strm
                  in
                  (sidx, oidx + 1, ctxs @ [eval strm q]) )
          in
          `Concat streams
      | `Empty ->
          if Gen.is_empty tups then `Empty
          else failwith "Expected an empty generator."
      | `Scalar p ->
          `Scalar
            ( lazy
              ( match Gen.next tups with
              | Some [x] -> x
              | Some t ->
                  Error.(
                    create "Scalar: unexpected tuple width." (p, t)
                      [%sexp_of: pred * Value.t list]
                    |> raise)
              | None -> failwith "Expected a tuple." ) )
      | `Query _ -> `Query tups
    in
    eval tups q

  class virtual ['ctx, 'a] unsafe_material_fold =
    object (self)
      method virtual private build_AList
          : 'ctx -> Meta.t -> t * t -> (Value.t list * eval_ctx) Gen.t -> 'a

      method virtual private build_ATuple : _

      method virtual private build_AHashIdx
          :    'ctx
            -> Meta.t
            -> t * t * hash_idx
            -> Value.t list Gen.t
            -> (Value.t list * eval_ctx) Gen.t
            -> 'a

      method virtual private build_AOrderedIdx
          :    'ctx
            -> Meta.t
            -> t * t * ordered_idx
            -> Value.t list Gen.t
            -> (Value.t list * eval_ctx) Gen.t
            -> 'a

      method virtual private build_AEmpty : 'ctx -> Meta.t -> 'a

      method virtual private build_AScalar
          : 'ctx -> Meta.t -> pred -> Value.t Lazy.t -> 'a

      method virtual private build_Select
          : 'ctx -> Meta.t -> pred list * t -> eval_ctx -> 'a

      method virtual private build_Filter
          : 'ctx -> Meta.t -> pred * t -> eval_ctx -> 'a

      method private visit_t ctx eval_ctx r =
        match (r.node, eval_ctx) with
        | AEmpty, `Empty -> self#build_AEmpty ctx r.meta
        | AScalar x, `Scalar v -> self#build_AScalar ctx r.meta x v
        | AList x, `For vs -> self#build_AList ctx r.meta x vs
        | AHashIdx x, `Concat [kctx; vctx] ->
            let key_gen, value_gen =
              match (kctx, vctx) with
              | `Query g1, `For g2 -> (g1, g2)
              | _ ->
                  Error.create "Bug: Mismatched context." r [%sexp_of: t]
                  |> Error.raise
            in
            self#build_AHashIdx ctx r.meta x key_gen value_gen
        | AOrderedIdx x, `Concat [kctx; vctx] ->
            let key_gen, value_gen =
              match (kctx, vctx) with
              | `Query g1, `For g2 -> (g1, g2)
              | _ ->
                  Error.create "Bug: Mismatched context." r [%sexp_of: t]
                  |> Error.raise
            in
            self#build_AOrderedIdx ctx r.meta x key_gen value_gen
        | ATuple x, `Concat vs -> self#build_ATuple ctx r.meta x vs
        | Select x, _ -> self#build_Select ctx r.meta x eval_ctx
        | Filter x, _ -> self#build_Filter ctx r.meta x eval_ctx
        | As (_, r), _ -> self#visit_t ctx eval_ctx r
        | (AEmpty | AScalar _ | AList _ | AHashIdx _ | AOrderedIdx _ | ATuple _), _
          ->
            Error.create "Bug: Mismatched context." r [%sexp_of: t] |> Error.raise
        | (Join _ | Dedup _ | OrderBy _ | Scan _ | GroupBy _), _ ->
            Error.create "Cannot materialize." r [%sexp_of: t] |> Error.raise

      method run ctx r = self#visit_t ctx (eval_query (gen_query r)) r
    end

  type ('i, 'a, 'v, 'o) fold = {pre: 'i -> 'a; body: 'a -> 'v -> 'a; post: 'a -> 'o}

  class virtual ['out, 'l, 'h1, 'h2, 'h3, 'o1, 'o2, 'o3] material_fold =
    object (self)
      inherit [unit, 'out] unsafe_material_fold

      method virtual list
          : Meta.t -> t * t -> (unit, 'l, Value.t list * 'out, 'out) fold

      method virtual hash_idx
          :    Meta.t
            -> t * t * hash_idx
            -> (unit, 'h1, Value.t list, 'h2) fold
               * ('h2, 'h3, 'out * 'out, 'out) fold

      method virtual ordered_idx
          :    Meta.t
            -> t * t * ordered_idx
            -> (unit, 'o1, Value.t list, 'o2) fold
               * ('o2, 'o3, 'out * 'out, 'out) fold

      method virtual tuple : Meta.t -> t list * tuple -> 'out list -> 'out

      method virtual empty : Meta.t -> 'out

      method virtual scalar : Meta.t -> pred -> Value.t -> 'out

      method virtual select : Meta.t -> pred list * t -> 'out -> 'out

      method virtual filter : Meta.t -> pred * t -> 'out -> 'out

      method private build_AList ctx meta ((_, r) as expr) gen =
        let {pre; body; post} = self#list meta expr in
        Gen.fold gen ~init:(pre ()) ~f:(fun acc (tup, ectx) ->
            let value = self#visit_t ctx ectx r in
            body acc (tup, value) )
        |> post

      method private build_ATuple ctx meta ((rs, _) as expr) es =
        self#tuple meta expr (List.map2_exn es rs ~f:(self#visit_t ctx))

      method private build_AHashIdx ctx meta
          ((_, value_query, {hi_key_layout= m_key_query; _}) as expr) kgen vgen =
        let key_query = Option.value_exn m_key_query in
        let f1, f2 = self#hash_idx meta expr in
        let acc = Gen.fold kgen ~init:(f1.pre ()) ~f:f1.body in
        Gen.fold vgen
          ~init:(f2.pre (f1.post acc))
          ~f:(fun acc (tup, ectx) ->
            f2.body acc
              ( self#visit_t ctx (to_ctx tup) key_query
              , self#visit_t ctx ectx value_query ) )
        |> f2.post

      method private build_AOrderedIdx ctx meta
          ((_, value_query, {oi_key_layout= m_key_query; _}) as expr) kgen vgen =
        let key_query = Option.value_exn m_key_query in
        let f1, f2 = self#ordered_idx meta expr in
        let acc = Gen.fold kgen ~init:(f1.pre ()) ~f:f1.body in
        Gen.fold vgen
          ~init:(f2.pre (f1.post acc))
          ~f:(fun acc (tup, ectx) ->
            f2.body acc
              ( self#visit_t ctx (to_ctx tup) key_query
              , self#visit_t ctx ectx value_query ) )
        |> f2.post

      method private build_AEmpty _ meta = self#empty meta

      method private build_AScalar _ meta expr gen =
        self#scalar meta expr (Lazy.force gen)

      method private build_Filter ctx meta ((_, r) as expr) ectx =
        self#filter meta expr (self#visit_t ctx ectx r)

      method private build_Select ctx meta ((_, r) as expr) ectx =
        self#select meta expr (self#visit_t ctx ectx r)
    end

  (* Wrapper module allows opening Type without clashes. *)
  module TF = struct
    open Type

    (** Returns the least general type of a layout. *)
    let rec least_general_of_layout r =
      match r.Abslayout.node with
      | Select (ps, r') | GroupBy (ps, _, r') ->
          FuncT ([least_general_of_layout r'], `Width (List.length ps))
      | OrderBy {rel= r'; _} | Filter (_, r') | Dedup r' ->
          FuncT ([least_general_of_layout r'], `Child_sum)
      | Join {r1; r2; _} ->
          FuncT
            ([least_general_of_layout r1; least_general_of_layout r2], `Child_sum)
      | AEmpty -> EmptyT
      | AScalar p ->
          Abslayout.pred_to_schema p |> Name.type_exn |> least_general_of_primtype
      | AList (_, r') -> ListT (least_general_of_layout r', {count= Bottom})
      | AHashIdx (_, vr, {hi_key_layout= Some kr; _}) ->
          HashIdxT
            (least_general_of_layout kr, least_general_of_layout vr, {count= Bottom})
      | AOrderedIdx (_, vr, {oi_key_layout= Some kr; _}) ->
          OrderedIdxT
            (least_general_of_layout kr, least_general_of_layout vr, {count= Bottom})
      | ATuple (rs, _) ->
          TupleT (List.map rs ~f:least_general_of_layout, {count= Bottom})
      | As (_, r') -> least_general_of_layout r'
      | AHashIdx (_, _, {hi_key_layout= None; _})
       |AOrderedIdx (_, _, {oi_key_layout= None; _})
       |Scan _ ->
          failwith "Layout is still abstract."

    (** Returns a layout type that is general enough to hold all of the data. *)
    class type_fold =
      object
        inherit [_, _, _, _, _, _, _, _] material_fold

        method select _ (exprs, _) t = FuncT ([t], `Width (List.length exprs))

        method filter _ _ t = FuncT ([t], `Child_sum)

        method empty _ = EmptyT

        method scalar _ _ =
          function
          | Value.Date x ->
              let x = Date.to_int x in
              DateT {range= AbsInt.of_int x; nullable= false}
          | Int x -> IntT {range= AbsInt.of_int x; nullable= false}
          | Bool _ -> BoolT {nullable= false}
          | String x ->
              StringT {nchars= AbsInt.of_int (String.length x); nullable= false}
          | Null -> NullT
          | Fixed x -> FixedT {value= AbsFixed.of_fixed x; nullable= false}

        method list _ (_, elem_l) =
          { pre= (fun () -> (least_general_of_layout elem_l, 0))
          ; body= (fun (t, c) (_, t') -> (unify_exn t t', c + 1))
          ; post=
              (fun (elem_type, num_elems) ->
                ListT (elem_type, {count= AbsInt.of_int num_elems}) ) }

        method tuple _ (_, kind) elem_types =
          let counts = List.map elem_types ~f:count in
          match kind with
          | Zip ->
              TupleT (elem_types, {count= List.fold_left1_exn ~f:AbsInt.join counts})
          | Concat ->
              TupleT
                (elem_types, {count= List.fold_left1_exn ~f:AbsInt.( + ) counts})
          | Cross ->
              TupleT
                (elem_types, {count= List.fold_left1_exn ~f:AbsInt.( * ) counts})

        method hash_idx _ (_, value_l, {hi_key_layout; _}) =
          let key_l = Option.value_exn hi_key_layout in
          ( {pre= (fun () -> ()); body= (fun () _ -> ()); post= (fun () -> ())}
          , { pre=
                (fun () ->
                  (least_general_of_layout key_l, least_general_of_layout value_l)
                  )
            ; body= (fun (kt, vt) (kt', vt') -> (unify_exn kt kt', unify_exn vt vt'))
            ; post= (fun (kt, vt) -> HashIdxT (kt, vt, {count= Type.count vt})) } )

        method ordered_idx _ (_, value_l, {oi_key_layout; _}) =
          let key_l = Option.value_exn oi_key_layout in
          ( {pre= (fun () -> ()); body= (fun () _ -> ()); post= (fun () -> ())}
          , { pre=
                (fun () ->
                  (least_general_of_layout key_l, least_general_of_layout value_l)
                  )
            ; body= (fun (kt, vt) (kt', vt') -> (unify_exn kt kt', unify_exn vt vt'))
            ; post= (fun (kt, vt) -> OrderedIdxT (kt, vt, {count= Top})) } )
      end
  end

  include TF

  let to_type l =
    Logs.info (fun m -> m "Computing type of abstract layout.") ;
    let type_ = (new type_fold)#run () l in
    Logs.info (fun m ->
        m "The type is: %s" (Sexp.to_string_hum ([%sexp_of: Type.t] type_)) ) ;
    type_

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
            (AHashIdx (r, l, m), Set.union key_ctx value_ctx)
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
            (AOrderedIdx (r, l, m), Set.union key_ctx value_ctx)
        | As (n, r) ->
            let r, ctx = resolve outer_ctx r in
            let ctx = rename n ctx in
            (As (n, r), ctx)
        | OrderBy {key; rel} ->
            let rel, inner_ctx = resolve outer_ctx rel in
            let key =
              List.map key ~f:(fun (p, o) -> (resolve_pred inner_ctx p, o))
            in
            (OrderBy {key; rel}, inner_ctx)
      in
      ({node= node'; meta}, ctx')
    in
    let r, _ = resolve params r in
    r

  let rec annotate_type r t =
    let open Type in
    match (r.node, t) with
    | AScalar _, (IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _ | NullT) ->
        Meta.(set_m r type_ t)
    | AList (_, r'), ListT (t', _) ->
        Meta.(set_m r type_ t) ;
        annotate_type r' t'
    | AHashIdx (_, vr, m), HashIdxT (kt, vt, _) ->
        Meta.(set_m r type_ t) ;
        Option.iter m.hi_key_layout ~f:(fun kr -> annotate_type kr kt) ;
        annotate_type vr vt
    | AOrderedIdx (_, vr, m), OrderedIdxT (kt, vt, _) ->
        Meta.(set_m r type_ t) ;
        Option.iter m.oi_key_layout ~f:(fun kr -> annotate_type kr kt) ;
        annotate_type vr vt
    | ATuple (rs, _), TupleT (ts, _) -> (
        Meta.(set_m r type_ t) ;
        match List.iter2 rs ts ~f:annotate_type with
        | Ok () -> ()
        | Unequal_lengths ->
            Error.(
              create "Mismatched tuple type." (r, t) [%sexp_of: Abslayout.t * T.t]
              |> raise) )
    | (Filter (_, r') | Select (_, r')), FuncT ([t'], _) ->
        Meta.(set_m r type_ t) ;
        annotate_type r' t'
    | As (_, r), _ -> annotate_type r t
    | ( ( Select _ | Filter _ | Join _ | GroupBy _ | OrderBy _ | Dedup _ | Scan _
        | AEmpty | AScalar _ | AList _ | ATuple _ | AHashIdx _ | AOrderedIdx _ )
      , ( NullT | IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _ | TupleT _
        | ListT _ | HashIdxT _ | OrderedIdxT _ | FuncT _ | EmptyT ) ) ->
        Error.create "Unexpected type." (r, t) [%sexp_of: Abslayout.t * t]
        |> Error.raise

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
end
