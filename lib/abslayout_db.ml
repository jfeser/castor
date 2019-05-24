open! Core
open Collections
open Abslayout
open Abslayout0

module type S = Abslayout_db_intf.S

module Config = struct
  module type S = sig
    val conn : Db.t

    val simplify : (t -> t) option

    include Project.Config.S
  end
end

module Make (Config : Config.S) = struct
  module P = Project.Make (Config)
  module R = Resolve.Make (Config)
  open Config

  module Query = struct
    [@@@warning "-17"]

    type 'q t =
      | Empty
      | Query of 'q
      | Scalars of (Pred.t[@name "pred"]) list
      | Concat of 'q t list
      | For of 'q * string * 'q t
      | Let of (string * 'q t) list * 'q t
      | Var of string
    [@@deriving
      visitors {variety= "reduce"}
      , visitors {variety= "mapreduce"}
      , visitors {variety= "map"}]

    [@@@warning "+17"]

    (** A query is invariant in a set of scopes if it doesn't refer to any name
       in one of the scopes. *)
    let is_invariant ss q =
      let visitor =
        object (self : 'self)
          inherit [_] reduce

          inherit [_] Util.conj_monoid

          method visit_names () ns =
            Set.for_all ns ~f:(fun n ->
                match Name.rel n with
                | Some s' -> not (List.mem ss s' ~equal:String.( = ))
                | None -> true )

          method visit_'q () q = self#visit_names () (names q)

          method visit_pred () p = self#visit_names () (Pred.names p)

          method! visit_Var () _ = false

          method! visit_Empty () = false
        end
      in
      visitor#visit_t () q

    let hoist_invariant ss q =
      let visitor =
        object (self : 'self)
          inherit [_] mapreduce as super

          inherit [_] Util.list_monoid

          method visit_pred _ x = (x, self#zero)

          method visit_'q _ x = (x, self#zero)

          method! visit_For ss r s q =
            let q', binds = self#visit_t (s :: ss) q in
            (For (r, s, q'), binds)

          method! visit_t ss q =
            if is_invariant ss q then
              let n = Fresh.name Global.fresh "q%d" in
              (Var n, [(n, q)])
            else super#visit_t ss q
        end
      in
      visitor#visit_t ss q

    let hoist_all q =
      let visitor =
        object (self : 'self)
          inherit [_] map

          method visit_pred _ x = x

          method visit_'q _ x = x

          method! visit_For ss r s q =
            let ss = s :: ss in
            let q', binds = hoist_invariant ss q in
            match binds with
            | [] -> For (r, s, self#visit_t ss q)
            | _ -> Let (binds, For (r, s, self#visit_t ss q'))
        end
      in
      visitor#visit_t [] q

    let to_width q =
      let visitor =
        object
          inherit [_] map

          method visit_'q () q = List.length (schema_exn q)

          method visit_pred () x = x
        end
      in
      visitor#visit_t () q
  end

  module Q = Query

  module Stripped_query = struct end

  module Ctx = struct
    type t =
      | Empty
      | Query of Value.t list Gen.t
      | Scalars of Value.t Lazy.t list
      | Concat of t list
      | For of (Value.t list * t) Gen.t
  end

  module C = Ctx

  let to_concat binds q = Q.Concat (List.map binds ~f:Tuple.T2.get2 @ [q])

  let rec width =
    let open Query in
    function
    | Empty | Var _ -> 0
    | For (n, _, q2) -> n + width q2
    | Scalars ps -> List.length ps
    | Concat qs -> 1 + List.sum (module Int) qs ~f:width
    | Let (binds, q) -> width (to_concat binds q)
    | Query n -> n

  let total_order_key q =
    let native_order = Abslayout.order_of q in
    let total_order = List.map (schema_exn q) ~f:(fun n -> (Name n, Asc)) in
    native_order @ total_order

  let to_scalars rs =
    List.map rs ~f:(fun t -> match t.node with AScalar p -> Some p | _ -> None)
    |> Option.all

  let to_ctx t = C.Scalars (List.map t ~f:Lazy.return)

  let rec gen_query q =
    let open Query in
    match q.node with
    | AList (q1, q2) ->
        let scope = scope_exn q1 in
        let q1 = strip_scope q1 in
        let q1 =
          let order_key = total_order_key q1 in
          order_by order_key q1
        in
        For (q1, scope, gen_query q2)
    | AHashIdx h ->
        let q1 =
          let order_key = total_order_key h.hi_keys in
          order_by order_key (dedup h.hi_keys)
        in
        For (q1, h.hi_scope, gen_query h.hi_values)
    | AOrderedIdx (q1, q2, _) ->
        let scope = scope_exn q1 in
        let q1 = strip_scope q1 in
        let q1 =
          let order_key = total_order_key q1 in
          order_by order_key (dedup q1)
        in
        For (q1, scope, gen_query q2)
    | AEmpty -> Empty
    | AScalar p -> Scalars [p]
    | ATuple (ts, _) -> (
      match to_scalars ts with
      | Some ps -> Scalars ps
      | None -> Concat (List.map ~f:gen_query ts) )
    | DepJoin {d_lhs; d_rhs; _} -> Concat [gen_query d_lhs; gen_query d_rhs]
    | Select (_, q) | Filter (_, q) | As (_, q) -> gen_query q
    | Relation _ | Dedup _ | OrderBy _ | GroupBy _ | Join _ ->
        failwith "Unsupported."

  let unwrap_order r =
    match r.node with OrderBy {key; rel} -> (key, rel) | _ -> ([], r)

  let rec to_ralgebra =
    let open Query in
    function
    | Var _ -> scalar (As_pred (Int 0, Fresh.name Global.fresh "var%d"))
    | Let (binds, q) -> to_ralgebra (to_concat binds q)
    | For (q1, scope, q2) ->
        let o1, q1 = unwrap_order q1 in
        let o2, q2 = to_ralgebra q2 |> unwrap_order in
        (* Generate a renaming so that the upward exposed names are fresh. *)
        let sctx =
          (schema_exn q1 |> Schema.scoped scope) @ schema_exn q2
          |> List.map ~f:(fun n ->
                 let n' = Fresh.name Global.fresh "x%d" in
                 (n, n') )
        in
        let slist = List.map sctx ~f:(fun (n, n') -> As_pred (Name n, n')) in
        let order =
          let sctx =
            List.map sctx ~f:(fun (n, n') -> (n, Name (Name.create n')))
            |> Map.of_alist_exn (module Name)
          in
          (* The renaming refers to the scoped names from q1, so scope before
             renaming. *)
          let o1 =
            List.map o1 ~f:(fun (p, o) -> (Pred.scoped (schema_exn q1) scope p, o))
          in
          List.map (o1 @ o2) ~f:(fun (p, o) -> (Pred.subst sctx p, o))
        in
        order_by order (dep_join q1 scope (select slist q2))
    | Concat qs ->
        let counter_name = Fresh.name Global.fresh "counter%d" in
        let orders, qs =
          List.map qs ~f:(fun q -> unwrap_order (to_ralgebra q)) |> List.unzip
        in
        (* The queries in qs can have different schemas, so we need to normalize
         them. This means creating a select list that has the union of the
         names in the queries. *)
        let queries_norm =
          List.mapi qs ~f:(fun i q ->
              let select_list =
                (* Add a counter so we know which query we're on. *)
                As_pred (Int i, counter_name)
                :: List.concat_mapi qs ~f:(fun j q' ->
                       schema_exn q'
                       |>
                       (* Take the names from this query's schema. *)
                       if i = j then List.map ~f:(fun n -> Name n)
                       else
                         (* Otherwise emit null. *)
                         List.map ~f:(fun n ->
                             As_pred (Null (Some (Name.type_exn n)), Name.name n) )
                   )
              in
              select select_list q )
        in
        let order = (Name (Name.create counter_name), Asc) :: List.concat orders in
        order_by order (tuple queries_norm Concat)
    | Empty -> empty
    | Scalars ps -> tuple (List.map ps ~f:scalar) Cross
    | Query q -> q

  let simplify = Option.value simplify ~default:(fun r -> R.resolve r |> P.project)

  let rec eval_concat ctx tups qs =
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
    let widths = List.map qs ~f:width in
    let extract_counter tup = List.hd_exn tup |> Value.to_int in
    let mk_extract_tuple ctr =
      let take_ct = List.nth_exn widths ctr in
      let drop_ct =
        List.sum (module Int) (List.take widths ctr) ~f:(fun x -> x) + 1
      in
      fun tup -> List.take (List.drop tup drop_ct) take_ct
    in
    List.mapi qs ~f:(fun oidx q ->
        let extract_tuple = mk_extract_tuple oidx in
        let strm =
          take_while (fun t -> extract_counter t = oidx) |> Gen.map ~f:extract_tuple
        in
        eval ctx strm q )

  and eval ctx tups = function
    | Q.Let (binds, q) ->
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
        let widths = List.map binds ~f:(fun (_, q) -> width q) @ [width q] in
        let extract_counter tup = List.hd_exn tup |> Value.to_int in
        let mk_extract_tuple ctr =
          let take_ct = List.nth_exn widths ctr in
          let drop_ct =
            List.sum (module Int) (List.take widths ctr) ~f:(fun x -> x) + 1
          in
          fun tup -> List.take (List.drop tup drop_ct) take_ct
        in
        let binds =
          List.mapi binds ~f:(fun oidx (n, q) ->
              let extract_tuple = mk_extract_tuple oidx in
              let strm =
                take_while (fun t -> extract_counter t = oidx)
                |> Gen.map ~f:extract_tuple |> Gen.persistent_lazy
              in
              (n, (q, strm)) )
        in
        let ctx =
          List.fold_left binds ~init:ctx ~f:(fun ctx (n, q) ->
              Map.set ctx ~key:n ~data:q )
        in
        let oidx = List.length binds in
        let extract_tuple = mk_extract_tuple oidx in
        let strm =
          take_while (fun t -> extract_counter t = oidx) |> Gen.map ~f:extract_tuple
        in
        let forced = ref false in
        let strm () =
          if not !forced then (
            forced := true ;
            List.iter binds ~f:(fun (_, (_, s)) -> s () |> Gen.iter ~f:(fun _ -> ())) ) ;
          strm ()
        in
        eval ctx strm q
    | Q.Var n ->
        Gen.junk tups ;
        let q, strm = Map.find_exn ctx n in
        eval ctx (strm ()) q
    | Q.For (n, _, q2) ->
        let extract_tup1, drop_tup1 =
          ((fun t -> List.take t n), fun t -> List.drop t n)
        in
        let eq t1 t2 =
          [%compare.equal: Value.t list] (extract_tup1 t1) (extract_tup1 t2)
        in
        let gen =
          Gen.group_eager eq tups
          |> Gen.filter_map ~f:(fun ts ->
                 let ts = List.rev ts in
                 Option.map (List.hd ts) ~f:(fun t ->
                     let x = extract_tup1 t in
                     let y =
                       eval ctx (ts |> Gen.of_list |> Gen.map ~f:drop_tup1) q2
                     in
                     (x, y) ) )
        in
        C.For gen
    | Q.Concat qs -> C.Concat (eval_concat ctx tups qs)
    | Q.Empty ->
        if Gen.is_empty tups then C.Empty
        else failwith "Expected an empty generator."
    | Q.Scalars ps ->
        let tup =
          lazy
            ( match Gen.next tups with
            | Some xs when List.length xs = List.length ps -> xs
            | Some t ->
                Error.(
                  create "Scalar: unexpected tuple width." (ps, t)
                    [%sexp_of: pred list * Value.t list]
                  |> raise)
            | None -> failwith "Expected a tuple." )
        in
        C.Scalars
          (List.init (List.length ps) ~f:(fun i ->
               Lazy.map tup ~f:(fun t -> List.nth_exn t i) ))
    | Q.Query _ -> C.Query tups

  let eval_query q =
    let q = Q.hoist_all q in
    Log.debug (fun m -> m "Executing type checking query %a." pp (to_ralgebra q)) ;
    let r = to_ralgebra q |> simplify in
    Log.debug (fun m -> m "Executing type checking query %a." pp r) ;
    let sql = Sql.of_ralgebra r in
    Log.debug (fun m ->
        m "Executing type checking query %s." (Sql.to_string_hum sql) ) ;
    let tups =
      Db.exec_cursor_exn conn
        (schema_exn r |> List.map ~f:Name.type_exn)
        (Sql.to_string_hum sql)
      |> Gen.map ~f:Array.to_list
    in
    eval (Map.empty (module String)) tups (Q.to_width q)

  class virtual ['ctx, 'a] unsafe_material_fold =
    object (self)
      method virtual private build_AList
          : 'ctx -> Meta.t -> t * t -> (Value.t list * Ctx.t) Gen.t -> 'a

      method virtual private build_ATuple : _

      method virtual private build_AHashIdx
          : 'ctx -> Meta.t -> hash_idx -> (Value.t list * Ctx.t) Gen.t -> 'a

      method virtual private build_AOrderedIdx
          :    'ctx
            -> Meta.t
            -> t * t * ordered_idx
            -> (Value.t list * Ctx.t) Gen.t
            -> 'a

      method virtual private build_AEmpty : 'ctx -> Meta.t -> 'a

      method virtual private build_AScalar
          : 'ctx -> Meta.t -> pred -> Value.t Lazy.t -> 'a

      method virtual private build_Select
          : 'ctx -> Meta.t -> pred list * t -> Ctx.t -> 'a

      method virtual private build_Filter
          : 'ctx -> Meta.t -> pred * t -> Ctx.t -> 'a

      method virtual private build_DepJoin : _

      method private visit_t ctx eval_ctx r =
        let open Ctx in
        match (r.node, eval_ctx) with
        | AEmpty, Empty -> self#build_AEmpty ctx r.meta
        | AScalar x, Scalars [v] -> self#build_AScalar ctx r.meta x v
        | AList x, For vs -> self#build_AList ctx r.meta x vs
        | AHashIdx x, For value_gen -> self#build_AHashIdx ctx r.meta x value_gen
        | AOrderedIdx x, For value_gen ->
            self#build_AOrderedIdx ctx r.meta x value_gen
        | ATuple x, Scalars ps ->
            self#build_ATuple ctx r.meta x (List.map ps ~f:(fun p -> Scalars [p]))
        | ATuple x, Concat vs -> self#build_ATuple ctx r.meta x vs
        | DepJoin x, Concat [lhs; rhs] -> self#build_DepJoin ctx r.meta x (lhs, rhs)
        | Select x, _ -> self#build_Select ctx r.meta x eval_ctx
        | Filter x, _ -> self#build_Filter ctx r.meta x eval_ctx
        | As (_, r), _ -> self#visit_t ctx eval_ctx r
        | ( ( DepJoin _ | AEmpty | AScalar _ | AList _ | AHashIdx _ | AOrderedIdx _
            | ATuple _ )
          , _ ) ->
            Error.create "Bug: Mismatched context." r [%sexp_of: t] |> Error.raise
        | (Join _ | Dedup _ | OrderBy _ | Relation _ | GroupBy _), _ ->
            Error.create "Cannot materialize." r [%sexp_of: t] |> Error.raise

      method run ctx r =
        self#visit_t ctx (eval_query (gen_query (ensure_alias r))) r
    end

  type ('i, 'a, 'v, 'o) fold = {pre: 'i -> 'a; body: 'a -> 'v -> 'a; post: 'a -> 'o}

  class virtual ['out, 'l, 'h, 'o] material_fold =
    object (self)
      inherit [unit, 'out] unsafe_material_fold

      method virtual list
          : Meta.t -> t * t -> (unit, 'l, Value.t list * 'out, 'out) fold

      method virtual hash_idx
          : Meta.t -> hash_idx -> (unit, 'h, 'out * 'out, 'out) fold

      method virtual ordered_idx
          : Meta.t -> t * t * ordered_idx -> (unit, 'o, 'out * 'out, 'out) fold

      method virtual tuple : Meta.t -> t list * tuple -> 'out list -> 'out

      method virtual empty : Meta.t -> 'out

      method virtual scalar : Meta.t -> pred -> Value.t -> 'out

      method virtual select : Meta.t -> pred list * t -> 'out -> 'out

      method virtual filter : Meta.t -> pred * t -> 'out -> 'out

      method virtual depjoin : Meta.t -> depjoin -> 'out -> 'out -> 'out

      method private build_AList ctx meta ((_, r) as expr) gen =
        let {pre; body; post} = self#list meta expr in
        Gen.fold gen ~init:(pre ()) ~f:(fun acc (tup, ectx) ->
            let value = self#visit_t ctx ectx r in
            body acc (tup, value) )
        |> post

      method private build_ATuple ctx meta ((rs, _) as expr) es =
        self#tuple meta expr (List.map2_exn es rs ~f:(self#visit_t ctx))

      method private build_AHashIdx ctx meta h vgen =
        let key_query = h_key_layout h in
        let f2 = self#hash_idx meta h in
        Gen.fold vgen ~init:(f2.pre ()) ~f:(fun acc (tup, ectx) ->
            f2.body acc
              ( self#visit_t ctx (to_ctx tup) key_query
              , self#visit_t ctx ectx h.hi_values ) )
        |> f2.post

      method private build_AOrderedIdx ctx meta ((_, value_query, m) as o) vgen =
        let key_query = o_key_layout m in
        let f2 = self#ordered_idx meta o in
        Gen.fold vgen ~init:(f2.pre ()) ~f:(fun acc (tup, ectx) ->
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

      method private build_DepJoin ctx meta expr (lhs, rhs) =
        let l = self#visit_t ctx lhs expr.d_lhs in
        let r = self#visit_t ctx rhs expr.d_rhs in
        self#depjoin meta expr l r
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
      | AScalar p -> Abslayout.Pred.to_type p |> least_general_of_primtype
      | AList (_, r') -> ListT (least_general_of_layout r', {count= Bottom})
      | DepJoin {d_lhs; d_rhs; _} ->
          FuncT
            ( [least_general_of_layout d_lhs; least_general_of_layout d_rhs]
            , `Child_sum )
      | AHashIdx h ->
          HashIdxT
            ( least_general_of_layout (h_key_layout h)
            , least_general_of_layout h.hi_values
            , {value_count= Bottom; key_count= Bottom} )
      | AOrderedIdx (_, vr, {oi_key_layout= Some kr; _}) ->
          OrderedIdxT
            (least_general_of_layout kr, least_general_of_layout vr, {count= Bottom})
      | ATuple (rs, _) ->
          TupleT (List.map rs ~f:least_general_of_layout, {count= Bottom})
      | As (_, r') -> least_general_of_layout r'
      | AOrderedIdx (_, _, {oi_key_layout= None; _}) | Relation _ ->
          failwith "Layout is still abstract."

    (** Returns a layout type that is general enough to hold all of the data. *)
    class type_fold =
      object
        inherit [_, _, _, _] material_fold

        method select _ (exprs, _) t = FuncT ([t], `Width (List.length exprs))

        method filter _ _ t = FuncT ([t], `Child_sum)

        method depjoin _ _ t1 t2 = FuncT ([t1; t2], `Child_sum)

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

        method hash_idx _ h =
          { pre=
              (fun () ->
                ( 0
                , least_general_of_layout (h_key_layout h)
                , least_general_of_layout h.hi_values ) )
          ; body=
              (fun (kct, kt, vt) (kt', vt') ->
                (kct + 1, unify_exn kt kt', unify_exn vt vt') )
          ; post=
              (fun (kct, kt, vt) ->
                HashIdxT
                  ( kt
                  , vt
                  , {key_count= AbsInt.of_int kct; value_count= Type.count vt} ) )
          }

        method ordered_idx _ (_, value_l, {oi_key_layout; _}) =
          let key_l = Option.value_exn oi_key_layout in
          { pre=
              (fun () ->
                ( least_general_of_layout key_l
                , least_general_of_layout value_l
                , AbsInt.zero ) )
          ; body=
              (fun (kt, vt, ct) (kt', vt') ->
                (unify_exn kt kt', unify_exn vt vt', AbsInt.(ct + of_int 1)) )
          ; post= (fun (kt, vt, ct) -> OrderedIdxT (kt, vt, {count= ct})) }
      end
  end

  include TF

  let type_of r =
    Log.info (fun m -> m "Computing type of abstract layout.") ;
    let type_ = (new type_fold)#run () r in
    Log.info (fun m ->
        m "The type is: %s" (Sexp.to_string_hum ([%sexp_of: Type.t] type_)) ) ;
    type_

  let annotate_type r =
    let rec annot r t =
      let open Type in
      Meta.(set_m r type_ t) ;
      match (r.node, t) with
      | AScalar _, (IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _ | NullT) -> ()
      | AList (_, r'), ListT (t', _)
       |(Filter (_, r') | Select (_, r')), FuncT ([t'], _) ->
          annot r' t'
      | AHashIdx h, HashIdxT (kt, vt, _) ->
          Option.iter h.hi_key_layout ~f:(fun kr -> annot kr kt) ;
          annot h.hi_values vt
      | AOrderedIdx (_, vr, m), OrderedIdxT (kt, vt, _) ->
          Option.iter m.oi_key_layout ~f:(fun kr -> annot kr kt) ;
          annot vr vt
      | ATuple (rs, _), TupleT (ts, _) -> (
        match List.iter2 rs ts ~f:annot with
        | Ok () -> ()
        | Unequal_lengths ->
            Error.(
              create "Mismatched tuple type." (r, t) [%sexp_of: Abslayout.t * T.t]
              |> raise) )
      | DepJoin {d_lhs; d_rhs; _}, FuncT ([t1; t2], _) ->
          annot d_lhs t1 ; annot d_rhs t2
      | As (_, r), _ -> annot r t
      | ( ( Select _ | Filter _ | DepJoin _ | Join _ | GroupBy _ | OrderBy _
          | Dedup _ | Relation _ | AEmpty | AScalar _ | AList _ | ATuple _
          | AHashIdx _ | AOrderedIdx _ )
        , ( NullT | IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _ | TupleT _
          | ListT _ | HashIdxT _ | OrderedIdxT _ | FuncT _ | EmptyT ) ) ->
          Error.create "Unexpected type." (r, t) [%sexp_of: Abslayout.t * t]
          |> Error.raise
    in
    annot r (type_of r) ;
    let visitor =
      object
        inherit runtime_subquery_visitor

        method visit_Subquery r = annot r (type_of r)
      end
    in
    visitor#visit_t () r

  let annotate_relations =
    let visitor =
      object
        inherit [_] endo

        method! visit_Relation () _ r = Relation (Db.relation conn r.r_name)
      end
    in
    visitor#visit_t ()

  let load_layout ?(params = Set.empty (module Name)) l =
    l |> strip_unused_as |> annotate_relations |> R.resolve ~params
    |> annotate_key_layouts

  let load_string ?params s = of_string_exn s |> load_layout ?params
end

let%test_module _ =
  ( module struct
    module Config = struct
      let conn = Lazy.force Test_util.test_db_conn

      let simplify = None
    end

    include Make (Config)

    let%expect_test "" =
      let ralgebra =
        "alist(r1 as k, filter(k.f = g, ascalar(k.g)))" |> load_string
      in
      let q = gen_query ralgebra in
      let r = to_ralgebra q in
      let sql = Sql.of_ralgebra r in
      printf "%s" (Sql.to_string_hum sql) ;
      [%expect
        {|
      SELECT
          "x0_0" AS "x0_0_0",
          "x1_0" AS "x1_0_0",
          "x2_0" AS "x2_0_0"
      FROM (
          SELECT
              r1_0. "f" AS "r1_0_f_0",
              r1_0. "g" AS "r1_0_g_0"
          FROM
              "r1" AS "r1_0") AS "t1",
          LATERAL (
              SELECT
                  "r1_0_f_0" AS "x0_0",
                  "r1_0_g_0" AS "x1_0",
                  "r1_0_g_0" AS "x2_0") AS "t0"
      ORDER BY
          "x0_0",
          "x1_0" |}]

    let%expect_test "" =
      let ralgebra =
        "depjoin(ascalar(0 as f) as k, select([k.f + g], alist(r1 as k1, \
         ascalar(k1.g))))" |> load_string
      in
      let q = gen_query ralgebra in
      let r = to_ralgebra q in
      let sql = Sql.of_ralgebra r in
      printf "%s" (Sql.to_string_hum sql) ;
      [%expect
        {|
      SELECT
          "counter0_0" AS "counter0_0_0",
          "f_1" AS "f_1_0",
          "x3_0" AS "x3_0_0",
          "x4_0" AS "x4_0_0",
          "x5_0" AS "x5_0_0"
      FROM ((
              SELECT
                  0 AS "counter0_0",
                  0 AS "f_1",
                  (null::integer) AS "x3_0",
                  (null::integer) AS "x4_0",
                  (null::integer) AS "x5_0")
          UNION ALL (
              SELECT
                  1 AS "counter0_1",
                  (null::integer) AS "f_2",
                  "x3_1" AS "x3_2",
                  "x4_1" AS "x4_2",
                  "x5_1" AS "x5_2"
              FROM (
                  SELECT
                      r1_1. "f" AS "r1_1_f_0",
                      r1_1. "g" AS "r1_1_g_0"
                  FROM
                      "r1" AS "r1_1") AS "t3",
                  LATERAL (
                      SELECT
                          "r1_1_f_0" AS "x3_1",
                          "r1_1_g_0" AS "x4_1",
                          "r1_1_g_0" AS "x5_1") AS "t2")) AS "t4"
      ORDER BY
          "counter0_0",
          "x3_0",
          "x4_0" |}]

    let%expect_test "" =
      let ralgebra = load_string Test_util.sum_complex in
      let q = gen_query ralgebra in
      let r = to_ralgebra q in
      Format.printf "%a" pp r ;
      [%expect
        {|
      orderby([x6, x7],
        depjoin(r1 as k,
          select([k.f as x6, k.g as x7, f as x8, v as x9],
            atuple([ascalar(k.f), ascalar((k.g - k.f) as v)], cross)))) |}] ;
      let sql = Sql.of_ralgebra r in
      printf "%s" (Sql.to_string_hum sql) ;
      [%expect
        {|
      SELECT
          "x6_0" AS "x6_0_0",
          "x7_0" AS "x7_0_0",
          "x8_0" AS "x8_0_0",
          "x9_0" AS "x9_0_0"
      FROM (
          SELECT
              r1_2. "f" AS "r1_2_f_0",
              r1_2. "g" AS "r1_2_g_0"
          FROM
              "r1" AS "r1_2") AS "t6",
          LATERAL (
              SELECT
                  "r1_2_f_0" AS "x6_0",
                  "r1_2_g_0" AS "x7_0",
                  "r1_2_f_0" AS "x8_0",
                  ("r1_2_g_0") - ("r1_2_f_0") AS "x9_0"
              WHERE (TRUE)) AS "t5"
      ORDER BY
          "x6_0",
          "x7_0" |}]

    let%expect_test "" =
      let ralgebra = load_string "alist(r1 as k, alist(r1 as j, ascalar(j.f)))" in
      let q = gen_query ralgebra in
      let r = to_ralgebra q in
      Format.printf "%a" pp r ;
      [%expect
        {|
        orderby([x13, x14, x15, x16],
          depjoin(r1 as k,
            select([k.f as x13, k.g as x14, x10 as x15, x11 as x16, x12 as x17],
              depjoin(r1 as j,
                select([j.f as x10, j.g as x11, f as x12],
                  atuple([ascalar(j.f)], cross)))))) |}] ;
      let sql = Sql.of_ralgebra r in
      printf "%s" (Sql.to_string_hum sql) ;
      [%expect
        {|
        SELECT
            "x13_0" AS "x13_0_0",
            "x14_0" AS "x14_0_0",
            "x15_0" AS "x15_0_0",
            "x16_0" AS "x16_0_0",
            "x17_0" AS "x17_0_0"
        FROM (
            SELECT
                r1_3. "f" AS "r1_3_f_0",
                r1_3. "g" AS "r1_3_g_0"
            FROM
                "r1" AS "r1_3") AS "t10",
            LATERAL (
                SELECT
                    "r1_3_f_0" AS "x13_0",
                    "r1_3_g_0" AS "x14_0",
                    "x10_0" AS "x15_0",
                    "x11_0" AS "x16_0",
                    "x12_0" AS "x17_0"
                FROM (
                    SELECT
                        r1_4. "f" AS "r1_4_f_0",
                        r1_4. "g" AS "r1_4_g_0"
                    FROM
                        "r1" AS "r1_4") AS "t8",
                    LATERAL (
                        SELECT
                            "r1_4_f_0" AS "x10_0",
                            "r1_4_g_0" AS "x11_0",
                            "r1_4_f_0" AS "x12_0") AS "t7") AS "t9"
        ORDER BY
            "x13_0",
            "x14_0",
            "x15_0",
            "x16_0" |}]
  end )
