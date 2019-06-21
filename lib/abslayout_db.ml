open! Core
open! Lwt
open Collections
open Abslayout0
module A = Abslayout
module T = Type

module type S = Abslayout_db_intf.S

module Config = struct
  module type S = sig
    val conn : Db.t

    val simplify : (A.t -> A.t) option
  end
end

module Query = struct
  [@@@warning "-17"]

  type ('q, 'm) node =
    | Empty
    | Scalars of (Pred.t[@name "pred"]) list
    | Concat of ('q, 'm) t list
    | For of ('q * string * ('q, 'm) t * bool)
    | Let of ((string * ('q, 'm) t) list * ('q, 'm) t)
    | Var of string

  and ('q, 'm) t = {node: ('q, 'm) node; meta: 'm}
  [@@deriving
    visitors {variety= "reduce"}
    , visitors {variety= "mapreduce"}
    , visitors {variety= "map"}
    , sexp_of]

  [@@@warning "+17"]

  let empty c = {node= Empty; meta= c}

  let scalars c p = {node= Scalars p; meta= c}

  let concat c x = {node= Concat x; meta= c}

  let for_ c x = {node= For x; meta= c}

  let let_ c x = {node= Let x; meta= c}

  let var c x = {node= Var x; meta= c}

  let map_meta ~f q =
    let visitor =
      object
        inherit [_] map

        method visit_'m () = f

        method visit_'q () x = x

        method visit_pred () x = x
      end
    in
    visitor#visit_t () q

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

        method visit_'q () q = self#visit_names () (A.names q)

        method visit_'m () _ = self#zero

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

        method visit_'m _ x = (x, self#zero)

        method visit_pred _ x = (x, self#zero)

        method visit_'q _ x = (x, self#zero)

        method! visit_For ss (r, s, q, x) =
          let q', binds = self#visit_t (s :: ss) q in
          (For (r, s, q', x), binds)

        method! visit_t ss q =
          if is_invariant ss q then
            let n = Fresh.name Global.fresh "q%d" in
            (var A.empty n, [(n, q)])
          else super#visit_t ss q
      end
    in
    visitor#visit_t ss q

  let hoist_all q =
    let visitor =
      object (self : 'self)
        inherit [_] map as super

        method visit_pred _ x = x

        method visit_'q _ x = x

        method visit_'m _ x = x

        method! visit_t ss q =
          match q.node with
          | For (r, s, q', x) -> (
              let ss = s :: ss in
              let q', binds = hoist_invariant ss q' in
              match binds with
              | [] -> for_ q.meta (r, s, self#visit_t ss q', x)
              | _ -> let_ A.empty (binds, for_ q.meta (r, s, self#visit_t ss q', x))
              )
          | _ -> super#visit_t ss q
      end
    in
    visitor#visit_t [] q

  let to_width q =
    let visitor =
      object
        inherit [_] map

        method visit_'q () q = List.length (A.schema_exn q)

        method visit_'m () x = x

        method visit_pred () x = x
      end
    in
    visitor#visit_t () q

  let total_order_key q =
    let native_order = Abslayout.order_of q in
    let total_order = List.map (A.schema_exn q) ~f:(fun n -> (Name n, Asc)) in
    native_order @ total_order

  let to_scalars rs =
    List.map rs ~f:(fun t -> match t.A.node with A.AScalar p -> Some p | _ -> None)
    |> Option.all

  let rec of_ralgebra q =
    match q.A.node with
    | AList (q1, q2) ->
        let scope = A.scope_exn q1 in
        let q1 = A.strip_scope q1 in
        let q1 =
          let order_key = total_order_key q1 in
          A.order_by order_key q1
        in
        for_ q (q1, scope, of_ralgebra q2, false)
    | AHashIdx h ->
        let q1 =
          let order_key = total_order_key h.hi_keys in
          A.order_by order_key (A.dedup h.hi_keys)
        in
        for_ q (q1, h.hi_scope, of_ralgebra h.hi_values, true)
    | AOrderedIdx (q1, q2, _) ->
        let scope = A.scope_exn q1 in
        let q1 = A.strip_scope q1 in
        let q1 =
          let order_key = total_order_key q1 in
          A.order_by order_key (A.dedup q1)
        in
        for_ q (q1, scope, of_ralgebra q2, true)
    | AEmpty -> empty q
    | AScalar p -> scalars q [p]
    | ATuple (ts, _) -> (
      match to_scalars ts with
      | Some ps -> scalars q ps
      | None -> concat q (List.map ~f:of_ralgebra ts) )
    | DepJoin {d_lhs= q1; d_rhs= q2; _} | Join {r1= q1; r2= q2; _} ->
        concat q [of_ralgebra q1; of_ralgebra q2]
    | Select (_, q')
     |Filter (_, q')
     |Dedup q'
     |OrderBy {rel= q'; _}
     |GroupBy (_, _, q') ->
        {(of_ralgebra q') with meta= q}
    | Relation _ -> failwith "Bare relation."
    | As _ -> failwith "Unexpected as."

  let to_concat binds q = concat None (List.map binds ~f:Tuple.T2.get2 @ [q])

  let unwrap_order r =
    match r.A.node with A.OrderBy {key; rel} -> (key, rel) | _ -> ([], r)

  let wrap q = map_meta ~f:Option.some q

  let unwrap q = map_meta ~f:(fun m -> Option.value_exn m) q

  let rec to_ralgebra' q =
    match q.node with
    | Var _ -> A.scalar (As_pred (Int 0, Fresh.name Global.fresh "var%d"))
    | Let (binds, q) -> to_ralgebra' (to_concat binds q)
    | For (q1, scope, q2, distinct) ->
        (* Extend the lhs query with a row number. Even if this query emits
         duplicate results, the row number will ensure that the duplicates
         remain distinct. *)
        let o1, q1 =
          let o1, q1 = unwrap_order q1 in
          let row_number = Fresh.name Global.fresh "rn%d" in
          let q1 =
            if distinct then q1
            else
              A.select
                ( As_pred (Row_number, row_number)
                :: (A.schema_exn q1 |> Schema.to_select_list) )
                q1
          in
          (o1, q1)
        in
        let o2, q2 = to_ralgebra' q2 |> unwrap_order in
        (* Generate a renaming so that the upward exposed names are fresh. *)
        let sctx =
          (A.schema_exn q1 |> Schema.scoped scope) @ A.schema_exn q2
          |> List.map ~f:(fun n ->
                 let n' = Fresh.name Global.fresh "x%d" in
                 (n, n') )
        in
        let slist = List.map sctx ~f:(fun (n, n') -> As_pred (Name n, n')) in
        (* Stick together the orders from the lhs and rhs queries. *)
        let order =
          let sctx =
            List.map sctx ~f:(fun (n, n') -> (n, Name (Name.create n')))
            |> Map.of_alist_exn (module Name)
          in
          (* The renaming refers to the scoped names from q1, so scope before
             renaming. *)
          let o1 =
            List.map o1 ~f:(fun (p, o) -> (Pred.scoped (A.schema_exn q1) scope p, o))
          in
          List.map (o1 @ o2) ~f:(fun (p, o) -> (Pred.subst sctx p, o))
        in
        A.order_by order (A.dep_join q1 scope (A.select slist q2))
    | Concat qs ->
        let counter_name = Fresh.name Global.fresh "counter%d" in
        let orders, qs =
          List.map qs ~f:(fun q -> unwrap_order (to_ralgebra' q)) |> List.unzip
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
                       A.schema_exn q'
                       |>
                       (* Take the names from this query's schema. *)
                       if i = j then List.map ~f:(fun n -> Name n)
                       else
                         (* Otherwise emit null. *)
                         List.map ~f:(fun n ->
                             As_pred (Null (Some (Name.type_exn n)), Name.name n) )
                   )
              in
              A.select select_list q )
        in
        let order = (Name (Name.create counter_name), Asc) :: List.concat orders in
        A.order_by order (A.tuple queries_norm A.Concat)
    | Empty -> A.empty
    | Scalars ps -> A.tuple (List.map ps ~f:A.scalar) Cross

  let to_ralgebra q = wrap q |> to_ralgebra'

  let rec width' q =
    match q.node with
    | Empty -> 0
    | Var _ -> 1
    | For (n, _, q2, distinct) -> (if distinct then 0 else 1) + n + width' q2
    | Scalars ps -> List.length ps
    | Concat qs -> 1 + List.sum (module Int) qs ~f:width'
    | Let (binds, q) -> width' (to_concat binds q)

  let width q = wrap q |> width'
end

module Make (Config : Config.S) = struct
  module R = Resolve
  module P = Project
  open Config
  module Q = Query

  let simplify = Option.value simplify ~default:(fun r -> R.resolve r |> P.project)

  module Fold = struct
    type ('a, 'b, 'c) fold = {init: 'b; fold: 'b -> 'a -> 'b; extract: 'b -> 'c}

    type ('a, 'c) t = Fold : ('a, 'b, 'c) fold -> ('a, 'c) t

    let run (Fold {init; fold; extract}) l =
      List.fold_left l ~init ~f:fold |> extract

    let run_gen (Fold {init; fold; extract}) l = Gen.fold l ~init ~f:fold |> extract

    let run_lwt (Fold {init; fold; extract}) l =
      let%lwt ret = Lwt_stream.fold (fun x y -> fold y x) l init in
      return (extract ret)
  end

  open Fold

  let two_arg_fold f =
    let init = RevList.empty in
    let fold acc x =
      if RevList.length acc < 2 then RevList.(acc ++ x)
      else failwith "Unexpected concat size."
    in
    let extract acc =
      match RevList.to_list acc with [lhs; rhs] -> f lhs rhs | _ -> assert false
    in
    Fold {init; fold; extract}

  let group_by eq strm =
    let open RevList in
    let cur = ref (`Group empty) in
    let rec next () =
      match !cur with
      | `Done -> return None
      | `Group g -> (
          let%lwt tup = Lwt_stream.get strm in
          match tup with
          | None ->
              cur := `Done ;
              (* Never return empty groups. *)
              if is_empty g then return None else return (Some (to_list g))
          | Some x -> (
            match last g with
            | None ->
                cur := `Group (singleton x) ;
                next ()
            | Some y ->
                if eq x y then (
                  cur := `Group (g ++ x) ;
                  next () )
                else (
                  cur := `Group (singleton x) ;
                  (* NOTE: Groups are created back to front, so must be
                     reversed. *)
                  return (Some (to_list g)) ) ) )
    in
    Lwt_stream.from next

  let extract_group widths ctr tups =
    let extract_counter tup = List.hd_exn tup |> Value.to_int in
    let extract_tuple =
      let take_ct = List.nth_exn widths ctr in
      let drop_ct =
        List.sum (module Int) (List.take widths ctr) ~f:(fun x -> x) + 1
      in
      fun tup -> List.take (List.drop tup drop_ct) take_ct
    in
    let%lwt group = Lwt_stream.get_while (fun t -> extract_counter t = ctr) tups in
    List.map group ~f:extract_tuple |> return

  class virtual ['a] abslayout_fold =
    object (self)
      method virtual list : _

      method virtual hash_idx : _

      method virtual ordered_idx : _

      method virtual tuple : _

      method virtual empty : _

      method virtual scalar : _

      method select _ _ x = x

      method filter _ _ x = x

      method virtual depjoin : _

      method order_by _ _ x = x

      method group_by _ _ x = x

      method dedup _ x = x

      method virtual join : _

      method private debug = false

      method private func a =
        let m = a.Q.meta.A.meta in
        match a.Q.meta.A.node with
        | Filter ((_, c) as x) -> (self#filter m x, c)
        | Select ((_, c) as x) -> (self#select m x, c)
        | OrderBy ({rel= c; _} as x) -> (self#order_by m x, c)
        | GroupBy ((_, _, c) as x) -> (self#group_by m x, c)
        | Dedup c -> (self#dedup m, c)
        | x -> Error.(create "Expected a function." x [%sexp_of: A.node] |> raise)

      method private qempty : (int, A.t) Q.t -> 'a =
        fun a ->
          let m = a.Q.meta.A.meta in
          match a.Q.meta.A.node with
          | AEmpty -> self#empty m
          | _ ->
              let f, child = self#func a in
              f (self#qempty {a with meta= child})

      method private scalars : (int, A.t) Q.t -> 't -> 'a =
        fun a ->
          let m = a.Q.meta.A.meta in
          match a.Q.meta.A.node with
          | AScalar x -> (
              fun t ->
                match t with
                | [v] -> self#scalar m x v
                | _ -> failwith "Expected a singleton tuple." )
          | ATuple ((xs, _) as x) ->
              fun t ->
                let (Fold {init; fold; extract}) = self#tuple m x in
                List.fold2_exn xs t ~init ~f:(fun acc r v ->
                    let m = r.meta in
                    match r.node with
                    | AScalar p -> fold acc (self#scalar m p v)
                    | _ -> failwith "Expected a scalar tuple." )
                |> extract
          | _ ->
              let f, child = self#func a in
              let g = self#scalars {a with meta= child} in
              fun t -> f (g t)

      method private for_
          : (int, A.t) Q.t -> (Value.t list * 'a option * 'a, 'a) Fold.t =
        fun a ->
          let m = a.Q.meta.A.meta in
          match a.Q.meta.A.node with
          | AList x ->
              let (Fold g) = self#list m x in
              Fold {g with fold= (fun a (x, _, z) -> g.fold a (x, z))}
          | AHashIdx x ->
              let (Fold g) = self#hash_idx m x in
              Fold
                { g with
                  fold= (fun a (x, y, z) -> g.fold a (x, Option.value_exn y, z)) }
          | AOrderedIdx x ->
              let (Fold g) = self#ordered_idx m x in
              Fold
                { g with
                  fold= (fun a (x, y, z) -> g.fold a (x, Option.value_exn y, z)) }
          | _ ->
              let f, child = self#func a in
              let (Fold g) = self#for_ {a with meta= child} in
              Fold {g with extract= (fun x -> f (g.extract x))}

      method private concat : (int, A.t) Q.t -> ('a, 'a) Fold.t =
        fun a ->
          let m = a.Q.meta.A.meta in
          match a.Q.meta.A.node with
          | ATuple x -> self#tuple m x
          | DepJoin x -> two_arg_fold (self#depjoin m x)
          | Join x -> two_arg_fold (self#join m x)
          | _ ->
              let f, child = self#func a in
              let (Fold g) = self#concat {a with meta= child} in
              Fold {g with extract= (fun acc -> f (g.extract acc))}

      method private key_layout q =
        match q.node with
        | AHashIdx x -> Some (A.h_key_layout x)
        | AOrderedIdx (_, _, x) -> Some (A.o_key_layout x)
        | Select (_, q')
         |Filter (_, q')
         |Dedup q'
         |OrderBy {rel= q'; _}
         |GroupBy (_, _, q') ->
            self#key_layout q'
        | _ -> None

      method private eval_for lctx tups a (n, _, q2, distinct) : 'a Lwt.t =
        let fold = self#for_ a in
        let extract_lhs t = List.take t n in
        let extract_rhs t = List.drop t n in
        let tups =
          if self#debug then
            Lwt_stream.map
              (fun t ->
                print_s ([%sexp_of: string * Value.t list] ("for", t)) ;
                t )
              tups
          else tups
        in
        let groups =
          if distinct then
            tups
            (* Split each tuple into lhs and rhs. *)
            |> Lwt_stream.map (fun t -> (extract_lhs t, extract_rhs t))
            (* Group by lhs *)
            |> group_by (fun (t1, _) (t2, _) -> [%compare.equal: Value.t list] t1 t2)
            (* Split each group into one lhs and many rhs. *)
            |> Lwt_stream.map (fun g ->
                   let lhs = List.hd_exn g |> Tuple.T2.get1 in
                   let rhs = List.map g ~f:Tuple.T2.get2 in
                   (lhs, rhs) )
          else
            tups
            (* Extract the row number from each tuple. *)
            |> Lwt_stream.map (function
                 | [] -> Error.of_string "Unexpected empty tuple." |> Error.raise
                 | rn :: t -> (rn, t) )
            (* Group by row numbers *)
            |> group_by (fun (rn1, _) (rn2, _) -> [%compare.equal: Value.t] rn1 rn2)
            (* Drop the row number from each group. *)
            |> Lwt_stream.map (fun g -> List.map g ~f:(fun (_, t) -> t))
            (* Split each group into one lhs and many rhs. *)
            |> Lwt_stream.map (fun g ->
                   let lhs = List.hd_exn g |> extract_lhs in
                   let rhs = List.map g ~f:extract_rhs in
                   (lhs, rhs) )
        in
        (* Process each group. *)
        groups
        |> Lwt_stream.map_s (fun (lhs, rhs) ->
               let lval =
                 Option.map (self#key_layout a.Q.meta) ~f:(fun l ->
                     self#scalars {a with meta= l} lhs )
               in
               let%lwt rval = self#eval lctx (Lwt_stream.of_list rhs) q2 in
               return (lhs, lval, rval) )
        |> run_lwt fold

      method private eval_concat lctx tups a qs : 'a Lwt.t =
        let (Fold {init; fold; extract}) = self#concat a in
        let tups =
          if self#debug then
            Lwt_stream.map
              (fun t ->
                print_s ([%sexp_of: string * Value.t list] ("concat", t)) ;
                t )
              tups
          else tups
        in
        let widths = List.map qs ~f:Q.width in
        let%lwt acc =
          List.foldi ~init:(return init) qs ~f:(fun oidx acc q ->
              let%lwt acc = acc in
              let%lwt group = extract_group widths oidx tups in
              let%lwt v = self#eval lctx (Lwt_stream.of_list group) q in
              return (fold acc v) )
        in
        return (extract acc)

      method private eval_scalars _ tups a ps : 'a Lwt.t =
        let tups =
          if self#debug then
            Lwt_stream.map
              (fun t ->
                print_s ([%sexp_of: string * Value.t list] ("scalar", t)) ;
                t )
              tups
          else tups
        in
        let%lwt tup = Lwt_stream.get tups in
        let values =
          match tup with
          | Some xs when List.length xs = List.length ps -> xs
          | Some t ->
              Error.(
                create "Scalar: unexpected tuple width." (ps, t)
                  [%sexp_of: pred list * Value.t list]
                |> raise)
          | None -> failwith "Expected a tuple."
        in
        return (self#scalars a values)

      method private eval_empty _ tups a : 'a Lwt.t =
        let%lwt is_empty = Lwt_stream.is_empty tups in
        if is_empty then return (self#qempty a)
        else failwith "Empty: expected an empty generator."

      method private eval_let lctx tups (binds, q) : 'a Lwt.t =
        let widths = List.map binds ~f:(fun (_, q) -> Q.width q) @ [Q.width q] in
        (* The first n groups contain the values for the bound layouts. *)
        let%lwt binds =
          Lwt_list.mapi_s
            (fun oidx (n, q) ->
              let%lwt strm = extract_group widths oidx tups in
              let%lwt v = self#eval lctx (Lwt_stream.of_list strm) q in
              return (n, v) )
            binds
        in
        let lctx =
          List.fold_left ~init:lctx
            ~f:(fun ctx (n, v) -> Map.set ctx ~key:n ~data:v)
            binds
        in
        (* The n+1 group contains values for the layout in the body of the let. *)
        let%lwt strm = extract_group widths (List.length binds) tups in
        self#eval lctx (Lwt_stream.of_list strm) q

      method private eval_var lctx tups n : 'a Lwt.t =
        let%lwt () = Lwt_stream.junk tups in
        return (Map.find_exn lctx n)

      method private eval lctx tups a : 'a Lwt.t =
        let tups =
          let l' = Q.width a in
          Lwt_stream.map
            (fun t ->
              let l = List.length t in
              if l = l' then t
              else Error.createf "Expected length %d got %d" l' l |> Error.raise )
            tups
        in
        match a.Q.node with
        | Q.Let x -> self#eval_let lctx tups x
        | Q.Var x -> self#eval_var lctx tups x
        | Q.For x -> self#eval_for lctx tups a x
        | Q.Concat x -> self#eval_concat lctx tups a x
        | Q.Empty -> self#eval_empty lctx tups a
        | Q.Scalars x -> self#eval_scalars lctx tups a x

      method run r =
        let q = A.ensure_alias r |> Q.of_ralgebra |> Q.hoist_all in
        let r = Q.to_ralgebra q |> simplify in
        let sql = Sql.of_ralgebra r in
        let tups =
          Db.exec_cursor_lwt_exn conn
            (A.schema_exn r |> List.map ~f:Name.type_exn)
            (Sql.to_string sql)
          |> Lwt_stream.map Array.to_list
        in
        self#eval (Map.empty (module String)) tups (Q.to_width q) |> Lwt_main.run
    end

  (** Returns the least general type of a layout. *)
  let rec least_general_of_layout r =
    match r.Abslayout.node with
    | Select (ps, r') | GroupBy (ps, _, r') ->
        T.FuncT ([least_general_of_layout r'], `Width (List.length ps))
    | OrderBy {rel= r'; _} | Filter (_, r') | Dedup r' ->
        FuncT ([least_general_of_layout r'], `Child_sum)
    | Join {r1; r2; _} ->
        FuncT ([least_general_of_layout r1; least_general_of_layout r2], `Child_sum)
    | AEmpty -> EmptyT
    | AScalar p -> Pred.to_type p |> T.least_general_of_primtype
    | AList (_, r') -> ListT (least_general_of_layout r', {count= Bottom})
    | DepJoin {d_lhs; d_rhs; _} ->
        FuncT
          ( [least_general_of_layout d_lhs; least_general_of_layout d_rhs]
          , `Child_sum )
    | AHashIdx h ->
        HashIdxT
          ( least_general_of_layout (A.h_key_layout h)
          , least_general_of_layout h.hi_values
          , {key_count= Bottom} )
    | AOrderedIdx (_, vr, {oi_key_layout= Some kr; _}) ->
        OrderedIdxT
          ( least_general_of_layout kr
          , least_general_of_layout vr
          , {key_count= Bottom} )
    | ATuple (rs, k) ->
        let kind =
          match k with
          | Cross -> `Cross
          | Concat -> `Concat
          | _ -> failwith "Unsupported"
        in
        TupleT (List.map rs ~f:least_general_of_layout, {kind})
    | As (_, r') -> least_general_of_layout r'
    | AOrderedIdx (_, _, {oi_key_layout= None; _}) | Relation _ ->
        failwith "Layout is still abstract."

  (** Returns a layout type that is general enough to hold all of the data. *)
  class type_fold =
    object
      inherit [_] abslayout_fold

      method! select _ (exprs, _) t = T.FuncT ([t], `Width (List.length exprs))

      method join _ _ t1 t2 = T.FuncT ([t1; t2], `Child_sum)

      method depjoin _ _ t1 t2 = T.FuncT ([t1; t2], `Child_sum)

      method! filter _ _ t = T.FuncT ([t], `Child_sum)

      method! order_by _ _ t = T.FuncT ([t], `Child_sum)

      method! dedup _ t = T.FuncT ([t], `Child_sum)

      method! group_by _ (exprs, _, _) t = FuncT ([t], `Width (List.length exprs))

      method empty _ = EmptyT

      method scalar _ _ =
        function
        | Value.Date x ->
            let x = Date.to_int x in
            DateT
              { range= T.AbsInt.of_int x
              ; nullable= false
              ; distinct= Map.singleton (module Int) x 1 }
        | Int x ->
            IntT
              { range= T.AbsInt.of_int x
              ; nullable= false
              ; distinct= Map.singleton (module Int) x 1 }
        | Bool _ -> BoolT {nullable= false}
        | String x ->
            StringT
              { nchars= T.AbsInt.of_int (String.length x)
              ; nullable= false
              ; distinct= Map.singleton (module String) x 1 }
        | Null -> NullT
        | Fixed x -> FixedT {value= T.AbsFixed.of_fixed x; nullable= false}

      method list _ ((_, elem_l) as l) =
        let init = (least_general_of_layout elem_l, 0) in
        let fold (t, c) (_, t') =
          try (T.unify_exn t t', c + 1)
          with Type.TypeError _ as exn ->
            Logs.err (fun m -> m "Type checking failed on: %a" A.pp (A.list' l)) ;
            Logs.err (fun m ->
                m "%a does not unify with %a" Sexp.pp_hum
                  ([%sexp_of: T.t] t)
                  Sexp.pp_hum
                  ([%sexp_of: T.t] t') ) ;
            raise exn
        in
        let extract (elem_type, num_elems) =
          T.ListT (elem_type, {count= T.AbsInt.of_int num_elems})
        in
        Fold {init; fold; extract}

      method tuple _ (_, kind) =
        let kind =
          match kind with Cross -> `Cross | Zip -> failwith "" | Concat -> `Concat
        in
        let init = RevList.empty in
        let fold = RevList.( ++ ) in
        let extract ts = T.TupleT (RevList.to_list ts, {kind}) in
        Fold {init; fold; extract}

      method hash_idx _ h =
        let init =
          ( 0
          , least_general_of_layout (A.h_key_layout h)
          , least_general_of_layout h.hi_values )
        in
        let fold (kct, kt, vt) (_, kt', vt') =
          (kct + 1, T.unify_exn kt kt', T.unify_exn vt vt')
        in
        let extract (kct, kt, vt) =
          T.HashIdxT (kt, vt, {key_count= T.AbsInt.of_int kct})
        in
        Fold {init; fold; extract}

      method ordered_idx _ (_, value_l, {oi_key_layout; _}) =
        let key_l = Option.value_exn oi_key_layout in
        let init =
          (least_general_of_layout key_l, least_general_of_layout value_l, 0)
        in
        let fold (kt, vt, ct) (_, kt', vt') =
          (T.unify_exn kt kt', T.unify_exn vt vt', ct + 1)
        in
        let extract (kt, vt, ct) =
          T.OrderedIdxT (kt, vt, {key_count= T.AbsInt.of_int ct})
        in
        Fold {init; fold; extract}
    end

  let type_of r =
    Log.info (fun m -> m "Computing type of abstract layout.") ;
    let type_ =
      try (new type_fold)#run r
      with exn ->
        let trace = Backtrace.Exn.most_recent () in
        Logs.err (fun m ->
            m "Type computation failed: %a@,%s" Exn.pp exn
              (Backtrace.to_string trace) ) ;
        raise exn
    in
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
    l |> A.strip_unused_as |> annotate_relations |> R.resolve ~params
    |> A.annotate_key_layouts

  let load_string ?params s = A.of_string_exn s |> load_layout ?params
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
      let q = Q.of_ralgebra ralgebra in
      let r = Q.to_ralgebra q in
      let sql = Sql.of_ralgebra r in
      printf "%s" (Sql.to_string_hum sql) ;
      [%expect
        {|
      SELECT
          "x0_0" AS "x0_0_0",
          "x1_0" AS "x1_0_0",
          "x2_0" AS "x2_0_0",
          "x3_0" AS "x3_0_0"
      FROM (
          SELECT
              row_number() OVER () AS "rn0_0",
              r1_0. "f" AS "f_1",
              r1_0. "g" AS "g_1"
          FROM
              "r1" AS "r1_0") AS "t1",
          LATERAL (
              SELECT
                  "rn0_0" AS "x0_0",
                  "f_1" AS "x1_0",
                  "g_1" AS "x2_0",
                  "g_1" AS "x3_0") AS "t0"
      ORDER BY
          "x1_0",
          "x2_0" |}]

    let%expect_test "" =
      let ralgebra =
        "depjoin(ascalar(0 as f) as k, select([k.f + g], alist(r1 as k1, \
         ascalar(k1.g))))" |> load_string
      in
      let q = Q.of_ralgebra ralgebra in
      let r = Q.to_ralgebra q in
      let sql = Sql.of_ralgebra r in
      printf "%s" (Sql.to_string_hum sql) ;
      [%expect
        {|
      SELECT
          "counter0_0" AS "counter0_0_0",
          "f_3" AS "f_3_0",
          "x4_0" AS "x4_0_0",
          "x5_0" AS "x5_0_0",
          "x6_0" AS "x6_0_0",
          "x7_0" AS "x7_0_0"
      FROM ((
              SELECT
                  0 AS "counter0_0",
                  0 AS "f_3",
                  (null::integer) AS "x4_0",
                  (null::integer) AS "x5_0",
                  (null::integer) AS "x6_0",
                  (null::integer) AS "x7_0")
          UNION ALL (
              SELECT
                  1 AS "counter0_1",
                  (null::integer) AS "f_6",
                  "x4_1" AS "x4_2",
                  "x5_1" AS "x5_2",
                  "x6_1" AS "x6_2",
                  "x7_1" AS "x7_2"
              FROM (
                  SELECT
                      row_number() OVER () AS "rn1_0",
                      r1_1. "f" AS "f_5",
                      r1_1. "g" AS "g_4"
                  FROM
                      "r1" AS "r1_1") AS "t3",
                  LATERAL (
                      SELECT
                          "rn1_0" AS "x4_1",
                          "f_5" AS "x5_1",
                          "g_4" AS "x6_1",
                          "g_4" AS "x7_1") AS "t2")) AS "t4"
      ORDER BY
          "counter0_0",
          "x5_0",
          "x6_0" |}]

    let%expect_test "" =
      let ralgebra = load_string Test_util.sum_complex in
      let q = Q.of_ralgebra ralgebra in
      let r = Q.to_ralgebra q in
      Format.printf "%a" pp r ;
      [%expect
        {|
      orderby([x9, x10],
        depjoin(select([row_number() as rn2, f, g], r1) as k,
          select([k.rn2 as x8, k.f as x9, k.g as x10, f as x11, v as x12],
            atuple([ascalar(k.f), ascalar((k.g - k.f) as v)], cross)))) |}] ;
      let sql = Sql.of_ralgebra r in
      printf "%s" (Sql.to_string_hum sql) ;
      [%expect
        {|
      SELECT
          "x8_0" AS "x8_0_0",
          "x9_0" AS "x9_0_0",
          "x10_0" AS "x10_0_0",
          "x11_0" AS "x11_0_0",
          "x12_0" AS "x12_0_0"
      FROM (
          SELECT
              row_number() OVER () AS "rn2_0",
              r1_2. "f" AS "f_8",
              r1_2. "g" AS "g_7"
          FROM
              "r1" AS "r1_2") AS "t6",
          LATERAL (
              SELECT
                  "rn2_0" AS "x8_0",
                  "f_8" AS "x9_0",
                  "g_7" AS "x10_0",
                  "f_8" AS "x11_0",
                  ("g_7") - ("f_8") AS "x12_0"
              WHERE (TRUE)) AS "t5"
      ORDER BY
          "x9_0",
          "x10_0" |}]

    let%expect_test "" =
      let ralgebra = load_string "alist(r1 as k, alist(r1 as j, ascalar(j.f)))" in
      let q = Q.of_ralgebra ralgebra in
      let r = Q.to_ralgebra q in
      Format.printf "%a" pp r ;
      [%expect
        {|
        orderby([x18, x19, x21, x22],
          depjoin(select([row_number() as rn3, f, g], r1) as k,
            select([k.rn3 as x17,
                    k.f as x18,
                    k.g as x19,
                    x13 as x20,
                    x14 as x21,
                    x15 as x22,
                    x16 as x23],
              depjoin(select([row_number() as rn4, f, g], r1) as j,
                select([j.rn4 as x13, j.f as x14, j.g as x15, f as x16],
                  atuple([ascalar(j.f)], cross)))))) |}] ;
      let sql = Sql.of_ralgebra r in
      printf "%s" (Sql.to_string_hum sql) ;
      [%expect
        {|
        SELECT
            "x17_0" AS "x17_0_0",
            "x18_0" AS "x18_0_0",
            "x19_0" AS "x19_0_0",
            "x20_0" AS "x20_0_0",
            "x21_0" AS "x21_0_0",
            "x22_0" AS "x22_0_0",
            "x23_0" AS "x23_0_0"
        FROM (
            SELECT
                row_number() OVER () AS "rn3_0",
                r1_3. "f" AS "f_11",
                r1_3. "g" AS "g_9"
            FROM
                "r1" AS "r1_3") AS "t10",
            LATERAL (
                SELECT
                    "rn3_0" AS "x17_0",
                    "f_11" AS "x18_0",
                    "g_9" AS "x19_0",
                    "x13_0" AS "x20_0",
                    "x14_0" AS "x21_0",
                    "x15_0" AS "x22_0",
                    "x16_0" AS "x23_0"
                FROM (
                    SELECT
                        row_number() OVER () AS "rn4_0",
                        r1_4. "f" AS "f_13",
                        r1_4. "g" AS "g_11"
                    FROM
                        "r1" AS "r1_4") AS "t8",
                    LATERAL (
                        SELECT
                            "rn4_0" AS "x13_0",
                            "f_13" AS "x14_0",
                            "g_11" AS "x15_0",
                            "f_13" AS "x16_0") AS "t7") AS "t9"
        ORDER BY
            "x18_0",
            "x19_0",
            "x21_0",
            "x22_0" |}]
  end )
