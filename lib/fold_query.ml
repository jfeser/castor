open Ast
module V = Visitors
open Schema
module A = Abslayout
module P = Pred.Infix

[@@@warning "-17"]

type scalar = (< >[@opaque]) annot pred [@@deriving sexp_of]

type ('q, 'm) node =
  | Empty
  | Scalars of (scalar[@name "pred"]) list
  | Concat of ('q, 'm) t list
  | For of ('q * string * ('q, 'm) t * bool)
  | Let of ((string * ('q, 'm) t) list * ('q, 'm) t)
  | Var of string

and ('q, 'm) t = { node : ('q, 'm) node; meta : 'm }
[@@deriving
  visitors { variety = "reduce" },
    visitors { variety = "mapreduce" },
    visitors { variety = "map" },
    sexp_of]

[@@@warning "+17"]

let empty c = { node = Empty; meta = c }

let scalars c p = { node = Scalars p; meta = c }

let concat c x = { node = Concat x; meta = c }

let for_ c x = { node = For x; meta = c }

let let_ c x = { node = Let x; meta = c }

let var c x = { node = Var x; meta = c }

(** A query is invariant in a set of scopes if it doesn't refer to any name in
   one of the scopes. *)
let is_invariant ss q =
  let names_visitor =
    object
      inherit [_] V.reduce

      inherit [_] Util.conj_monoid

      method! visit_Name () n =
        match Name.rel n with
        | Some s' -> not (List.mem ss s' ~equal:String.( = ))
        | None -> true
    end
  in
  let visitor =
    object (self : 'self)
      inherit [_] reduce

      inherit [_] Util.conj_monoid

      method visit_'q () q = names_visitor#visit_t () q

      method visit_'m () _ = self#zero

      method visit_pred () p = names_visitor#visit_pred () p

      (* Vars are not invariant so we don't have to reason about hoisting above
         other let bindings. *)
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
          (var None n, [ (n, q) ])
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
            | _ -> let_ None (binds, for_ q.meta (r, s, self#visit_t ss q', x))
            )
        | _ -> super#visit_t ss q
    end
  in
  visitor#visit_t [] q

let to_width q =
  let visitor =
    object
      inherit [_] map

      method visit_'q () q = List.length (schema q)

      method visit_'m () x = x

      method visit_pred () x = x
    end
  in
  visitor#visit_t () q

let total_order_key q =
  let native_order = A.order_of q in
  let total_order = List.map (schema q) ~f:(fun n -> (Name n, Asc)) in
  native_order @ total_order

let to_scalars rs =
  List.map rs ~f:(fun t ->
      match t.Ast.node with AScalar p -> Some p | _ -> None)
  |> Option.all

let of_list of_ralgebra q { l_keys = q1; l_scope = scope; l_values = q2 } =
  let q1 =
    let order_key = total_order_key q1 in
    A.order_by order_key (strip_meta q1)
  in
  for_ q (q1, scope, of_ralgebra q2, false)

let of_hash_idx of_ralgebra q h =
  let q1 =
    let order_key = total_order_key h.hi_keys in
    A.order_by order_key @@ A.dedup @@ strip_meta h.hi_keys
  in
  for_ q (q1, h.hi_scope, of_ralgebra h.hi_values, true)

let of_ordered_idx of_ralgebra q
    { oi_keys = q1; oi_values = q2; oi_scope = scope; _ } =
  let q1 =
    let order_key = total_order_key q1 in
    A.order_by order_key @@ A.dedup @@ strip_meta q1
  in
  for_ q (q1, scope, of_ralgebra q2, true)

(** Convert a query to the simplified fold query AST. *)
let rec of_ralgebra : 'a. (< .. > as 'a) annot -> (< > annot, 'a annot) t =
 fun q ->
  match q.Ast.node with
  | AList x -> of_list of_ralgebra q x
  | AHashIdx h -> of_hash_idx of_ralgebra q h
  | AOrderedIdx x -> of_ordered_idx of_ralgebra q x
  | AEmpty | Range _ -> empty q
  | AScalar p -> scalars q [ Pred.strip_meta p ]
  | ATuple (ts, _) -> (
      match to_scalars ts with
      | Some ps -> scalars q (List.map ~f:Pred.strip_meta ps)
      | None -> concat q (List.map ~f:of_ralgebra ts) )
  | DepJoin { d_lhs = q1; d_rhs = q2; _ } | Join { r1 = q1; r2 = q2; _ } ->
      concat q [ of_ralgebra q1; of_ralgebra q2 ]
  | Select (_, q')
  | Filter (_, q')
  | Dedup q'
  | OrderBy { rel = q'; _ }
  | GroupBy (_, _, q') ->
      { (of_ralgebra q') with meta = q }
  | Relation _ -> failwith "Bare relation."

let to_concat binds q = concat None (List.map binds ~f:Tuple.T2.get2 @ [ q ])

let unwrap_order r =
  match r.Ast.node with OrderBy { key; rel } -> (key, rel) | _ -> ([], r)

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

let wrap q = map_meta ~f:Option.some q

let unwrap q = map_meta ~f:(fun m -> Option.value_exn m) q

(** Convert a fold query into a ralgebra query that produces the stream that the
   fold acts on. *)
let rec to_ralgebra q =
  match q.node with
  | Var _ -> A.scalar @@ As_pred (Int 0, Fresh.name Global.fresh "var%d")
  | Let (binds, q) -> to_ralgebra (to_concat binds q)
  | For (q1, scope, q2, distinct) ->
      (* Extend the lhs query with a row number. Even if this query emits
         duplicate results, the row number will ensure that the duplicates
         remain distinct. *)
      let o1, q1 =
        let o1, q1 = unwrap_order q1 in
        let count = Fresh.name Global.fresh "ct%d" in
        let o1, q1 =
          if distinct then (o1, q1)
          else
            ( o1,
              A.group_by
                (As_pred (Count, count) :: (schema q1 |> Schema.to_select_list))
                (schema q1) q1 )
        in
        (o1, q1)
      in
      let o2, q2 = to_ralgebra q2 |> unwrap_order in
      (* Generate a renaming so that the upward exposed names are fresh. *)
      let sctx =
        (schema q1 |> Schema.scoped scope) @ schema q2
        |> List.map ~f:(fun n ->
               let n' = Fresh.name Global.fresh "x%d" in
               (n, n'))
      in
      let slist = List.map sctx ~f:(fun (n, n') -> P.as_ (P.name n) n') in
      (* Stick together the orders from the lhs and rhs queries. *)
      let order =
        let sctx =
          List.map sctx ~f:(fun (n, n') -> (n, P.name @@ Name.create n'))
          |> Map.of_alist_exn (module Name)
        in
        (* The renaming refers to the scoped names from q1, so scope before
           renaming. *)
        let o1 =
          List.map o1 ~f:(fun (p, o) -> (Pred.scoped (schema q1) scope p, o))
        in
        List.map (o1 @ o2) ~f:(fun (p, o) -> (Pred.subst sctx p, o))
      in
      A.order_by order @@ A.dep_join q1 scope @@ A.select slist q2
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
              P.(as_ (int i) counter_name)
              :: List.concat_mapi qs ~f:(fun j q' ->
                     let f n =
                       (* Take the names from this query's schema. *)
                       if i = j then P.name n (* Otherwise emit null. *)
                       else P.as_ (Null (Some (Name.type_exn n))) (Name.name n)
                     in
                     List.map (schema q') ~f)
            in
            A.select select_list q)
      and order =
        (P.name (Name.create counter_name), Asc) :: List.concat orders
      in
      A.order_by order @@ A.tuple queries_norm Concat
  | Empty -> A.empty
  | Scalars ps -> A.tuple (List.map ps ~f:A.scalar) Cross

let rec n_parallel q =
  match q.node with
  | Empty -> 0
  | For _ | Scalars _ | Var _ -> 1
  | Let (binds, q) ->
      List.sum (module Int) binds ~f:(fun (_, q) -> n_parallel q) + n_parallel q
  | Concat qs -> List.sum (module Int) qs ~f:n_parallel

let to_ralgebra q =
  Log.info (fun m -> m "Potential parallelism: %d queries" (n_parallel q));
  let order, query = unwrap_order @@ to_ralgebra @@ wrap q in
  A.order_by order @@ A.dedup query

let rec width' q =
  match q.node with
  | Empty -> 0
  | Var _ -> 1
  | For (n, _, q2, distinct) -> (if distinct then 0 else 1) + n + width' q2
  | Scalars ps -> List.length ps
  | Concat qs -> 1 + List.sum (module Int) qs ~f:width'
  | Let (binds, q) -> width' (to_concat binds q)

let width q = wrap q |> width'
