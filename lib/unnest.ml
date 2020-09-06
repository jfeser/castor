open Ast
module V = Visitors
open Schema
module A = Abslayout
module P = Pred.Infix

include (val Log.make "castor.unnest")

let default_meta =
  object
    method was_depjoin = false
  end

module Q = Constructors.Query

module C = (val Constructors.Annot.with_strip_meta (fun () -> default_meta))

(** In this module, we assume that dep_join returns attributes from both its lhs
   and rhs. This assumption is safe because we first wrap depjoins in selects
   that project out their lhs attributes. The queries returned by the main
   function no longer contain depjoins. *)

(** Schemas work differently when the lhs of a depjoin is visible in its output. *)
let rec schema q =
  match q.node with
  | DepJoin { d_lhs; d_rhs; d_alias } ->
      ( schema d_lhs
      |> List.map ~f:(fun n ->
             Name.create (sprintf "%s_%s" d_alias (Name.name n))) )
      @ schema d_rhs
  | _ -> Schema.schema_open schema q

let schema_invariant q q' =
  let s = Schema.schema q in
  let s' = schema q' in
  if [%compare.equal: Schema.t] s s' then ()
  else
    Error.create "Schema mismatch" (s, s', q, q')
      [%sexp_of: Schema.t * Schema.t * _ annot * _ annot]
    |> Error.raise

let attrs q = schema q |> Set.of_list (module Name)

let unscope n =
  match Name.rel n with
  | Some s -> Name.copy ~scope:None ~name:(sprintf "%s_%s" s (Name.name n)) n
  | None -> n

class to_lhs_visible_depjoin =
  object (self)
    inherit [_] V.map as super

    method! visit_Name () n = Name (unscope n)

    method visit_output_pred p =
      match Pred.to_name p with
      | None -> self#visit_pred () p
      | Some n -> P.as_ (self#visit_pred () p) (Name.name n)

    method! visit_AScalar () p = AScalar (self#visit_output_pred p)

    method! visit_Select () (ps, r) =
      Select (List.map ~f:self#visit_output_pred ps, self#visit_t () r)

    method! visit_DepJoin () d =
      (* Ensure that the output attributes are the same under the modified
         depjoin semantics. Note that we use Schema.schema on the unprocessed
         rhs and schema on the processed rhs to account for the difference in
         semantics. *)
      let old_rhs_schema = Schema.schema d.d_rhs in
      let old_lhs_schema = Schema.schema d.d_lhs in
      let d = super#visit_depjoin () d in
      let projection =
        List.map2_exn (schema d.d_rhs) old_rhs_schema ~f:(fun n n' ->
            if [%compare.equal: Name.t] n n' then P.name n
            else P.(as_ (name n) (Name.name n')))
      in
      let renaming =
        List.map2_exn (schema d.d_lhs) old_lhs_schema ~f:(fun n n' ->
            P.(as_ (name n) (sprintf "%s_%s" d.d_alias (Name.name n'))))
      in
      Q.select projection
        (A.dep_join' { d with d_lhs = A.select renaming d.d_lhs })
  end

let to_visible_depjoin q =
  let q' = (new to_lhs_visible_depjoin)#visit_t () q in
  schema_invariant q q';
  q'

(** Check that the LHS of a dependent join satisfies its no-duplicates requirement. *)
let check_lhs lhs =
  match lhs.node with
  | Dedup _ -> ()
  | _ ->
      Error.create "Expected LHS to be distinct" lhs [%sexp_of: _ annot]
      |> Error.raise

let dep_join lhs rhs =
  check_lhs lhs;
  C.dep_join lhs "x" rhs

let simple_join lhs rhs =
  {
    (C.join (Bool true) lhs rhs) with
    meta =
      object
        method was_depjoin = true
      end;
  }

let to_nice_depjoin t1 t2 =
  let t1_attr = attrs t1 in
  let t2_free = Free.free t2 in

  (* Create a relation of the unique values of the attributes that come from t1
     and are bound in t2. *)
  let d =
    let attrs =
      Set.inter t2_free t1_attr |> Set.to_list |> List.map ~f:P.name
    in
    C.dedup @@ C.select attrs t1
  in

  (* Create a renaming of the attribute names in d. This will ensure that we can
     join d and t1 without name clashes. *)
  let subst =
    schema d
    |> List.map ~f:(fun n -> (n, Fresh.name Global.fresh "bnd%d"))
    |> Map.of_alist_exn (module Name)
  in

  (* Apply the renaming to t1. *)
  let t1 =
    let select_list =
      schema t1
      |> List.map ~f:(fun n ->
             match Map.find subst n with
             | Some alias -> Pred.Infix.(as_ (name n) alias)
             | None -> P.name n)
    in
    C.select select_list t1
  in

  (* Create a predicate that joins t1 and d. *)
  let join_pred =
    Map.to_alist subst
    |> List.map ~f:(fun (n, n') -> Pred.Infix.(name n = name (Name.create n')))
    |> Pred.conjoin
  in

  C.join join_pred t1 (dep_join d t2)

let rec to_nice r =
  match r.node with
  | DepJoin d ->
      let d_lhs = to_nice d.d_lhs in
      let d_rhs = to_nice d.d_rhs in
      to_nice_depjoin d_lhs d_rhs
  | q -> { r with node = V.Map.query to_nice to_nice_pred q }

and to_nice_pred p = V.Map.pred to_nice to_nice_pred p

let push_join d { pred = p; r1 = t1; r2 = t2 } =
  let d_attr = attrs d in
  (* A selection list that maintains the original schema. *)
  let orig_select = schema d @ schema t1 @ schema t2 |> List.map ~f:P.name in
  if
    (* The rhs of the join is not dependent. *)
    Set.inter (Free.free t2) d_attr |> Set.is_empty
  then C.join p (dep_join d t1) t2
  else if
    (* The lhs of the join is not dependent. *)
    Set.inter (Free.free t1) d_attr |> Set.is_empty
  then
    (* Pushing the depjoin to the right will change the schema. *)
    C.select orig_select @@ C.join p t1 @@ dep_join d t2
  else
    (* Rename the d relation in the rhs of the join *)
    let d_rhs =
      schema d |> List.map ~f:(fun n -> (n, Fresh.name Global.fresh "d%d"))
    in
    let rhs_select =
      List.map d_rhs ~f:(fun (x, x') -> Infix.(as_ (name x) x'))
      @ (schema t2 |> List.map ~f:P.name)
    in

    (* Perform a natural join on the two copies of d *)
    let d_pred =
      List.map d_rhs ~f:(fun (x, x') -> Infix.(name x = n x')) |> Pred.conjoin
    in
    C.select orig_select
    @@ C.join
         Infix.(p && d_pred)
         (dep_join d t1)
         (C.select rhs_select @@ dep_join d t2)

let push_filter d (p, t2) = C.filter p (dep_join d t2)

let push_groupby d (aggs, keys, q) =
  let schema = schema d in
  let aggs = Select_list.(Schema.to_select_list schema @ aggs)
  and keys = keys @ schema in
  C.group_by aggs keys (dep_join d q)

let push_select d (preds, q) =
  let d_schema = schema d in
  let preds = (d_schema |> List.map ~f:P.name) @ preds in
  match A.select_kind preds with
  | `Scalar -> C.select preds (dep_join d q)
  | `Agg ->
      let preds =
        List.map preds ~f:(fun p ->
            match Pred.kind p with
            | `Agg -> p
            | `Scalar -> (
                match Pred.to_name p with
                | Some n ->
                    if List.mem d_schema ~equal:[%compare.equal: Name.t] n then
                      P.name n
                    else P.as_ (Min p) (Name.name n)
                | None -> Min p )
            | `Window -> p)
      in
      C.group_by preds d_schema (dep_join d q)

(** Push a dependent join with an orderby on the rhs. Preserves the order of the
   lhs and the rhs. *)
let push_orderby d { key; rel } =
  let d_order = A.order_of d |> List.map ~f:(fun (p, o) -> (C.pred p, o)) in
  C.order_by (d_order @ key) (dep_join d rel)

let push_concat_tuple d qs = C.tuple (List.map qs ~f:(dep_join d)) Concat

let stuck d =
  Error.create "Stuck depjoin" d [%sexp_of: _ annot depjoin] |> Error.raise

let push_cross_tuple d qs =
  let select_list =
    (schema d.d_lhs |> to_select_list)
    @ List.map qs ~f:(fun q ->
          match q.node with AScalar p -> p | _ -> stuck d)
  in
  C.select select_list d.d_lhs

let push_scalar d p =
  let select_list = (schema d |> to_select_list) @ [ p ] in
  C.select select_list d

let push_dedup d q = C.dedup (dep_join d q)

let push_range d (lo, hi) =
  let fresh_name f = Fresh.name Global.fresh f in
  let rhs =
    C.range
      (First (C.group_by [ P.as_ (Min lo) (fresh_name "min%d") ] [] d))
      (First (C.group_by [ P.as_ (Max hi) (fresh_name "max%d") ] [] d))
  and join_pred =
    let v = Name (Name.create "range") in
    P.(lo <= v && v <= hi)
  in
  C.join join_pred d rhs

let rec push_depjoin r =
  match r.node with
  | DepJoin ({ d_lhs; d_rhs; _ } as d) ->
      check_lhs d_lhs;
      let d_lhs = push_depjoin d_lhs in
      let d_rhs = push_depjoin d_rhs in

      (* If the join is not really dependent, then replace with a regular join.
         *)
      if Set.inter (Free.free d_rhs) (attrs d_lhs) |> Set.is_empty then
        simple_join d_lhs d_rhs
      else
        (* Otherwise, push the depjoin further down the tree. *)
        let r' =
          match d_rhs.node with
          | Filter x -> push_filter d_lhs x
          | Join x -> push_join d_lhs x
          | GroupBy x -> push_groupby d_lhs x
          | Select x -> push_select d_lhs x
          | ATuple (qs, Concat) -> push_concat_tuple d_lhs qs
          | ATuple (qs, Cross) -> push_cross_tuple d qs
          | AScalar p -> push_scalar d_lhs p
          | Dedup x -> push_dedup d_lhs x
          | OrderBy x -> push_orderby d_lhs x
          | Range x -> push_range d_lhs x
          | _ -> stuck d
        in
        push_depjoin r'
  | q -> { r with node = V.Map.query push_depjoin push_depjoin_pred q }

and push_depjoin_pred p = V.Map.pred push_depjoin push_depjoin_pred p

let hoist_meta r = V.map_meta (fun m -> m#meta) r

let unnest q =
  let check q' =
    schema_invariant q q';
    Validate.annot q'
  in
  let q_visible =
    q |> strip_meta |> Layout_to_depjoin.annot |> to_visible_depjoin
    |> V.map_meta (fun _ -> default_meta)
  in
  let q_nice = to_nice q_visible in
  check q_nice;
  let q_pushed = push_depjoin q_nice in
  check q_pushed;
  let q' =
    q_pushed |> Cardinality.annotate
    (* We can remove the results of an eliminated depjoin regardless of the
       usual rules around cardinality preservation. *)
    |> V.map_meta (fun m ->
           object
             method cardinality_matters =
               (not m#meta#was_depjoin) && m#cardinality_matters

             method why_card_matters = m#why_card_matters
           end)
    |> Join_elim.remove_joins |> hoist_meta
  in
  check q';
  Validate.resolve q q';
  q'
