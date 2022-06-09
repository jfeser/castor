(** In this module, we assume that dep_join returns attributes from both its lhs
   and rhs. This assumption is safe because we first wrap depjoins in selects
   that project out their lhs attributes. The queries returned by the main
   function no longer contain depjoins. *)

open Core
open Collections
open Ast
module V = Visitors
module A = Abslayout
module P = Pred.Infix
include (val Log.make "castor.unnest")

let default_meta =
  object
    method was_depjoin = false
  end

module Q = Constructors.Query

module T = struct
  type ('p, 'r) visible_depjoin_query =
    | Query of ('p, 'r) query
    | Visible_depjoin of 'r depjoin
  [@@deriving sexp]

  type 'm visible_depjoin_annot = {
    node :
      ( 'm visible_depjoin_annot pred,
        'm visible_depjoin_annot )
      visible_depjoin_query;
    meta : 'm;
  }
  [@@deriving sexp]
end

open T

(** Schemas work differently when the lhs of a depjoin is visible in its output. *)
let rec schema q =
  match q.node with
  | Visible_depjoin { d_lhs; d_rhs } -> schema d_lhs @ schema d_rhs
  | Query q -> Schema.schema_query_open schema q

let schema_set r = Set.of_list (module Name) (schema r)
let zero = Set.map (module Name) ~f:Name.zero
let decr = Set.map (module Name) ~f:Name.decr

let rec free r = free_query r.node

and free_query = function
  | Visible_depjoin d ->
      Set.O.(
        free d.d_lhs
        || decr
             (free d.d_rhs - zero (Set.of_list (module Name) (schema d.d_lhs))))
  | Query q -> Free.free_query_open ~schema_set free q

let assert_schema_invariant q q' =
  let s = Schema.schema q in
  let s' = schema q' in
  if [%equal: Name.t list] s s' then ()
  else
    Error.create "Schema mismatch" (s, s', q, q')
      [%sexp_of: Name.t list * Name.t list * _ annot * _ visible_depjoin_annot]
    |> Error.raise

let attrs q = schema q |> Set.of_list (module Name)

let to_visible_depjoin r =
  let id = ref 0 in
  let fresh_id () =
    incr id;
    !id
  in
  let rec annot (r : _ annot) : _ visible_depjoin_annot =
    { node = query r.node; meta = r.meta }
  and query = function
    | DepJoin d ->
        (* Ensure that the output attributes are the same under the modified
           depjoin semantics. Note that we use Schema.schema on the unprocessed
           rhs and schema on the processed rhs to account for the difference in
           semantics. *)
        let old_lhs_schema = Schema.schema d.d_lhs in
        let d_lhs = annot d.d_lhs in

        let lhs_renaming =
          let id = fresh_id () in
          List.map2_exn (schema d_lhs) old_lhs_schema ~f:(fun n n' ->
              (n, sprintf "%d_%s" id (Name.name n')))
        in
        let lhs_subst =
          List.map lhs_renaming ~f:(fun (n, n') ->
              (Name.zero n, P.name (Name.create n')))
          |> Map.of_alist_exn (module Name)
        in
        let lhs_project =
          List.map lhs_renaming ~f:(fun (n, n') -> (P.name n, n'))
        in
        let d_rhs_renamed = Subst.subst lhs_subst d.d_rhs in
        let old_rhs_schema = Schema.schema d.d_rhs in
        let d_rhs = annot d_rhs_renamed in

        let rhs_project =
          List.map2_exn (schema d_rhs) old_rhs_schema ~f:(fun n n' ->
              (P.name n, Name.name n'))
        in
        Query
          (Select
             ( rhs_project,
               {
                 node =
                   Visible_depjoin
                     {
                       d_lhs =
                         {
                           node = Query (Select (lhs_project, d_lhs));
                           meta = object end;
                         };
                       d_rhs;
                     };
                 meta = object end;
               } ))
    | q -> Query (V.Map.query annot pred q)
  and pred p = V.Map.pred annot pred p in
  let r' = annot r in
  assert_schema_invariant r r';
  r'

(** Check that the LHS of a dependent join satisfies its no-duplicates requirement. *)
let check_lhs lhs =
  match lhs.node with
  | Query (Dedup _) -> ()
  | _ ->
      Error.create "Expected LHS to be distinct" lhs
        [%sexp_of: _ visible_depjoin_annot]
      |> Error.raise

let dep_join d_lhs d_rhs =
  check_lhs d_lhs;
  { node = Visible_depjoin { d_lhs; d_rhs }; meta = default_meta }

let dedup x = { node = Query (Dedup x); meta = default_meta }
let select x y = { node = Query (Select (x, y)); meta = default_meta }

let join pred r1 r2 =
  { node = Query (Join { pred; r1; r2 }); meta = default_meta }

let filter x y = { node = Query (Filter (x, y)); meta = default_meta }
let group_by x y z = { node = Query (GroupBy (x, y, z)); meta = default_meta }
let tuple x y = { node = Query (ATuple (x, y)); meta = default_meta }
let range x y = { node = Query (Range (x, y)); meta = default_meta }

let order_by key rel =
  { node = Query (OrderBy { key; rel }); meta = default_meta }

let simple_join lhs rhs =
  {
    node = Query (Q.join (`Bool true) lhs rhs);
    meta =
      object
        method was_depjoin = true
      end;
  }

let to_nice_depjoin (t1 : _ visible_depjoin_annot)
    (t2 : _ visible_depjoin_annot) =
  let t1_attr = attrs t1 in
  let t2_free = free t2 in

  (* Create a relation of the unique values of the attributes that come from t1
     and are bound in t2. *)
  let d =
    let attrs =
      Set.inter t2_free t1_attr |> Set.to_list |> Select_list.of_names
    in
    dedup @@ select attrs t1
  in

  (* Create a renaming of the attribute names in d. This will ensure that we can
     join d and t1 without name clashes. *)
  let subst =
    schema d
    |> List.map ~f:(fun n -> (n, sprintf "lhs_%s" (Name.name n)))
    |> Map.of_alist_exn (module Name)
  in

  (* Apply the renaming to t1. *)
  let t1 =
    let select_list =
      schema t1
      |> List.map ~f:(fun n ->
             match Map.find subst n with
             | Some alias -> (P.name n, alias)
             | None -> (P.name n, Name.name n))
    in
    select select_list t1
  in

  (* Create a predicate that joins t1 and d. *)
  let join_pred =
    Map.to_alist subst
    |> List.map ~f:(fun (n, n') -> P.(name n = name (Name.create n')))
    |> P.and_
  in

  let t2_project = Select_list.of_names @@ schema t2 in

  Query (Select (t2_project, join join_pred t1 (dep_join d t2)))

let rec to_nice r = { node = to_nice_query r.node; meta = default_meta }

and to_nice_query = function
  | Visible_depjoin d ->
      let d_lhs = to_nice d.d_lhs in
      let d_rhs = to_nice d.d_rhs in
      to_nice_depjoin d_lhs d_rhs
  | Query q -> Query (V.Map.query to_nice to_nice_pred q)

and to_nice_pred p = V.Map.pred to_nice to_nice_pred p

let push_join d { pred = p; r1 = t1; r2 = t2 } =
  let d_attr = attrs d in
  (* A selection list that maintains the original schema. *)
  let orig_select = Select_list.of_names (schema d @ schema t1 @ schema t2) in
  if
    (* The rhs of the join is not dependent. *)
    Set.inter (free t2) d_attr |> Set.is_empty
  then join p (dep_join d t1) t2
  else if
    (* The lhs of the join is not dependent. *)
    Set.inter (free t1) d_attr |> Set.is_empty
  then
    (* Pushing the depjoin to the right will change the schema. *)
    select orig_select @@ join p t1 @@ dep_join d t2
  else
    (* Rename the d relation in the rhs of the join *)
    let d_rhs =
      List.map (schema d) ~f:(fun n -> (n, sprintf "rhs_%s" (Name.name n)))
    in
    let rhs_select =
      List.map d_rhs ~f:(fun (x, x') -> (P.name x, x'))
      @ Select_list.of_names (schema t2)
    in

    (* Perform a natural join on the two copies of d *)
    let d_pred =
      P.and_
        (List.map d_rhs ~f:(fun (x, x') -> P.(name x = name (Name.create x'))))
    in
    select orig_select
    @@ join P.(p && d_pred) (dep_join d t1) (select rhs_select @@ dep_join d t2)

let push_filter d (p, t2) = filter p (dep_join d t2)

let push_groupby d (aggs, keys, q) =
  let schema = schema d in
  let aggs = Select_list.of_names schema @ aggs in
  let keys = keys @ schema in
  group_by aggs keys (dep_join d q)

let push_select d (preds, q) =
  let d_schema = schema d in
  let preds = Select_list.of_names d_schema @ preds in
  match A.select_kind preds with
  | `Scalar -> select preds (dep_join d q)
  | `Agg ->
      let preds =
        Select_list.map preds ~f:(fun p n ->
            match Pred.kind p with
            | `Agg -> p
            | `Scalar ->
                let n = Name.create n in
                if List.mem ~equal:[%equal: Name.t] d_schema n then P.name n
                else `Min p
            | `Window -> p)
      in
      group_by preds d_schema (dep_join d q)

let rec eqs r = eqs_query r.node

and eqs_query = function
  | Query q -> Equiv.eqs_query_open ~schema eqs q
  | Visible_depjoin d -> eqs d.d_rhs

let rec order_of r = order_of_query r.node

and order_of_query = function
  | Query q -> A.order_query_open ~schema ~eq:eqs order_of q
  | Visible_depjoin _ -> []

(** Push a dependent join with an orderby on the rhs. Preserves the order of the
   lhs and the rhs. *)
let push_orderby d { key; rel } =
  let d_order = order_of d in
  order_by (d_order @ key) (dep_join d rel)

let push_concat_tuple d qs = tuple (List.map qs ~f:(dep_join d)) Concat

let stuck d =
  Error.create "Stuck depjoin" d [%sexp_of: _ visible_depjoin_annot depjoin]
  |> Error.raise

let push_cross_tuple d qs =
  let select_list =
    Select_list.of_names (schema d.d_lhs)
    @ List.map qs ~f:(fun q ->
          match q.node with
          | Query (AScalar p) -> (p.s_pred, p.s_name)
          | _ -> stuck d)
  in
  select select_list d.d_lhs

let push_scalar d p =
  let select_list =
    Select_list.of_names (schema d) @ [ (p.s_pred, p.s_name) ]
  in
  select select_list d

let push_dedup d q = dedup (dep_join d q)

let push_range d (lo, hi) =
  let fresh_name f = Fresh.name Global.fresh f in
  let rhs =
    range
      (`First (group_by [ (`Min lo, fresh_name "min%d") ] [] d))
      (`First (group_by [ (`Max hi, fresh_name "max%d") ] [] d))
  and join_pred =
    let v = `Name (Name.create "range") in
    P.(lo <= v && v <= hi)
  in
  join join_pred d rhs

let rec push_depjoin r : _ visible_depjoin_annot =
  match r.node with
  | Visible_depjoin ({ d_lhs; d_rhs } as d) ->
      check_lhs d_lhs;
      let d_lhs = push_depjoin d_lhs in
      let d_rhs = push_depjoin d_rhs in

      (* `If the join is not really dependent, then replace with a regular join.
         *)
      if Set.inter (free d_rhs) (attrs d_lhs) |> Set.is_empty then
        simple_join d_lhs d_rhs
      else
        (* Otherwise, push the depjoin further down the tree. *)
        let r' =
          match d_rhs.node with
          | Query (Filter x) -> push_filter d_lhs x
          | Query (Join x) -> push_join d_lhs x
          | Query (GroupBy x) -> push_groupby d_lhs x
          | Query (Select x) -> push_select d_lhs x
          | Query (ATuple (qs, Concat)) -> push_concat_tuple d_lhs qs
          | Query (ATuple (qs, Cross)) -> push_cross_tuple d qs
          | Query (AScalar p) -> push_scalar d_lhs p
          | Query (Dedup x) -> push_dedup d_lhs x
          | Query (OrderBy x) -> push_orderby d_lhs x
          | Query (Range x) -> push_range d_lhs x
          | _ -> stuck d
        in
        push_depjoin r'
  | Query q ->
      {
        node = Query (V.Map.query push_depjoin push_depjoin_pred q);
        meta = default_meta;
      }

and push_depjoin_pred p = V.Map.pred push_depjoin push_depjoin_pred p

let rec map_meta f { node; meta } =
  { node = map_meta_query f node; meta = f meta }

and map_meta_query f = function
  | Visible_depjoin d -> Visible_depjoin (V.Map.dep_join (map_meta f) d)
  | Query q -> Query (V.Map.query (map_meta f) (map_meta_pred f) q)

and map_meta_pred f p = V.Map.pred (map_meta f) (map_meta_pred f) p

let rec of_visible_depjoin r : _ annot =
  { node = of_visible_depjoin_query r.node; meta = r.meta }

and of_visible_depjoin_query = function
  | Visible_depjoin _ -> failwith "unexpected depjoin"
  | Query q -> V.Map.query of_visible_depjoin of_visible_depjoin_pred q

and of_visible_depjoin_pred p =
  V.Map.pred of_visible_depjoin of_visible_depjoin_pred p

let unnest q =
  strip_meta q |> Layout_to_depjoin.annot |> to_visible_depjoin
  |> map_meta (fun _ -> default_meta)
  |> to_nice |> push_depjoin |> of_visible_depjoin |> Cardinality.annotate
  (* We can remove the results of an eliminated depjoin regardless of the
     usual rules around cardinality preservation. *)
  |> V.map_meta (fun m ->
         object
           method cardinality_matters =
             (not m#meta#was_depjoin) && m#cardinality_matters

           method why_card_matters = m#why_card_matters
         end)
  |> Join_elim.remove_joins |> A.hoist_meta

module Private = struct
  include T

  let attrs = attrs
  let free = free
  let to_nice_depjoin = to_nice_depjoin
  let to_visible_depjoin = to_visible_depjoin
  let of_visible_depjoin = of_visible_depjoin
  let map_meta = map_meta

  let rec pp fmt r =
    match r.node with
    | Query q -> Abslayout_pp.pp_query_open pp pp_pred fmt q
    | Visible_depjoin d -> Fmt.pf fmt "vdepjoin(%a,@ %a)" pp d.d_lhs pp d.d_rhs

  and pp_pred fmt p = Abslayout_pp.pp_pred_open pp pp_pred fmt p
end
