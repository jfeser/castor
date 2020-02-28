open Ast
open Schema
open Abslayout_visitors
open Collections
module A = Abslayout
module P = Pred.Infix

let project_def refcnt p =
  match Pred.to_name p with
  | None ->
      (* Filter out definitions that have no name *)
      false
  | Some n -> (
      (* Filter out definitions that are never referenced. *)
      match Map.find refcnt n with
      | Some c -> c
      | None ->
          (* Be conservative if refcount is missing. *)
          true )

let project_defs refcnt ps = List.filter ps ~f:(project_def refcnt)

(** True if all fields emitted by r are unreferenced when emitted by r'. *)
let all_unref_at r r' =
  Schema.schema r |> Schema.unscoped
  |> List.for_all ~f:(fun n ->
         match Map.find r'.meta#meta#refs n with
         | Some c -> not c
         | None -> false)

(** True if all fields emitted by r are unreferenced. *)
let all_unref r = all_unref_at r r

let dummy () = A.scalar (As_pred (Bool false, Fresh.name Global.fresh "d%d"))

let rec project r =
  let refs = r.meta#meta#refs in
  let card_matters = r.meta#cardinality_matters in
  if all_unref r && not card_matters then dummy ()
  else
    match r.node with
    | Select (ps, r) ->
        A.select (project_defs refs ps |> List.map ~f:project_pred) (project r)
    | GroupBy (ps, ns, r) ->
        A.group_by
          (project_defs refs ps |> List.map ~f:project_pred)
          ns (project r)
    | Dedup r ->
        if card_matters then A.dedup @@ no_project r else A.dedup @@ project r
    | AList (rk, rv) ->
        let scope = A.scope_exn rk in
        let rk = A.strip_scope rk in
        let rk =
          let old_n = schema rk |> List.length in
          let ps =
            project_defs rk.meta#meta#refs (schema rk |> List.map ~f:P.name)
            |> List.map ~f:project_pred
          in
          let new_n = List.length ps in
          if old_n > new_n then A.select ps (no_project rk) else project rk
        in
        A.list rk scope (project rv)
    | AScalar p ->
        if project_def refs p then A.scalar (project_pred p) else dummy ()
    | ATuple ([], _) -> A.empty
    | ATuple ([ r ], _) -> project r
    | ATuple (rs, Concat) -> A.tuple (List.map rs ~f:project) Concat
    | ATuple (rs, Cross) ->
        let rs =
          (* Remove unreferenced parts of the tuple. *)
          List.filter rs ~f:(fun r ->
              let is_unref = all_unref r in
              let is_scalar =
                match r.node with AScalar _ -> true | _ -> false
              in
              let should_remove =
                is_unref && is_scalar
                (* match count with
                 * (\* If the count matters, then we can only remove
                 *        unreferenced scalars. *\)
                 * | Exact -> is_unref && is_scalar
                 * (\* Otherwise we can remove anything unreferenced. *\)
                 * | AtLeastOne -> is_unref *)
              in
              not should_remove)
          |> List.map ~f:project
        in
        let rs = if List.length rs = 0 then [ dummy () ] else rs in
        A.tuple rs Cross
    | Join { r1; r2; pred } ->
        if card_matters then
          A.join (project_pred pred) (project r1) (project r2)
        else if
          (* If one side of a join is unused then the join can be dropped. *)
          all_unref_at r1 r
        then project r2
        else if all_unref_at r2 r then project r1
        else A.join (project_pred pred) (project r1) (project r2)
    | DepJoin { d_lhs; d_rhs; d_alias } ->
        if card_matters then A.dep_join (project d_lhs) d_alias (project d_rhs)
        else if
          (* If one side of a join is unused then the join can be dropped. *)
          all_unref d_lhs
        then project d_rhs
        else if all_unref d_rhs then dummy ()
        else A.dep_join (project d_lhs) d_alias (project d_rhs)
    | Range (p, p') -> A.range (project_pred p) (project_pred p')
    | q -> { node = map_query project project_pred q; meta = () }

and project_pred p = map_pred project project_pred p

and no_project r =
  { node = map_query no_project project_pred r.node; meta = () }

let project_once r = Cardinality.annotate r |> project

let project ?(params = Set.empty (module Name)) ?(max_iters = 10) r =
  let rec loop ct r =
    if ct >= max_iters then r
    else
      let r' = Resolve.resolve r ~params |> project_once in
      if [%compare.equal: _ annot] r r' then r' else loop (ct + 1) r'
  in
  loop 0 (A.strip_meta r)
