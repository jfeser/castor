open Ast
module V = Visitors
open Collections
module A = Abslayout
module P = Pred.Infix

let dummy_select = (`Bool false, "dummy")
let dummy = A.scalar (`Bool false) "dummy"

let project_def refcnt (_, n) =
  (* Filter out definitions that are never referenced. *)
  match Map.find refcnt @@ Name.create n with
  | Some c -> c
  | None ->
      (* Be conservative if refcount is missing. *)
      true

let project_defs refcnt ps =
  let ps = List.filter ps ~f:(project_def refcnt) in
  if List.is_empty ps then [ dummy_select ] else ps

(** True if all fields emitted by r are unreferenced when emitted by r'. *)
let all_unref_at r r' =
  Schema.schema r |> Schema.unscoped
  |> List.for_all ~f:(fun n ->
         match Map.find r'.meta#meta#refs n with
         | Some c -> not c
         | None -> false)

(** True if all fields emitted by r are unreferenced. *)
let all_unref r = all_unref_at r r

let project_list project no_project
    { l_keys = rk; l_values = rv; l_scope = scope } =
  let rk =
    let old_n = Schema.schema rk |> List.length in
    let ps =
      Schema.schema rk |> Schema.to_select_list
      |> project_defs rk.meta#meta#refs
    in
    let new_n = List.length ps in
    if old_n > new_n then A.select ps (no_project rk) else project rk
  in
  A.list rk scope (project rv)

let project_select_list project_pred refs ps =
  project_defs refs ps |> List.map ~f:(fun (p, n) -> (project_pred p, n))

let project_open project no_project project_pred r =
  let refs = r.meta#meta#refs in
  let card_matters = r.meta#cardinality_matters in
  if all_unref r && not card_matters then dummy
  else
    match r.node with
    | Select (ps, r) ->
        A.select (project_select_list project_pred refs ps) (project r)
    | GroupBy (ps, ns, r) ->
        A.group_by (project_select_list project_pred refs ps) ns (project r)
    | Dedup r ->
        if card_matters then A.dedup @@ no_project r else A.dedup @@ project r
    | AList l -> project_list project no_project l
    | AScalar s ->
        if project_def refs (s.s_pred, s.s_name) then
          A.scalar' { s with s_pred = project_pred s.s_pred }
        else dummy
    | ATuple ([], _) -> A.empty
    | ATuple ([ r ], _) -> project r
    | ATuple (rs, Concat) -> A.tuple (List.map rs ~f:project) Concat
    | ATuple (rs, Cross) ->
        let rs =
          (* Remove unreferenced parts of the tuple. *)
          List.filter rs ~f:(fun r ->
              let is_unref = all_unref r
              and is_scalar =
                match r.node with AScalar _ -> true | _ -> false
              in
              not (is_unref && is_scalar))
          |> List.map ~f:project
        in
        let rs = if List.length rs = 0 then [ dummy ] else rs in
        A.tuple rs Cross
    | Join { r1; r2; pred } ->
        if card_matters then
          A.join (project_pred pred) (project r1) (project r2)
        else if
          (* `If one side of a join is unused then the join can be dropped. *)
          all_unref_at r1 r
        then project r2
        else if all_unref_at r2 r then project r1
        else A.join (project_pred pred) (project r1) (project r2)
    | DepJoin { d_lhs; d_rhs; d_alias } ->
        if card_matters then A.dep_join (project d_lhs) d_alias (project d_rhs)
        else if
          (* `If one side of a join is unused then the join can be dropped. *)
          all_unref d_lhs
        then project d_rhs
        else if all_unref d_rhs then dummy
        else A.dep_join (project d_lhs) d_alias (project d_rhs)
    | Range (p, p') -> A.range (project_pred p) (project_pred p')
    | q -> { node = V.Map.query project project_pred q; meta = object end }

let project_pred_open project project_pred p =
  match p with
  | `Exists _ | `First _ -> Pred.strip_meta p
  | p -> V.Map.pred project project_pred p

(** Apply no projection until a select is reached, under which projection resumes. *)
let no_project_open project no_project project_pred r =
  match r.node with
  | Select (ps, r) -> A.select (ps :> Pred.t Select_list.t) (project r)
  | AList l -> project_list no_project no_project l
  | _ ->
      { node = V.Map.query no_project project_pred r.node; meta = object end }

let project_once ~params r =
  let rec project r = project_open project no_project project_pred r
  and project_pred p = project_pred_open project project_pred p
  and no_project r = no_project_open project no_project project_pred r in
  let r' = Cardinality.annotate r |> project in
  Check.schema r r';
  Check.resolve ~params r r';
  r'

let project ~params ?(max_iters = 100) r =
  let rec loop ct r =
    if ct >= max_iters then r
    else
      let r' = Resolve.resolve_exn r ~params |> project_once ~params in
      if [%equal: _ annot] r r' then r' else loop (ct + 1) r'
  in
  loop 0 (strip_meta r)

