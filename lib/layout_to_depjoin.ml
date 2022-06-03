open Core
open Ast
module V = Visitors
module S = Schema
module A = Constructors.Annot

let list l = { d_lhs = l.l_keys; d_rhs = l.l_values }

let dedup_names =
  List.stable_dedup_staged ~compare:(fun n n' ->
      [%compare: String.t] (Name.name n) (Name.name n'))
  |> Staged.unstage

let hash_idx h =
  let rk_schema = S.schema h.hi_keys |> S.zero
  and rv_schema = S.schema h.hi_values in
  let key_pred =
    List.map2_exn rk_schema h.hi_lookup ~f:(fun p1 p2 ->
        `Binop (Binop.Eq, `Name p1, p2))
    |> Pred.conjoin
  and slist = rk_schema @ rv_schema |> dedup_names |> Select_list.of_names in
  {
    d_lhs = strip_meta h.hi_keys;
    d_rhs = A.select slist (A.filter key_pred h.hi_values);
  }

let ordered_idx { oi_keys = rk; oi_values = rv; oi_lookup; _ } =
  let rk = strip_meta rk and rv = strip_meta rv in
  let rk_schema = S.schema rk and rv_schema = S.schema rv in
  let key_pred =
    List.zip_exn rk_schema oi_lookup
    |> List.concat_map ~f:(fun (n, (lb, ub)) ->
           let p1 =
             Option.map lb ~f:(fun (p, b) ->
                 match b with
                 | `Closed -> [ `Binop (Binop.Ge, `Name n, p) ]
                 | `Open -> [ `Binop (Gt, `Name n, p) ])
             |> Option.value ~default:[]
           in
           let p2 =
             Option.map ub ~f:(fun (p, b) ->
                 match b with
                 | `Closed -> [ `Binop (Binop.Le, `Name n, p) ]
                 | `Open -> [ `Binop (Lt, `Name n, p) ])
             |> Option.value ~default:[]
           in
           p1 @ p2)
    |> Pred.conjoin
  and slist = rk_schema @ rv_schema |> dedup_names |> Select_list.of_names in
  { d_lhs = rk; d_rhs = A.select slist (A.filter key_pred rv) }

let cross_tuple ts =
  let scalars, others =
    List.partition_map ts ~f:(fun r ->
        match r.node with
        | AScalar p -> First (p.s_pred, p.s_name)
        | _ -> Second r)
  in
  let base_relation, base_schema =
    match List.reduce others ~f:(A.join (`Bool true)) with
    | Some r -> (r, S.schema r)
    | None -> (A.scalar (`Int 0) (Fresh.name Global.fresh "x%d"), [])
  in
  let select_list = scalars @ S.to_select_list base_schema in
  Select (select_list, base_relation)

let rec annot r = { r with node = query r.node }

and query = function
  | AOrderedIdx o -> DepJoin (ordered_idx (V.Map.ordered_idx annot pred o))
  | AHashIdx h -> DepJoin (hash_idx (V.Map.hash_idx annot pred h))
  | AList l -> DepJoin (list @@ V.Map.list annot l)
  | ATuple (ts, Cross) -> cross_tuple (List.map ~f:annot ts)
  | q -> V.Map.query annot pred q

and pred p = V.Map.pred annot pred p
