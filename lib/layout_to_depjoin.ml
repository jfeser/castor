open Abslayout_visitors
open Schema
open Abslayout

let strip_meta_pred q = map_meta_pred (fun _ -> ()) q

let list rk rv =
  let scope = scope_exn rk in
  { d_lhs = strip_scope rk; d_alias = scope; d_rhs = rv }

let hash_idx h =
  let rk_schema = schema h.hi_keys |> scoped h.hi_scope in
  let rv_schema = schema h.hi_values in
  let key_pred =
    List.map2_exn rk_schema h.hi_lookup ~f:(fun p1 p2 ->
        Binop (Eq, Name p1, strip_meta_pred p2))
    |> Pred.conjoin
  in
  let slist = rk_schema @ rv_schema |> List.map ~f:(fun n -> Name n) in
  {
    d_lhs = strip_meta h.hi_keys;
    d_alias = h.hi_scope;
    d_rhs =
      strip_meta @@ select slist (filter key_pred @@ strip_meta h.hi_values);
  }

let ordered_idx rk rv m =
  let rk = strip_meta rk in
  let rv = strip_meta rv in
  let scope = scope_exn rk in
  let rk_schema = schema rk in
  let rv_schema = schema rv in
  let key_pred =
    let rk_schema = schema rk in
    List.zip_exn rk_schema m.oi_lookup
    |> List.concat_map ~f:(fun (n, (lb, ub)) ->
           let p1 =
             Option.map lb ~f:(fun (p, b) ->
                 match b with
                 | `Closed -> [ Binop (Ge, Name n, strip_meta_pred p) ]
                 | `Open -> [ Binop (Gt, Name n, strip_meta_pred p) ])
             |> Option.value ~default:[]
           in
           let p2 =
             Option.map ub ~f:(fun (p, b) ->
                 match b with
                 | `Closed -> [ Binop (Le, Name n, strip_meta_pred p) ]
                 | `Open -> [ Binop (Lt, Name n, strip_meta_pred p) ])
             |> Option.value ~default:[]
           in
           p1 @ p2)
    |> Pred.conjoin
  in
  let slist = rk_schema @ rv_schema |> List.map ~f:(fun n -> Name n) in
  {
    d_lhs = strip_scope rk;
    d_alias = scope;
    d_rhs = select slist (filter key_pred rv);
  }

let rec annot r = { r with node = query r.node }

and query = function
  | AOrderedIdx (rk, rv, m) ->
      DepJoin (ordered_idx (annot rk) (annot rv) (map_ordered_idx annot pred m))
  | AHashIdx h -> DepJoin (hash_idx (map_hash_idx annot pred h))
  | AList (rk, rv) -> DepJoin (list (annot rk) (annot rv))
  | q -> map_query annot pred q

and pred p = map_pred annot pred p
