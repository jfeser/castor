open Core
open Ast

let rec shift = function
  | `Name n -> `Name (Name.incr n)
  | p -> Visitors.Map.pred Fun.id shift p

let incr ctx =
  Map.to_alist ctx
  |> List.map ~f:(fun (n, p) -> (Name.incr n, shift p))
  |> Map.of_alist_exn (module Name)

let rec subst ctx r = { r with node = subst_query ctx r.node }

and subst_query ctx = function
  | DepJoin x ->
      DepJoin { d_lhs = subst ctx x.d_lhs; d_rhs = subst (incr ctx) x.d_rhs }
  | AList x ->
      AList
        { l_keys = subst ctx x.l_keys; l_values = subst (incr ctx) x.l_values }
  | AOrderedIdx x ->
      AOrderedIdx
        {
          oi_keys = subst ctx x.oi_keys;
          oi_values = subst (incr ctx) x.oi_values;
          oi_key_layout = Option.map x.oi_key_layout ~f:(subst ctx);
          oi_lookup = subst_lookup ctx x.oi_lookup;
        }
  | AHashIdx x ->
      AHashIdx
        {
          hi_keys = subst ctx x.hi_keys;
          hi_values = subst (incr ctx) x.hi_values;
          hi_key_layout = Option.map x.hi_key_layout ~f:(subst ctx);
          hi_lookup = List.map x.hi_lookup ~f:(subst_pred ctx);
        }
  | q -> Visitors.Map.query (subst ctx) (subst_pred ctx) q

and subst_lookup ctx =
  List.map ~f:(fun (l, h) ->
      (Option.map l ~f:(subst_bound ctx), Option.map h ~f:(subst_bound ctx)))

and subst_bound ctx (p, b) = (subst_pred ctx p, b)

and subst_pred ctx = function
  | `Name n as p -> Map.find ctx n |> Option.value ~default:p
  | p -> Visitors.Map.pred Fun.id (subst_pred ctx) p
