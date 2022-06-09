open Core
open Ast

let decr_pred p =
  let exception Decr in
  try
    Some
      (Pred.map_names
         ~f:(fun n ->
           match Name.decr n with Some n' -> `Name n' | None -> raise Decr)
         p)
  with Decr -> None

let decr_pred_exn p = Pred.map_names ~f:(fun n -> `Name (Name.decr_exn n)) p

let rec shift ~cutoff d r = { r with node = shift_query ~cutoff d r.node }

and shift_query ~cutoff:c d =
  let shift c = shift ~cutoff:c d in
  let shift_pred c = shift_pred ~cutoff:c d in
  function
  | DepJoin x ->
      DepJoin { d_lhs = shift c x.d_lhs; d_rhs = shift (c + 1) x.d_rhs }
  | AList x ->
      AList { l_keys = shift c x.l_keys; l_values = shift (c + 1) x.l_values }
  | AOrderedIdx x ->
      AOrderedIdx
        {
          oi_keys = shift c x.oi_keys;
          oi_values = shift (c + 1) x.oi_values;
          oi_key_layout = Option.map x.oi_key_layout ~f:(shift c);
          oi_lookup = shift_lookup ~cutoff:c d x.oi_lookup;
        }
  | AHashIdx x ->
      AHashIdx
        {
          hi_keys = shift c x.hi_keys;
          hi_values = shift (c + 1) x.hi_values;
          hi_key_layout = Option.map x.hi_key_layout ~f:(shift c);
          hi_lookup = List.map x.hi_lookup ~f:(shift_pred c);
        }
  | q -> Visitors.Map.query (shift c) (shift_pred c) q

and shift_lookup ~cutoff d =
  List.map ~f:(fun (l, h) ->
      ( Option.map l ~f:(shift_bound ~cutoff d),
        Option.map h ~f:(shift_bound ~cutoff d) ))

and shift_bound ~cutoff d (p, b) = (shift_pred ~cutoff d p, b)

and shift_pred ~cutoff d = function
  | `Name n -> `Name (Name.shift ~cutoff d n)
  | p -> Visitors.Map.pred Fun.id (shift_pred ~cutoff d) p

let incr x = shift ~cutoff:0 1 x
let incr_pred x = shift_pred ~cutoff:0 1 x

let incr_ctx ctx =
  Map.to_alist ctx
  |> List.map ~f:(fun (n, p) -> (Name.incr n, incr_pred p))
  |> Map.of_alist_exn (module Name)

let rec subst ctx r = { r with node = subst_query ctx r.node }

and subst_query ctx = function
  | DepJoin x ->
      DepJoin
        { d_lhs = subst ctx x.d_lhs; d_rhs = subst (incr_ctx ctx) x.d_rhs }
  | AList x ->
      AList
        {
          l_keys = subst ctx x.l_keys;
          l_values = subst (incr_ctx ctx) x.l_values;
        }
  | AOrderedIdx x ->
      AOrderedIdx
        {
          oi_keys = subst ctx x.oi_keys;
          oi_values = subst (incr_ctx ctx) x.oi_values;
          oi_key_layout = Option.map x.oi_key_layout ~f:(subst ctx);
          oi_lookup = subst_lookup ctx x.oi_lookup;
        }
  | AHashIdx x ->
      AHashIdx
        {
          hi_keys = subst ctx x.hi_keys;
          hi_values = subst (incr_ctx ctx) x.hi_values;
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
