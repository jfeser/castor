open Core
open Ast
module V = Visitors

let annotate_stage r =
  let incr = List.map ~f:(fun (n, s) -> (Name.incr n, s)) in
  let schema r = r.meta#schema in
  let comptime = List.map ~f:(fun n -> (Name.zero n, `Compile)) in
  let runtime = List.map ~f:(fun n -> (Name.zero n, `Run)) in
  let rec annot stage r =
    {
      node = query stage r.node;
      meta =
        object
          method meta = r.meta
          method stage = stage
        end;
    }
  and query stage = function
    | AHashIdx x ->
        AHashIdx
          {
            hi_keys = annot stage x.hi_keys;
            hi_values =
              annot (incr stage @ comptime (schema x.hi_keys)) x.hi_values;
            hi_key_layout = Option.map x.hi_key_layout ~f:(annot stage);
            hi_lookup = List.map x.hi_lookup ~f:(pred stage);
          }
    | AOrderedIdx x ->
        AOrderedIdx
          {
            oi_keys = annot stage x.oi_keys;
            oi_values =
              annot (incr stage @ comptime (schema x.oi_keys)) x.oi_values;
            oi_key_layout = Option.map x.oi_key_layout ~f:(annot stage);
            oi_lookup =
              List.map x.oi_lookup ~f:(fun (b, b') ->
                  ( Option.map ~f:(V.Map.bound (pred stage)) b,
                    Option.map ~f:(V.Map.bound (pred stage)) b' ));
          }
    | AList x ->
        AList
          {
            l_keys = annot stage x.l_keys;
            l_values =
              annot (incr stage @ comptime (schema x.l_keys)) x.l_values;
          }
    | DepJoin x ->
        DepJoin
          {
            d_lhs = annot stage x.d_lhs;
            d_rhs = annot (incr stage @ runtime (schema x.d_lhs)) x.d_rhs;
          }
    | q -> V.Map.query (annot stage) (pred stage) q
  and pred stage p = V.Map.pred (annot stage) (pred stage) p in
  annot [] r

let is_static r =
  Free.annot r
  |> Set.for_all ~f:(fun n ->
         match
           List.find r.meta#stage ~f:(fun (n', _) -> [%equal: Name.t] n n')
         with
         | Some (_, `Compile) -> true
         | Some (_, `Run) | None -> false)
