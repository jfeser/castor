open Core
open Ast
module V = Visitors
(* module A = Abslayout *)

(* class ['a] stage_iter = *)
(*   object (self : 'a) *)
(*     inherit [_] V.iter *)

(*     method! visit_AList (ctx, phase) { l_keys = rk; l_values = rv; _ } = *)
(*       self#visit_t (ctx, `Compile) rk; *)
(*       self#visit_t (ctx, phase) rv *)

(*     method! visit_AHashIdx (ctx, phase) h = *)
(*       List.iter h.hi_lookup ~f:(self#visit_pred (ctx, phase)); *)
(*       self#visit_t (ctx, `Compile) h.hi_keys; *)
(*       self#visit_t (ctx, phase) h.hi_values *)

(*     method! visit_AOrderedIdx (ctx, phase) *)
(*         { oi_keys = rk; oi_values = rv; oi_lookup; _ } = *)
(*       let bound_iter = *)
(*         Option.iter ~f:(fun (p, _) -> self#visit_pred (ctx, phase) p) *)
(*       in *)
(*       List.iter oi_lookup ~f:(fun (b1, b2) -> *)
(*           bound_iter b1; *)
(*           bound_iter b2); *)
(*       self#visit_t (ctx, `Compile) rk; *)
(*       self#visit_t (ctx, phase) rv *)

(*     method! visit_AScalar (ctx, _) p = self#visit_pred (ctx, `Compile) p.s_pred *)
(*   end *)

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
  Free.free r
  |> Set.for_all ~f:(fun n ->
         match
           List.find r.meta#stage ~f:(fun (n', _) -> [%equal: Name.t] n n')
         with
         | Some (_, `Compile) -> true
         | Some (_, `Run) | None -> false)

(* exception Un_serial of string *)

(* class ['a] ops_serializable_visitor = *)
(*   object *)
(*     inherit ['a] stage_iter as super *)

(*     method! visit_t ((), s) r = *)
(*       super#visit_t ((), s) r; *)
(*       match (s, r.node) with *)
(*       | `Run, (Relation _ | GroupBy (_, _, _) | Join _ | OrderBy _ | Dedup _) -> *)
(*           raise *)
(*           @@ Un_serial *)
(*                (Format.asprintf *)
(*                   "Cannot serialize: Bad operator in run-time position %a" A.pp *)
(*                   r) *)
(*       | _ -> () *)
(*   end *)

(* class ['a] names_serializable_visitor stage = *)
(*   object *)
(*     inherit ['a] stage_iter as super *)

(*     method! visit_Name (_, s) n = *)
(*       match (stage n, s) with *)
(*       | `Compile, `Run | `Run, `Compile -> *)
(*           let stage = match s with `Compile -> "compile" | `Run -> "run" in *)
(*           let msg = *)
(*             Fmt.str "Cannot serialize: Found %a in %s time position." Name.pp n *)
(*               stage *)
(*           in *)
(*           raise @@ Un_serial msg *)
(*       | _ -> () *)

(*     method! visit_t (_, s) = super#visit_t ((), s) *)
(*   end *)

(* (\** Return true if `r` is serializable. This function performs two checks: *)
(*     - `r` must not contain any compile time only operations in run time position. *)
(*     - Run-time names may only appear in run-time position and vice versa. *\) *)
(* let is_serializeable ?(path = Path.root) ?params r = *)
(*   try *)
(*     (new ops_serializable_visitor)#visit_t ((), `Run) @@ Path.get_exn path r; *)
(*     let stage = stage ?params r in *)
(*     (new names_serializable_visitor stage)#visit_t ((), `Run) *)
(*     @@ Path.get_exn path r; *)
(*     Ok () *)
(*   with Un_serial msg -> Error msg *)

(* class ['a] ops_spine_serializable_visitor = *)
(*   object *)
(*     inherit ['a] ops_serializable_visitor *)
(*     method! visit_Exists _ _ = () *)
(*     method! visit_First _ _ = () *)
(*   end *)

(* class ['a] names_spine_serializable_visitor stage = *)
(*   object *)
(*     inherit ['a] names_serializable_visitor stage *)
(*     method! visit_Exists _ _ = () *)
(*     method! visit_First _ _ = () *)
(*   end *)

(* (\** Return true if the spine of r (the part of the query with no subqueries) is *)
(*    serializable. *\) *)
(* let is_spine_serializeable ?(path = Path.root) ?params r = *)
(*   try *)
(*     (new ops_spine_serializable_visitor)#visit_t ((), `Run) *)
(*     @@ Path.get_exn path r; *)
(*     let stage = stage ?params r in *)
(*     (new names_spine_serializable_visitor stage)#visit_t ((), `Run) *)
(*     @@ Path.get_exn path r; *)
(*     Ok () *)
(*   with Un_serial msg -> Error msg *)
