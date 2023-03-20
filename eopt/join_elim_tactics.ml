open Core
module Subst = Castor.Subst
module Schema = Castor.Schema
module Name = Castor.Name
module Free = Castor.Free
module Abslayout = Castor.Abslayout
module Pred = Castor.Pred
module Select_list = Castor.Select_list
module Fresh = Castor.Fresh
module Global = Castor.Global
module Egraph = Castor.Egraph
module P = Pred.Infix
module A = Abslayout
open Egraph_matcher

let elim_join_nest g _ =
  let%map root, join = M.any_join g in
  let lhs_schema = G.schema g join.r1 in
  let pred =
    Subst.subst_pred
      (List.map lhs_schema ~f:(fun n -> (n, `Name (Name.zero n)))
      |> Map.of_alist_exn (module Name))
      join.pred
  in
  let lhs = C.tuple g (List.map lhs_schema ~f:(C.scalar_name g)) Cross
  and rhs = C.filter g pred (of_annot g @@ Subst.incr @@ to_annot g join.r2) in
  (root, C.depjoin g { d_lhs = join.r1; d_rhs = C.tuple g [ lhs; rhs ] Cross })

let elim_join_hash g _ =
  let%bind root, join = M.any_join g in
  match join.pred with
  | `Binop (Eq, kl, kr) ->
      let r1_schema = G.schema g join.r1 in
      let kl =
        Subst.incr_pred kl
        |> Subst.subst_pred
             (Map.of_alist_exn (module Name)
             @@ List.map r1_schema ~f:(fun n -> (n, `Name (Name.zero n))))
      in
      let layout =
        let slist =
          Select_list.of_names
            (List.map ~f:Name.zero r1_schema @ G.schema g join.r2)
        in
        C.depjoin g
          {
            d_lhs = join.r1;
            d_rhs =
              C.select g slist
              @@ C.hash_idx g
                   {
                     hi_keys =
                       C.dedup g
                       @@ C.select g [ (kr, "key") ]
                       @@ of_annot g @@ Subst.incr @@ to_annot g join.r2;
                     hi_values =
                       C.filter g
                         (`Binop
                           (Eq, `Name (Name.zero @@ Name.create "key"), kr))
                       @@ of_annot g @@ Subst.incr @@ Subst.incr
                       @@ to_annot g join.r2;
                     hi_lookup = [ kl ];
                     hi_key_layout = None;
                   };
          }
      in
      return (root, layout)
  | _ -> empty

let elim_join_filter g _ =
  let%map root, join = M.any_join g in
  (root, C.filter g join.pred (C.join g (`Bool true) join.r1 join.r2))

(* let hoist_join_param_filter r = *)
(*   let open Option.Let_syntax in *)
(*   let%bind pred, r1, r2 = to_join r in *)
(*   let has_params p = *)
(*     not (Set.is_empty @@ Set.inter (Free.pred_free p) C.params) *)
(*   in *)
(*   let hoist, keep = Pred.conjuncts pred |> List.partition_tf ~f:has_params in *)
(*   if List.is_empty hoist then None *)
(*   else *)
(*     return @@ A.filter (Pred.conjoin hoist) @@ A.join (Pred.conjoin keep) r1 r2 *)

(* let hoist_join_param_filter = *)
(*   of_func hoist_join_param_filter ~name:"hoist-join-param-filter" *)

(* let hoist_join_filter r = *)
(*   let open Option.Let_syntax in *)
(*   let%bind pred, r1, r2 = to_join r in *)
(*   return @@ A.filter pred @@ A.join (`Bool true) r1 r2 *)

(* let hoist_join_filter = of_func hoist_join_filter ~name:"hoist-join-filter" *)

(* let push_join_filter r = *)
(*   let open Option.Let_syntax in *)
(*   let stage = r.meta#stage in *)
(*   let r = strip_meta r in *)
(*   let%bind p, r, r' = to_join r in *)
(*   let s = Schema.schema r |> Set.of_list (module Name) *)
(*   and s' = Schema.schema r' |> Set.of_list (module Name) in *)
(*   let left, right, above = *)
(*     Pred.conjuncts p *)
(*     |> List.partition3_map ~f:(fun p -> *)
(*            if Tactics_util.is_supported stage s p then `Fst p *)
(*            else if Tactics_util.is_supported stage s' p then `Snd p *)
(*            else `Trd p) *)
(*   in *)
(*   if List.is_empty left && List.is_empty right then None *)
(*   else *)
(*     let r = if List.is_empty left then r else A.filter (Pred.conjoin left) r *)
(*     and r' = *)
(*       if List.is_empty right then r' else A.filter (Pred.conjoin right) r' *)
(*     in *)
(*     return @@ A.join (Pred.conjoin above) r r' *)

(* let push_join_filter = *)
(*   seq' *)
(*     (of_func_cond ~name:"push-join-filter" *)
(*        ~pre:(fun r -> Some (Resolve.resolve_exn ~params r)) *)
(*        push_join_filter *)
(*        ~post:(fun r -> Resolve.resolve ~params r |> Result.ok)) *)
(*     simplify *)

(* let split_out path pk = *)
(*   let open A in *)
(*   let pk = A.name_of_string_exn pk in *)
(*   let eq = [%compare.equal: Name.t] in *)
(*   let fresh_name = Fresh.name Global.fresh in *)

(*   of_func ~name:"split-out" @@ fun r -> *)
(*   let open Option.Let_syntax in *)
(*   let%bind path = path r in *)
(*   let rel = Path.get_exn path r in *)
(*   let rel_schema = Schema.schema rel in *)

(*   let schema = schema r in *)
(*   if List.mem schema pk ~equal:eq then *)
(*     let scope = fresh_name "s%d" in *)
(*     let alias = Name.scoped scope @@ Name.fresh "x%d" in *)
(*     (\* Partition the schema of the original layout between the two new layouts. *\) *)
(*     let fst_sel_list, snd_sel_list = *)
(*       List.partition_tf schema ~f:(fun n -> *)
(*           eq n pk *)
(*           || not *)
(*                (List.mem rel_schema n ~equal:(fun n n' -> *)
(*                     String.(Name.(name n = name n'))))) *)
(*     in *)
(*     let scope2 = fresh_name "s%d" in *)
(*     Option.return *)
(*     @@ dep_join *)
(*          (select (Schema.to_select_list fst_sel_list) r) *)
(*          scope2 *)
(*          (hash_idx *)
(*             (dedup (select [ (`Name pk, Name.name alias) ] rel)) *)
(*             scope *)
(*             (select *)
(*                (Select_list.of_names *)
(*                   (List.map fst_sel_list ~f:(fun n -> Name.scoped scope2 n) *)
(*                   @ snd_sel_list)) *)
(*                (filter (`Binop (Eq, `Name pk, `Name alias)) rel)) *)
(*             [ `Name (Name.scoped scope2 pk) ]) *)
(*   else None *)

(* (\* let hoist_join_left r = *)
(*  *   let open Option.Let_syntax in *)
(*  *   let%bind p1, r1, r2 = to_join r in *)
(*  *   let%bind p2, r3, r4 = to_join r1 in *)
(*  *   return A.join p2 (A.join p1) *)
(*  *   let has_params p = *)
(*  *     not (Set.is_empty @@ Set.inter (Free.pred_free p) C.params) *)
(*  *   in *)
(*  *   let hoist, keep = Pred.conjuncts pred |> List.partition_tf ~f:has_params in *)
(*  *   if List.is_empty hoist then None *)
(*  *   else *)
(*  *     return *)
(*  *     @@ A.filter (Pred.conjoin hoist) *)
(*  *     @@ A.join (Pred.conjoin keep) r1 r2 *\) *)

let () =
  Ops.register elim_join_nest "elim-join-nest";
  Ops.register elim_join_hash "elim-join-hash";
  Ops.register elim_join_hash "elim-join-filter"
