open Visitors
open Collections
module A = Abslayout
module P = Pred.Infix

module Config = struct
  module type S = sig
    include Ops.Config.S

    include Tactics_util.Config.S
  end
end

module Make (C : Config.S) = struct
  module Ops = Ops.Make (C)

  open Tactics_util.Make (C)

  open Ops

  let src = Logs.Src.create "groupby-tactics"

  (** Remove all references to names in params while ensuring that the resulting
     relation overapproximates the original. *)
  let over_approx params r =
    let visitor =
      object (self)
        inherit [_] map as super

        method! visit_Filter () (p, r) =
          if Set.is_empty (Set.inter (Pred.names p) params) then
            super#visit_Filter () (p, r)
          else (self#visit_t () r).node

        method! visit_Select () (ps, r) =
          match A.select_kind ps with
          | `Agg -> Select (ps, r)
          | `Scalar -> Select (ps, self#visit_t () r)

        method! visit_GroupBy () (ps, ks, r) = GroupBy (ps, ks, r)
      end
    in
    let r = visitor#visit_t () r in
    let remains = Set.inter (Free.free r) params in
    if Set.is_empty remains then Ok r
    else
      Or_error.error "Failed to remove all parameters." remains
        [%sexp_of: Set.M(Name).t]

  let elim_groupby r =
    match r.node with
    | GroupBy (ps, key, r) -> (
        let key_name = Fresh.name Global.fresh "k%d" in
        let key_preds = List.map key ~f:P.name in
        let filter_pred =
          List.map key ~f:(fun n ->
              Pred.Infix.(name n = name (Name.copy n ~scope:(Some key_name))))
          |> Pred.conjoin
        in
        let keys = A.dedup (A.select key_preds r) in
        (* Try to remove any remaining parameters from the keys relation. *)
        match over_approx C.params keys with
        | Ok keys ->
            Some (A.list keys key_name (A.select ps (A.filter filter_pred r)))
        | Error err ->
            Logs.info ~src (fun m -> m "elim-groupby: %a" Error.pp err);
            None )
    (* Otherwise, if some keys are computed, fail. *)
    | _ -> None

  let elim_groupby = of_func elim_groupby ~name:"elim-groupby"

  (** Eliminate a group by operator without representing duplicate key values. *)
  let elim_groupby_flat r =
    match r.node with
    | GroupBy (ps, key, r) -> (
        let key_name = Fresh.name Global.fresh "k%d" in
        let key_preds = List.map key ~f:P.name in
        let filter_pred =
          List.map key ~f:(fun n ->
              Pred.Infix.(name n = name (Name.copy n ~scope:(Some key_name))))
          |> Pred.conjoin
        in
        let keys = A.dedup (A.select key_preds r) in
        (* Try to remove any remaining parameters from the keys relation. *)
        match over_approx C.params keys with
        | Ok keys ->
            let scalars, rest =
              let schema = Schema.schema r in
              List.partition_tf ps ~f:(fun p ->
                  match Pred.to_name p with
                  | Some n -> List.mem schema n ~equal:[%compare.equal: Name.t]
                  | None -> false)
            in
            let scalars =
              List.map scalars ~f:(fun p ->
                  A.scalar @@ Pred.scoped key key_name p)
            in
            Option.return @@ A.list keys key_name
            @@ A.tuple
                 (scalars @ [ A.select rest (A.filter filter_pred r) ])
                 Cross
        | Error err ->
            Logs.info ~src (fun m -> m "elim-groupby: %a" Error.pp err);
            None )
    (* Otherwise, if some keys are computed, fail. *)
    | _ -> None

  let elim_groupby_flat = of_func elim_groupby_flat ~name:"elim-groupby-flat"

  let elim_groupby_approx r =
    match r.node with
    | GroupBy (ps, key, r) -> (
        let key_name = Fresh.name Global.fresh "k%d" in
        let key_preds = List.map key ~f:P.name in
        let filter_pred =
          List.map key ~f:(fun n ->
              Pred.Infix.(name n = name (Name.copy n ~scope:(Some key_name))))
          |> Pred.conjoin
        in
        (* Try to remove any remaining parameters from the keys relation. *)
        match all_values_approx key_preds r with
        | Ok keys ->
            Some (A.list keys key_name (A.select ps (A.filter filter_pred r)))
        | Error err ->
            Logs.info ~src (fun m -> m "elim-groupby-approx: %a" Error.pp err);
            None )
    (* Otherwise, if some keys are computed, fail. *)
    | _ -> None

  let elim_groupby_approx =
    of_func elim_groupby_approx ~name:"elim-groupby-approx"

  let db_relation n = A.relation (Db.relation C.conn n)

  let elim_groupby_partial =
    let open A in
    Branching.local ~name:"elim-groupby-partial" (function
      | { node = GroupBy (ps, key, r); _ } ->
          Seq.of_list key
          |> Seq.map ~f:(fun k ->
                 let key_name = Fresh.name Global.fresh "k%d" in
                 let scope = Fresh.name Global.fresh "s%d" in
                 let rel =
                   (Option.value_exn
                      (Db.relation_has_field C.conn (Name.name k)))
                     .r_name
                 in
                 let lhs =
                   dedup
                     (select [ As_pred (Name k, key_name) ] (db_relation rel))
                 in
                 let new_key =
                   List.filter key ~f:(fun k' -> Name.O.(k <> k'))
                 in
                 let new_ps =
                   List.filter ps ~f:(fun p ->
                       not ([%compare.equal: Pred.t] p (Name k)))
                   |> List.map ~f:(Pred.scoped (Schema.schema lhs) scope)
                 in
                 let filter_pred =
                   Binop (Eq, Name k, Name (Name.create key_name))
                   |> Pred.scoped (Schema.schema lhs) scope
                 in
                 let new_r =
                   replace_rel rel (filter filter_pred (db_relation rel)) r
                 in
                 let new_group_by =
                   if List.is_empty new_key then select new_ps new_r
                   else group_by new_ps new_key new_r
                 in
                 let key_scalar =
                   let p =
                     Pred.scoped (Schema.schema lhs) scope
                       (As_pred (Name (Name.create key_name), Name.name k))
                   in
                   scalar p
                 in
                 list lhs scope (tuple [ key_scalar; new_group_by ] Cross))
      | _ -> Seq.empty)
end
