open Core
open Visitors
open Collections
open Match.Query
module A = Abslayout
module P = Pred.Infix
include (val Log.make ~level:(Some Warning) "castor-opt.groupby-tactics")

module Config = struct
  module type S = sig
    include Ops.Config.S
    include Tactics_util.Config.S
  end
end

module Make (C : Config.S) = struct
  open Ops.Make (C)
  open Tactics_util.Make (C)

  (** Eliminate a group by operator without representing duplicate key values. *)
  let elim_groupby_flat r =
    match r.node with
    | GroupBy (ps, key, r) -> (
        let key_name = Fresh.name Global.fresh "k%d" in
        let filter_pred =
          List.map key ~f:(fun n ->
              Pred.Infix.(name n = name (Name.copy n ~scope:(Some key_name))))
          |> Pred.conjoin
        in
        (* Try to remove any remaining parameters from the keys relation. *)
        match
          over_approx C.params (A.dedup (A.select (Select_list.of_names key) r))
        with
        | Ok keys ->
            let schema = Schema.schema r in
            let scalars, rest =
              List.partition_map ps ~f:(fun ((p, _) as s) ->
                  match p with
                  | Name n
                    when List.mem schema n ~equal:[%compare.equal: Name.t] ->
                      First n
                  | _ -> Second s)
            in
            let scalars = List.map scalars ~f:A.scalar_name in
            Option.return @@ A.list keys key_name
            @@ A.tuple
                 (scalars @ [ A.select rest (A.filter filter_pred r) ])
                 Cross
        | Error err ->
            info (fun m -> m "elim-groupby: %a" Error.pp err);
            None)
    (* Otherwise, if some keys are computed, fail. *)
    | _ -> None

  let elim_groupby_flat = of_func elim_groupby_flat ~name:"elim-groupby-flat"

  let elim_groupby_approx r =
    let open Option.Let_syntax in
    let%bind ps, key, r = to_groupby r in
    let key_name = Fresh.name Global.fresh "k%d" in
    let filter_pred =
      List.map key ~f:(fun n ->
          Pred.Infix.(name n = name (Name.copy n ~scope:(Some key_name))))
      |> Pred.conjoin
    in
    (* Try to remove any remaining parameters from the keys relation. *)
    let%bind keys =
      match all_values_approx (Select_list.of_names key) r with
      | Ok keys -> return @@ A.dedup keys
      | Error err ->
          (* Otherwise, if some keys are computed, fail. *)
          info (fun m -> m "elim-groupby-approx: %a" Error.pp err);
          None
    in
    return @@ A.list keys key_name (A.select ps (A.filter filter_pred r))

  let elim_groupby_approx =
    of_func elim_groupby_approx ~name:"elim-groupby-approx"

  let elim_groupby = elim_groupby_approx
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
                   dedup (select [ (Name k, key_name) ] (db_relation rel))
                 in
                 let new_key =
                   List.filter key ~f:(fun k' -> Name.O.(k <> k'))
                 in
                 let new_ps =
                   List.filter ps ~f:(fun (p, _) ->
                       not ([%compare.equal: Pred.t] p (Name k)))
                   |> Select_list.map ~f:(fun p _ ->
                          Pred.scoped (Schema.schema lhs) scope p)
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
                       (Name (Name.create key_name))
                   in
                   scalar p (Name.name k)
                 in
                 list lhs scope (tuple [ key_scalar; new_group_by ] Cross))
      | _ -> Seq.empty)
end
