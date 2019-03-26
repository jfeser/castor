open Base
open Castor
open Abslayout
open Collections

module Config = struct
  module type S = sig
    val fresh : Fresh.t

    include Ops.Config.S

    include Abslayout_db.Config.S
  end
end

module Make (C : Config.S) = struct
  open C
  module Ops = Ops.Make (C)
  open Ops
  module M = Abslayout_db.Make (C)

  (* Generate aggregates for collections that act by concatenating their children. *)
  let gen_concat_select_list outer_preds inner_schema =
    let visitor =
      object (self : 'a)
        inherit [_] Abslayout0.mapreduce

        inherit [_] Util.list_monoid

        method add_agg aggs a =
          match
            List.find aggs ~f:(fun (_, a') -> [%compare.equal: pred] a a')
          with
          | Some (n, _) -> (Name n, aggs)
          | None ->
              let type_ = pred_to_schema a |> Name.type_exn in
              let n = Fresh.name fresh "agg%d" |> Name.create ~type_ in
              (Name n, (n, a) :: aggs)

        method! visit_Sum aggs p =
          let n, aggs' = self#add_agg aggs (Sum p) in
          (Sum n, aggs')

        method! visit_Count aggs =
          let n, aggs' = self#add_agg aggs Count in
          (Sum n, aggs')

        method! visit_Min aggs p =
          let n, aggs' = self#add_agg aggs (Min p) in
          (Min n, aggs')

        method! visit_Max aggs p =
          let n, aggs' = self#add_agg aggs (Max p) in
          (Max n, aggs')

        method! visit_Avg aggs p =
          let s, aggs' = self#add_agg aggs (Sum p) in
          let c, aggs'' = self#add_agg aggs' Count in
          (Binop (Div, Sum s, Sum c), aggs'')
      end
    in
    let outer_aggs, inner_aggs =
      List.fold_left outer_preds ~init:([], []) ~f:(fun (op, ip) p ->
          let p', ip' = visitor#visit_pred ip p in
          (op @ [p'], ip') )
    in
    let inner_aggs =
      List.map inner_aggs ~f:(fun (n, a) -> As_pred (a, Name.name n))
    in
    (* Don't want to project out anything that we might need later. *)
    let inner_fields = inner_schema |> List.map ~f:(fun n -> Name n) in
    (outer_aggs, inner_aggs @ inner_fields)

  let push_select r =
    M.annotate_schema r ;
    match r.node with
    | Select (ps, r') -> (
      match r'.node with
      | AHashIdx (rk, rv, m) ->
          let outer_preds =
            List.filter_map ps ~f:pred_to_name |> List.map ~f:(fun n -> Name n)
          in
          let inner_preds =
            (* TODO: This hack works around problems with sql conversion and
               lateral joins. *)
            let kschema = Meta.(find_exn rk schema) in
            List.filter ps ~f:(function
              | Name n -> not (List.mem ~equal:Name.O.( = ) kschema n)
              | _ -> true )
          in
          Some (select outer_preds (hash_idx' rk (select inner_preds rv) m))
      | AOrderedIdx (rk, rv, m) ->
          let outer_aggs, inner_aggs =
            gen_concat_select_list ps Meta.(find_exn rv schema)
          in
          Some (select outer_aggs (ordered_idx rk (select inner_aggs rv) m))
      | AList (rk, rv) ->
          let outer_aggs, inner_aggs =
            gen_concat_select_list ps Meta.(find_exn rv schema)
          in
          Some (select outer_aggs (list rk (select inner_aggs rv)))
      | ATuple (r' :: rs', Concat) ->
          let outer_aggs, inner_aggs =
            gen_concat_select_list ps Meta.(find_exn r' schema)
          in
          Some
            (select outer_aggs
               (tuple (List.map (r' :: rs') ~f:(select inner_aggs)) Concat))
      | _ -> None )
    | _ -> None

  let push_select = of_func push_select ~name:"push-select"
end
