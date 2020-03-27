open Ast
open Abslayout
open Collections
module V = Visitors
module A = Abslayout
module P = Pred.Infix

module Config = struct
  module type My_s = sig
    val conn : Db.t

    val params : Set.M(Name).t
  end

  module type S = sig
    include My_s

    include Ops.Config.S
  end
end

module Make (C : Config.S) = struct
  module Ops = Ops.Make (C)
  open Ops

  module C : Config.My_s = C

  open C

  let subst_rel ~key ~data r =
    let visitor =
      object
        inherit [_] V.map

        method! visit_Relation () rel =
          if [%compare.equal: Relation.t] key rel then data.node
          else Relation rel
      end
    in
    visitor#visit_t () r

  let dictionary_encode _ r =
    let preds_v =
      object
        inherit [_] V.reduce

        inherit [_] Util.list_monoid

        method! visit_Binop () op a1 a2 =
          match (op, Pred.to_type a1, Pred.to_type a2) with
          | Eq, StringT _, StringT _ -> [ (a1, a2) ]
          | _ -> []
      end
    in
    preds_v#visit_t () r
    |> List.filter_map ~f:(function
         | Name n1, Name n2 -> (
             match
               ( Db.relation_has_field conn (Name.name n1),
                 Db.relation_has_field conn (Name.name n2) )
             with
             | Some rel, None ->
                 if Set.mem params n2 then Some (n1, rel, n2) else None
             | None, Some rel ->
                 if Set.mem params n1 then Some (n2, rel, n1) else None
             | _ -> None )
         | _ -> None)
    |> List.map ~f:(fun (key, rel, lookup) ->
           let count_name = Fresh.name Global.fresh "c%d" in
           let encoded_name = Fresh.name Global.fresh "x%d" in
           let encoded_lookup_name = Fresh.name Global.fresh "x%d" in
           let map_name = Fresh.name Global.fresh "m%d" in
           let mapping =
             select
               [
                 As_pred (Row_number, encoded_name); Name (Name.create map_name);
               ]
               (order_by
                  [ (Name (Name.create encoded_name), Desc) ]
                  (dedup
                     (select [ As_pred (Name key, map_name) ] (relation rel))))
           in
           let encoded_rel =
             join
               (Binop (Eq, Name key, Name (Name.create map_name)))
               (relation rel) mapping
           in
           let encoded_lookup =
             let scope = Fresh.name Global.fresh "s%d" in
             select
               [
                 As_pred
                   ( If
                       ( Binop (Gt, Name (Name.create count_name), Int 0),
                         Name (Name.create encoded_name),
                         Int (-1) ),
                     encoded_lookup_name );
               ]
               (select
                  [
                    As_pred (Count, count_name); Name (Name.create encoded_name);
                  ]
                  (hash_idx
                     (dedup
                        (select [ As_pred (Name key, map_name) ] (relation rel)))
                     scope
                     (select
                        [ Name (Name.create encoded_name) ]
                        (A.filter
                           P.(
                             Name (Name.create ~scope map_name)
                             = Name (Name.create map_name))
                           mapping))
                     [ Name lookup ]))
           in
           let scope = Fresh.name Global.fresh "s%d" in
           dep_join encoded_lookup scope
             (subst_rel ~key:rel ~data:encoded_rel
                (subst
                   (Map.of_alist_exn
                      (module Name)
                      [
                        (key, P.name @@ Name.create encoded_name);
                        ( lookup,
                          P.name @@ Name.create ~scope encoded_lookup_name );
                      ])
                   r)))
    |> Seq.of_list

  let dictionary_encode =
    Branching.(global dictionary_encode ~name:"dictionary-encode")
end
