open! Core
open Abslayout
open Schema

let unscoped_free q = free q |> Set.map (module Name) ~f:Name.unscoped

let schema_set q = schema q |> Set.of_list (module Name)

let to_nice_depjoin { d_lhs = t1; d_rhs = t2; d_alias } =
  let t2_free = unscoped_free t2 in
  let t1_attr = schema_set t1 in

  (* Create a relation of the unique values of the attributes that come from t1
     and are bound in t2. *)
  let d =
    dedup
    @@ select
         (Set.inter t2_free t1_attr |> Set.to_list |> List.map ~f:Pred.name)
         t1
  in

  (* Create a renaming of the attribute names in d. This will ensure that we can join d and t1 without name clashes. *)
  let subst =
    schema d
    |> List.map ~f:(fun n -> (n, Fresh.name Global.fresh "bnd%d"))
    |> Map.of_alist_exn (module Name)
  in

  (* Apply the renaming to t1. *)
  let t1 =
    let select_list =
      schema t1
      |> List.map ~f:(fun n ->
             match Map.find subst n with
             | Some alias -> Pred.Infix.(as_ (name n) alias)
             | None -> Pred.name n)
    in
    select select_list t1
  in

  (* Create a predicate that joins t1 and d. *)
  let join_pred =
    Map.to_alist subst
    |> List.map ~f:(fun (n, n') -> Pred.Infix.(name n = name (Name.create n')))
    |> Pred.conjoin
  in

  join join_pred t1 (dep_join d d_alias t2)

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in

  let q =
    Infix.(
      dep_join (r "r") "k"
        (select [ n "g" ] (filter (n "k.f" = n "f") (r "r1"))))
    |> Abslayout_load.load_layout conn
  in

  let d = match q.node with DepJoin d -> d | _ -> assert false in
  to_nice_depjoin d |> Format.printf "%a" pp;
  [%expect
    {|
    join((f = bnd0),
      select([f as bnd0, g], r),
      depjoin(dedup(select([f], r)) as k, select([g], filter((k.f = f), r1)))) |}]

let push_join d d_alias { pred = p; r1 = t1; r2 = t2 } =
  let d_attr = schema_set d in
  if Set.inter (unscoped_free t2) d_attr |> Set.is_empty then
    join (Pred.unscoped d_alias p) (dep_join d d_alias t1) t2
  else if Set.inter (unscoped_free t1) d_attr |> Set.is_empty then
    join (Pred.unscoped d_alias p) t1 (dep_join d d_alias t2)
  else
    (* Rename the d relation in the rhs of the join *)
    let d_rhs =
      schema d |> List.map ~f:(fun n -> (n, Fresh.name Global.fresh "d%d"))
    in
    let rhs_select =
      List.map d_rhs ~f:(fun (x, x') -> Infix.(as_ (name x) x'))
      @ (schema t2 |> List.map ~f:Pred.name)
    in
    (* Select out the duplicate d attributes *)
    let outer_select =
      schema t1 @ schema t2 @ schema d |> List.map ~f:Pred.name
    in
    (* Perform a natural join on the two copies of d *)
    let d_pred =
      List.map d_rhs ~f:(fun (x, x') -> Infix.(name x = n x')) |> Pred.conjoin
    in
    select outer_select
    @@ join
         Infix.(Pred.unscoped d_alias p && d_pred)
         (dep_join d d_alias t1)
         (select rhs_select @@ dep_join d d_alias t2)

let push_filter d d_alias (p, t2) =
  filter (Pred.unscoped d_alias p) (dep_join d d_alias t2)

let push_groupby d d_alias (aggs, keys, q) =
  let aggs =
    List.map ~f:(Pred.unscoped d_alias) aggs
    @ (schema d |> List.map ~f:Pred.name)
  in
  let keys = keys @ schema d in
  group_by aggs keys (dep_join d d_alias q)

let push_select d d_alias (preds, q) =
  let preds =
    List.map ~f:(Pred.unscoped d_alias) preds
    @ (schema d |> List.map ~f:Pred.name)
  in
  select preds (dep_join d d_alias q)

let push_tuple d d_alias qs =
  let qs = List.map qs ~f:(dep_join d d_alias) in
  tuple qs Concat

class push_depjoin_visitor =
  object
    inherit [_] map as super

    method! visit_DepJoin () { d_lhs = d; d_alias; d_rhs } =
      let q =
        if Set.inter (unscoped_free d_rhs) (schema_set d) |> Set.is_empty then
          join (Pred.bool true) d d_rhs
        else
          match d_rhs.node with
          | Filter x -> push_filter d d_alias x
          | Join x -> push_join d d_alias x
          | GroupBy x -> push_groupby d d_alias x
          | Select x -> push_select d d_alias x
          | ATuple (qs, Concat) -> push_tuple d d_alias qs
          | _ -> failwith "Stuck depjoin."
      in
      (super#visit_t () q).node
  end

let unnest q =
  let nice_visitor =
    object
      inherit [_] map

      method! visit_DepJoin () d = (to_nice_depjoin d).node
    end
  in
  q |> nice_visitor#visit_t () |> (new push_depjoin_visitor)#visit_t ()

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in

  let q =
    Infix.(
      dep_join (r "r") "k"
        (select [ n "g" ] (filter (n "k.f" = n "f") (r "r1"))))
    |> Abslayout_load.load_layout conn
  in

  unnest q |> Format.printf "%a" pp;
  [%expect
    {|
    join((f = bnd0),
      select([f as bnd0, g], r),
      depjoin(dedup(select([f], r)) as k, select([g], filter((k.f = f), r1)))) |}]
