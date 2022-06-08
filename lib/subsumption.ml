open Core
open Collections
open Ast
open Sql

module Foreign_key_join_graph = struct
  module Edge = struct
    type t = { l_attr : string; r_attr : string } [@@deriving compare]

    let default = { l_attr = ""; r_attr = "" }
  end

  include Graph.Persistent.Digraph.ConcreteLabeled (String) (Edge)
end

(* split conjunction into column equality and other predicates *)
let split p =
  let ps = Pred.conjuncts p in
  List.partition_map ps ~f:(fun p ->
      match p with
      | `Binop (Eq, `Name n, `Name n') -> First (n, n')
      | p -> Second p)

let find_output_column equiv output name =
  List.find_map output ~f:(fun e ->
      match e.pred with
      | `Name n when [%equal: Name.t] n name || equiv n name ->
          Some (Name.create e.alias)
      | _ -> None)
  |> Result.of_option
       ~error:
         (Error.create_s [%message "no output column matching" (name : Name.t)])

let find_output_columns_in_pred equiv output p =
  let exception No_output_col of Name.t in
  try
    Pred.map_names p ~f:(fun n ->
        match find_output_column equiv output n with
        | Ok n' -> `Name n'
        | Error _ -> raise (No_output_col n))
    |> Or_error.return
  with No_output_col n ->
    Or_error.error_s [%message "no output column matching" (n : Name.t)]

let equijoin_sub view_equiv query_equiv =
  let classes m =
    Map.to_alist m
    |> List.map ~f:(fun (n, n') -> (Union_find.get n', n))
    |> Map.of_alist_multi (module Name)
    |> Map.data
    |> List.map ~f:(Set.of_list (module Name))
  in

  let view_equiv = classes view_equiv and query_equiv = classes query_equiv in

  let extra_view_filter =
    List.find view_equiv ~f:(fun view_class ->
        not
          (List.exists query_equiv ~f:(fun qc ->
               Set.is_subset view_class ~of_:qc)))
  in
  match extra_view_filter with
  | Some p ->
      Or_error.error_s [%message "extra view filter" (p : Set.M(Name).t)]
  | None ->
      (* for every query equivalence class, find all view equivalence classes that are contained *)
      List.concat_map query_equiv ~f:(fun query_class ->
          let view_subsets =
            List.filter view_equiv ~f:(fun view_class ->
                Set.is_subset view_class ~of_:query_class)
          in
          List.mapw2 view_subsets ~f:(fun s s' ->
              (Set.choose_exn s, Set.choose_exn s')))
      |> Or_error.return

let residual_sub query_equiv view_preds query_preds =
  let subst_ctx = Map.map query_equiv ~f:(fun n -> `Name (Union_find.get n)) in
  let view_preds =
    List.map view_preds ~f:(Pred.subst subst_ctx) |> Set.of_list (module Pred)
  in
  let query_preds =
    List.map query_preds ~f:(Pred.subst subst_ctx) |> Set.of_list (module Pred)
  in

  let extra_view_preds = Set.diff view_preds query_preds in
  if not (Set.is_empty extra_view_preds) then
    Or_error.error_s
      [%message "extra view predicates" (extra_view_preds : Set.M(Pred).t)]
  else Ok (Set.to_list @@ Set.diff query_preds view_preds)

let relations x = List.map x.relations ~f:(fun (r, a) -> (r, a))

let of_sql : Sql.t -> _ =
  let open Option.Let_syntax in
  function
  | `Union_all _ -> None
  | `Query q ->
      let%bind relations =
        List.map q.relations ~f:(fun (r, a, j) ->
            match (r, j) with `Table t, `Left -> Some (t, a) | _ -> None)
        |> Option.all
      in
      return { q with relations }

let subsumes ~view ~query =
  let open Or_error.Let_syntax in
  if view.distinct || query.distinct then
    Or_error.error_s [%message "dedup not allowed"]
  else if not (List.is_empty view.group && List.is_empty query.group) then
    Or_error.error_s [%message "groupby not allowed"]
  else if Option.is_some view.limit || Option.is_some query.limit then
    Or_error.error_s [%message "limit not allowed"]
  else
    let view_relations = relations view and query_relations = relations query in

    if not ([%equal: (Relation.t * string) list] view_relations query_relations)
    then
      Or_error.error_s
        [%message
          "different relations"
            (view_relations : (Relation.t * string) list)
            (query_relations : (Relation.t * string) list)]
    else
      let view_equiv = Pred.equiv (Pred.conjoin view.conds) in
      let query_equiv = Pred.equiv (Pred.conjoin query.conds) in
      let equiv eq x x' =
        match (Map.find eq x, Map.find eq x') with
        | Some c, Some c' -> Union_find.same_class c c'
        | _ -> false
      in

      (* collect compensating predicates *)
      let%bind eq_comp_preds = equijoin_sub view_equiv query_equiv in

      let%bind eq_comp_preds =
        List.map eq_comp_preds ~f:(fun (n, n') ->
            let%bind n = find_output_column (equiv view_equiv) view.select n in
            let%bind n' =
              find_output_column (equiv view_equiv) view.select n'
            in
            return (n, n'))
        |> Result.all
      in

      let%bind residual_comp_preds =
        residual_sub query_equiv view.conds query.conds
      in
      let%bind residual_comp_preds =
        List.map residual_comp_preds
          ~f:(find_output_columns_in_pred (equiv query_equiv) view.select)
        |> Result.all
      in

      let%bind output =
        List.map query.select ~f:(fun e ->
            let%bind pred =
              find_output_columns_in_pred (equiv query_equiv) view.select e.pred
            in
            return { e with pred })
        |> Result.all
      in

      let eq_comp_preds =
        List.map eq_comp_preds ~f:(fun (n, n') ->
            `Binop (Binop.Eq, `Name n, `Name n'))
      in
      return (output, eq_comp_preds @ residual_comp_preds)

let%expect_test "" =
  let relations =
    [
      (Relation.create "lineitem", "lineitem");
      (Relation.create "orders", "orders");
      (Relation.create "part", "part");
    ]
  in
  let view =
    Sql.create_spj ~relations
      ~conds:
        Pred.Infix.
          [
            name_s "l_orderkey" = name_s "o_orderkey";
            name_s "l_partkey" = name_s "p_partkey";
          ]
      (Sql.create_entries_s
         [ "l_orderkey"; "o_custkey"; "l_partkey"; "l_shipdate"; "o_orderdate" ]
         (* @ [ *)
         (*     Sql.create_entry ~alias:"gross_revenue" *)
         (*       Pred.Infix.(name_s "l_quantity" * name_s "l_extendedprice") *)
         (*       "x"; *)
         (*   ] *))
  in
  let query =
    Sql.create_spj ~relations
      ~conds:
        Pred.Infix.
          [
            name_s "l_orderkey" = name_s "o_orderkey";
            name_s "l_partkey" = name_s "p_partkey";
            name_s "o_orderdate" = name_s "l_shipdate";
          ]
      (Sql.create_entries_s
         [ "l_orderkey"; "o_custkey"; "l_partkey" ]
         (* @ [ *)
         (*     Sql.create_entry *)
         (*       Pred.Infix.(name_s "l_quantity" * name_s "l_extendedprice") *)
         (*       "x"; *)
         (*   ] *))
  in
  print_s
    [%message
      (subsumes ~query ~view
        : (select_entry list * _ annot pred list) Or_error.t)];
  [%expect
    {|
       ("subsumes ~query ~view"
        (Ok
         ((((pred (Name ((name (Simple l_orderkey))))) (alias l_orderkey) (cast ()))
           ((pred (Name ((name (Simple o_custkey))))) (alias o_custkey) (cast ()))
           ((pred (Name ((name (Simple l_partkey))))) (alias l_partkey) (cast ())))
          ((Binop
            (Eq (Name ((name (Simple l_shipdate))))
             (Name ((name (Simple l_shipdate)))))))))) |}]
