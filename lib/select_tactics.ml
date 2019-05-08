open Core
open Castor
open Abslayout
open Collections

module Config = struct
  module type S = sig
    include Ops.Config.S

    include Abslayout_db.Config.S
  end
end

module Make (C : Config.S) = struct
  module Ops = Ops.Make (C)
  open Ops
  module M = Abslayout_db.Make (C)

  (** Extend a list of predicates to include those needed by aggregate `p`.
     Returns a name to use in the aggregate. *)
  let extend_aggs aggs p =
    let aggs = ref aggs in
    let add_agg a =
      match
        List.find !aggs ~f:(fun (_, a') -> [%compare.equal: pred] a a')
      with
      | Some (n, _) -> Name n
      | None ->
          let n =
            Fresh.name Global.fresh "agg%d"
            |> Name.create ~type_:(Pred.to_type a)
          in
          aggs := (n, a) :: !aggs ;
          Name n
    in
    let visitor =
      object
        inherit [_] Abslayout0.map

        method! visit_Sum () p = Sum (add_agg (Sum p))

        method! visit_Count () = Sum (add_agg Count)

        method! visit_Min () p = Min (add_agg (Min p))

        method! visit_Max () p = Max (add_agg (Max p))

        method! visit_Avg () p =
          Binop (Div, Sum (add_agg (Sum p)), Sum (add_agg Count))
      end
    in
    let p' = visitor#visit_pred () p in
    (!aggs, p')

  (* Generate aggregates for collections that act by concatenating their children. *)
  let gen_concat_select_list outer_preds inner_schema =
    let outer_aggs, inner_aggs =
      List.fold_left outer_preds ~init:([], []) ~f:(fun (op, ip) p ->
          let ip, p = extend_aggs ip p in
          (op @ [p], ip) )
    in
    let inner_aggs =
      List.map inner_aggs ~f:(fun (n, a) -> As_pred (a, Name.name n))
    in
    (* Don't want to project out anything that we might need later. *)
    let inner_fields = inner_schema |> List.map ~f:(fun n -> Name n) in
    (outer_aggs, inner_aggs @ inner_fields)

  (* Look for evidence of a previous pushed select. *)
  let already_pushed r' =
    try
      match Path.get_exn (Path.child Path.root 1) r' with
      | {node= Filter (_, {node= Select _; _}); _} -> true
      | _ -> false
    with _ -> false

  let push_select r =
    match r.node with
    | Select (ps, r') ->
        if already_pushed r' then None
        else
          let open Option.Let_syntax in
          let%bind outer_preds, inner_preds =
            match r'.node with
            | AHashIdx (rk, _, _) ->
                let o =
                  List.filter_map ps ~f:Pred.to_name
                  |> List.map ~f:(fun n -> Name n)
                in
                let i =
                  (* TODO: This hack works around problems with sql conversion and
               lateral joins. *)
                  let kschema = schema_exn rk in
                  List.filter ps ~f:(function
                    | Name n -> not (List.mem ~equal:Name.O.( = ) kschema n)
                    | _ -> true )
                in
                Some (o, i)
            | AOrderedIdx (_, rv, _) | AList (_, rv) | ATuple (rv :: _, Concat)
              ->
                let o, i = gen_concat_select_list ps (schema_exn rv) in
                Some (o, i)
            | _ -> None
          in
          let%map mk_collection =
            match r'.node with
            | AHashIdx (rk, rv, m) ->
                Some
                  (fun mk_select -> hash_idx rk (scope_exn rk) (mk_select rv) m)
            | AOrderedIdx (rk, rv, m) ->
                Some
                  (fun mk_select ->
                    ordered_idx rk (scope_exn rk) (mk_select rv) m )
            | AList (rk, rv) ->
                Some (fun mk_select -> list rk (scope_exn rk) (mk_select rv))
            | ATuple (r' :: rs', Concat) ->
                Some
                  (fun mk_select ->
                    tuple (List.map (r' :: rs') ~f:mk_select) Concat )
            | _ -> None
          in
          let count_n = Fresh.name Global.fresh "count%d" in
          let inner_preds = As_pred (Count, count_n) :: inner_preds in
          select outer_preds
            (mk_collection (fun rv ->
                 filter
                   (Binop (Gt, Name (Name.create count_n), Int 0))
                   (select inner_preds rv) ))
    | _ -> None

  let push_select = of_func push_select ~name:"push-select"
end

(* module Test = struct
 *   module C = struct
 *     let params =
 *       Set.of_list
 *         (module Name)
 *         [Name.create ~type_:(DateT {nullable= false}) "param0"]
 * 
 *     let fresh = Fresh.create ()
 * 
 *     let verbose = false
 * 
 *     let validate = false
 * 
 *     let param_ctx = Map.empty (module Name)
 * 
 *     let conn = Db.create "postgresql:///tpch_1k"
 *   end
 * 
 *   module T = Make (C)
 *   module O = Ops.Make (C)
 *   open T
 *   open O
 *   open C
 * 
 *   let%expect_test "" =
 *     let r =
 *       of_string_exn
 *         {|
 * select([customer.c_custkey,
 *          customer.c_name,
 *          sum((lineitem.l_extendedprice * (1 - lineitem.l_discount))) as revenue,
 *          customer.c_acctbal,
 *          nation.n_name,
 *          customer.c_address,
 *          customer.c_phone,
 *          customer.c_comment],
 *    aorderedidx(dedup(
 *                  select([orders.o_orderdate as k1],
 *                    dedup(
 *                      select([orders.o_orderdate],
 *                        join((customer.c_custkey = orders.o_custkey),
 *                          join((customer.c_nationkey = nation.n_nationkey),
 *                            customer,
 *                            nation),
 *                          join((lineitem.l_orderkey = orders.o_orderkey),
 *                            lineitem,
 *                            orders)))))),
 *        join((customer.c_custkey = orders.o_custkey),
 *          join((customer.c_nationkey = nation.n_nationkey), customer, nation),
 *          join((lineitem.l_orderkey = orders.o_orderkey), lineitem, orders)),
 *      (param0 + day(1)),
 *      ((param0 + month(3)) + day(1))))
 * |}
 *     in
 *     let r = M.resolve ~params r in
 *     apply push_select r |> Option.iter ~f:(Format.printf "%a@." pp) ;
 *     [%expect
 *       {|
 *       select([customer.c_custkey,
 *               customer.c_name,
 *               sum(agg0) as revenue,
 *               customer.c_acctbal,
 *               nation.n_name,
 *               customer.c_address,
 *               customer.c_phone,
 *               customer.c_comment],
 *         aorderedidx(dedup(
 *                       select([orders.o_orderdate as k1],
 *                         dedup(
 *                           select([orders.o_orderdate],
 *                             join((customer.c_custkey = orders.o_custkey),
 *                               join((customer.c_nationkey = nation.n_nationkey),
 *                                 customer,
 *                                 nation),
 *                               join((lineitem.l_orderkey = orders.o_orderkey),
 *                                 lineitem,
 *                                 orders)))))),
 *           filter((count1 > 0),
 *             select([count() as count1,
 *                     sum((lineitem.l_extendedprice * (1 - lineitem.l_discount))) as agg0,
 *                     customer.c_custkey,
 *                     customer.c_name,
 *                     customer.c_address,
 *                     customer.c_nationkey,
 *                     customer.c_phone,
 *                     customer.c_acctbal,
 *                     customer.c_mktsegment,
 *                     customer.c_comment,
 *                     nation.n_nationkey,
 *                     nation.n_name,
 *                     nation.n_regionkey,
 *                     nation.n_comment,
 *                     lineitem.l_orderkey,
 *                     lineitem.l_partkey,
 *                     lineitem.l_suppkey,
 *                     lineitem.l_linenumber,
 *                     lineitem.l_quantity,
 *                     lineitem.l_extendedprice,
 *                     lineitem.l_discount,
 *                     lineitem.l_tax,
 *                     lineitem.l_returnflag,
 *                     lineitem.l_linestatus,
 *                     lineitem.l_shipdate,
 *                     lineitem.l_commitdate,
 *                     lineitem.l_receiptdate,
 *                     lineitem.l_shipinstruct,
 *                     lineitem.l_shipmode,
 *                     lineitem.l_comment,
 *                     orders.o_orderkey,
 *                     orders.o_custkey,
 *                     orders.o_orderstatus,
 *                     orders.o_totalprice,
 *                     orders.o_orderdate,
 *                     orders.o_orderpriority,
 *                     orders.o_clerk,
 *                     orders.o_shippriority,
 *                     orders.o_comment],
 *               join((customer.c_custkey = orders.o_custkey),
 *                 join((customer.c_nationkey = nation.n_nationkey), customer, nation),
 *                 join((lineitem.l_orderkey = orders.o_orderkey), lineitem, orders)))),
 *           (param0 + day(1)),
 *           ((param0 + month(3)) + day(1)))) |}]
 * end *)
