open Core
open Abslayout
open Collections

module Config = struct
  module type S = sig
    include Abslayout_db.Config.S
  end
end

module Make (C : Config.S) = struct
  module M = Abslayout_db.Make (C)

  let test = Logs.Src.create ~doc:"Source for testing project." "project-test"

  let wrap_selects r =
    let visitor =
      object
        inherit [_] map

        method! visit_Scan () s =
          let schema = M.relation_schema s in
          Select (List.map schema ~f:(fun n -> Name n), scan s)
      end
    in
    M.annotate_schema r ; visitor#visit_t () r

  let project_defs ps =
    List.filter ps ~f:(fun p ->
        match pred_to_name p with
        | None ->
            (* Filter out definitions that have no name *)
            false
        | Some n -> (
          (* Filter out definitions that are never referenced. *)
          match Name.Meta.(find n refcnt) with
          | Some c -> c > 0
          | None ->
              (* Be conservative if refcount is missing. *)
              true ) )

  let project_visitor =
    object (self : 'a)
      inherit [_] endo as super

      method! visit_Select () _ (ps, r) = Select (project_defs ps, self#visit_t () r)

      method! visit_GroupBy () _ ps ns r =
        GroupBy (project_defs ps, ns, self#visit_t () r)

      method! visit_AScalar () _ p =
        match project_defs [p] with
        | [] -> AScalar Null
        | [p] -> AScalar p
        | _ -> assert false

      method! visit_ATuple () t (ps, k) =
        match super#visit_ATuple () t (ps, k) with
        | ATuple (ps, (Cross as k)) ->
            let ps =
              List.filter ps ~f:(fun r ->
                  match r.node with AScalar Null -> false | _ -> true )
            in
            (* Preserve a single scalar to avoid messing up count aggregates.
                 *)
            let ps = if List.is_empty ps then [scalar Null] else ps in
            ATuple (ps, k)
        | ATuple _ as a -> a
        | _ -> assert false
    end

  let project ~params r =
    let r = M.resolve ~params r in
    let r = wrap_selects r in
    let rec loop r =
      let r' = M.resolve r ~params |> project_visitor#visit_t () in
      Logs.debug ~src:test (fun m -> m "%a@." pp r') ;
      if Abslayout.O.(r = r') then r' else loop r'
    in
    loop r
end

module Test = struct
  module T = Make (struct
    let conn = Db.create "postgresql:///tpch_1k"
  end)

  open T

  let pp, _ = mk_pp ~pp_name:Name.pp_with_stage_and_refcnt ()

  let%expect_test "" =
    let r =
      of_string_exn
        {|
alist(orderby([k0.l_shipmode],
         select([lineitem.l_shipmode],
           dedup(select([lineitem.l_shipmode], lineitem))) as k0),
   select([lineitem.l_shipmode,
           sum(agg2) as high_line_count,
           sum(agg3) as low_line_count],
     aorderedidx(dedup(
                   select([lineitem.l_receiptdate as k1],
                     dedup(
                       select([lineitem.l_receiptdate],
                         alist(join((orders.o_orderkey = lineitem.l_orderkey),
                                 lineitem,
                                 orders),
                           atuple([ascalar(lineitem.l_orderkey),
                                   ascalar(lineitem.l_partkey),
                                   ascalar(lineitem.l_suppkey),
                                   ascalar(lineitem.l_linenumber),
                                   ascalar(lineitem.l_quantity),
                                   ascalar(lineitem.l_extendedprice),
                                   ascalar(lineitem.l_discount),
                                   ascalar(lineitem.l_tax),
                                   ascalar(lineitem.l_returnflag),
                                   ascalar(lineitem.l_linestatus),
                                   ascalar(lineitem.l_shipdate),
                                   ascalar(lineitem.l_commitdate),
                                   ascalar(lineitem.l_receiptdate),
                                   ascalar(lineitem.l_shipinstruct),
                                   ascalar(lineitem.l_shipmode),
                                   ascalar(lineitem.l_comment),
                                   ascalar(orders.o_orderkey),
                                   ascalar(orders.o_custkey),
                                   ascalar(orders.o_orderstatus),
                                   ascalar(orders.o_totalprice),
                                   ascalar(orders.o_orderdate),
                                   ascalar(orders.o_orderpriority),
                                   ascalar(orders.o_clerk),
                                   ascalar(orders.o_shippriority),
                                   ascalar(orders.o_comment)],
                             cross)))))),
       filter((count4 > 0),
         select([count() as count4,
                 sum((if (not((orders.o_orderpriority = "1-URGENT")) &&
                         not((orders.o_orderpriority = "2-HIGH"))) then 1 else 0)) as agg3,
                 sum((if ((orders.o_orderpriority = "1-URGENT") ||
                         (orders.o_orderpriority = "2-HIGH")) then 1 else 0)) as agg2,
                 lineitem.l_shipmode],
           alist(filter(((lineitem.l_shipmode = k0.l_shipmode) &&
                        ((lineitem.l_commitdate < lineitem.l_receiptdate) &&
                        ((lineitem.l_shipdate < lineitem.l_commitdate) &&
                        (k1 = lineitem.l_receiptdate)))),
                   join((orders.o_orderkey = lineitem.l_orderkey),
                     lineitem,
                     orders)),
             filter(((lineitem.l_shipmode = param1) ||
                    (lineitem.l_shipmode = param2)),
               atuple([ascalar(lineitem.l_shipmode),
                       ascalar(orders.o_orderpriority)],
                 cross))))),
       (param3 + day(1)),
       ((param3 + year(1)) + day(1)))))
|}
    in
    project
      ~params:
        (Set.of_list
           (module Name)
           [ Name.create ~type_:(StringT {nullable= false}) "param1"
           ; Name.create ~type_:(StringT {nullable= false}) "param2"
           ; Name.create ~type_:(DateT {nullable= false}) "param3" ])
      r
    |> Format.printf "%a@." pp ;
    [%expect
      {|
      alist(orderby([k0.l_shipmode],
              select([lineitem.l_shipmode],
                dedup(select([lineitem.l_shipmode], lineitem))) as k0),
        select([lineitem.l_shipmode,
                sum(agg2) as high_line_count,
                sum(agg3) as low_line_count],
          aorderedidx(dedup(
                        select([lineitem.l_receiptdate as k1],
                          dedup(
                            select([lineitem.l_receiptdate],
                              alist(join((orders.o_orderkey = lineitem.l_orderkey),
                                      lineitem,
                                      orders),
                                atuple([ascalar(lineitem.l_receiptdate)], cross)))))),
            filter((count4 > 0),
              select([count() as count4,
                      sum((if (not((orders.o_orderpriority = "1-URGENT")) &&
                              not((orders.o_orderpriority = "2-HIGH"))) then 1 else 0)) as agg3,
                      sum((if ((orders.o_orderpriority = "1-URGENT") ||
                              (orders.o_orderpriority = "2-HIGH")) then 1 else 0)) as agg2,
                      lineitem.l_shipmode],
                alist(filter(((lineitem.l_shipmode = k0.l_shipmode) &&
                             ((lineitem.l_commitdate < lineitem.l_receiptdate) &&
                             ((lineitem.l_shipdate < lineitem.l_commitdate) &&
                             (k1 = lineitem.l_receiptdate)))),
                        join((orders.o_orderkey = lineitem.l_orderkey),
                          lineitem,
                          orders)),
                  filter(((lineitem.l_shipmode = param1) ||
                         (lineitem.l_shipmode = param2)),
                    atuple([ascalar(lineitem.l_shipmode),
                            ascalar(orders.o_orderpriority)],
                      cross))))),
            (param3 + day(1)),
            ((param3 + year(1)) + day(1))))) |}]

  let%expect_test _ =
    let r =
      of_string_exn
        {|
select([nation.n_name, revenue],
   alist(select([nation.n_name], dedup(select([nation.n_name], nation))) as k0,
     select([nation.n_name, sum(agg3) as revenue],
       aorderedidx(dedup(
                     select([orders.o_orderdate as k2],
                       dedup(select([orders.o_orderdate], orders)))),
         filter((count4 > 0),
           select([count() as count4,
                   sum((lineitem.l_extendedprice * (1 - lineitem.l_discount))) as agg3,
                   k1,
                   region.r_regionkey,
                   region.r_name,
                   region.r_comment,
                   customer.c_custkey,
                   customer.c_name,
                   customer.c_address,
                   customer.c_nationkey,
                   customer.c_phone,
                   customer.c_acctbal,
                   customer.c_mktsegment,
                   customer.c_comment,
                   orders.o_orderkey,
                   orders.o_custkey,
                   orders.o_orderstatus,
                   orders.o_totalprice,
                   orders.o_orderdate,
                   orders.o_orderpriority,
                   orders.o_clerk,
                   orders.o_shippriority,
                   orders.o_comment,
                   lineitem.l_orderkey,
                   lineitem.l_partkey,
                   lineitem.l_suppkey,
                   lineitem.l_linenumber,
                   lineitem.l_quantity,
                   lineitem.l_extendedprice,
                   lineitem.l_discount,
                   lineitem.l_tax,
                   lineitem.l_returnflag,
                   lineitem.l_linestatus,
                   lineitem.l_shipdate,
                   lineitem.l_commitdate,
                   lineitem.l_receiptdate,
                   lineitem.l_shipinstruct,
                   lineitem.l_shipmode,
                   lineitem.l_comment,
                   supplier.s_suppkey,
                   supplier.s_name,
                   supplier.s_address,
                   supplier.s_nationkey,
                   supplier.s_phone,
                   supplier.s_acctbal,
                   supplier.s_comment,
                   nation.n_nationkey,
                   nation.n_name,
                   nation.n_regionkey,
                   nation.n_comment],
             ahashidx(dedup(
                        select([region.r_name as k1],
                          atuple([alist(region,
                                    atuple([ascalar(region.r_regionkey),
                                            ascalar(region.r_name),
                                            ascalar(region.r_comment)],
                                      cross)),
                                  filter((nation.n_regionkey =
                                         region.r_regionkey),
                                    alist(join((supplier.s_nationkey =
                                               nation.n_nationkey),
                                            join(((lineitem.l_suppkey =
                                                  supplier.s_suppkey) &&
                                                 (customer.c_nationkey =
                                                 supplier.s_nationkey)),
                                              join((lineitem.l_orderkey =
                                                   orders.o_orderkey),
                                                join((customer.c_custkey =
                                                     orders.o_custkey),
                                                  customer,
                                                  orders),
                                                lineitem),
                                              supplier),
                                            nation),
                                      atuple([ascalar(customer.c_custkey),
                                              ascalar(customer.c_name),
                                              ascalar(customer.c_address),
                                              ascalar(customer.c_nationkey),
                                              ascalar(customer.c_phone),
                                              ascalar(customer.c_acctbal),
                                              ascalar(customer.c_mktsegment),
                                              ascalar(customer.c_comment),
                                              ascalar(orders.o_orderkey),
                                              ascalar(orders.o_custkey),
                                              ascalar(orders.o_orderstatus),
                                              ascalar(orders.o_totalprice),
                                              ascalar(orders.o_orderdate),
                                              ascalar(orders.o_orderpriority),
                                              ascalar(orders.o_clerk),
                                              ascalar(orders.o_shippriority),
                                              ascalar(orders.o_comment),
                                              ascalar(lineitem.l_orderkey),
                                              ascalar(lineitem.l_partkey),
                                              ascalar(lineitem.l_suppkey),
                                              ascalar(lineitem.l_linenumber),
                                              ascalar(lineitem.l_quantity),
                                              ascalar(lineitem.l_extendedprice),
                                              ascalar(lineitem.l_discount),
                                              ascalar(lineitem.l_tax),
                                              ascalar(lineitem.l_returnflag),
                                              ascalar(lineitem.l_linestatus),
                                              ascalar(lineitem.l_shipdate),
                                              ascalar(lineitem.l_commitdate),
                                              ascalar(lineitem.l_receiptdate),
                                              ascalar(lineitem.l_shipinstruct),
                                              ascalar(lineitem.l_shipmode),
                                              ascalar(lineitem.l_comment),
                                              ascalar(supplier.s_suppkey),
                                              ascalar(supplier.s_name),
                                              ascalar(supplier.s_address),
                                              ascalar(supplier.s_nationkey),
                                              ascalar(supplier.s_phone),
                                              ascalar(supplier.s_acctbal),
                                              ascalar(supplier.s_comment),
                                              ascalar(nation.n_nationkey),
                                              ascalar(nation.n_name),
                                              ascalar(nation.n_regionkey),
                                              ascalar(nation.n_comment)],
                                        cross)))],
                            cross))),
               atuple([alist(filter((k1 = region.r_name), region),
                         atuple([ascalar(region.r_regionkey),
                                 ascalar(region.r_name),
                                 ascalar(region.r_comment)],
                           cross)),
                       filter((nation.n_regionkey = region.r_regionkey),
                         alist(filter(((nation.n_name = k0.n_name) &&
                                      (k2 = orders.o_orderdate)),
                                 join((supplier.s_nationkey =
                                      nation.n_nationkey),
                                   join(((lineitem.l_suppkey =
                                         supplier.s_suppkey) &&
                                        (customer.c_nationkey =
                                        supplier.s_nationkey)),
                                     join((lineitem.l_orderkey =
                                          orders.o_orderkey),
                                       join((customer.c_custkey =
                                            orders.o_custkey),
                                         customer,
                                         orders),
                                       lineitem),
                                     supplier),
                                   nation)),
                           atuple([ascalar(customer.c_custkey),
                                   ascalar(customer.c_name),
                                   ascalar(customer.c_address),
                                   ascalar(customer.c_nationkey),
                                   ascalar(customer.c_phone),
                                   ascalar(customer.c_acctbal),
                                   ascalar(customer.c_mktsegment),
                                   ascalar(customer.c_comment),
                                   ascalar(orders.o_orderkey),
                                   ascalar(orders.o_custkey),
                                   ascalar(orders.o_orderstatus),
                                   ascalar(orders.o_totalprice),
                                   ascalar(orders.o_orderdate),
                                   ascalar(orders.o_orderpriority),
                                   ascalar(orders.o_clerk),
                                   ascalar(orders.o_shippriority),
                                   ascalar(orders.o_comment),
                                   ascalar(lineitem.l_orderkey),
                                   ascalar(lineitem.l_partkey),
                                   ascalar(lineitem.l_suppkey),
                                   ascalar(lineitem.l_linenumber),
                                   ascalar(lineitem.l_quantity),
                                   ascalar(lineitem.l_extendedprice),
                                   ascalar(lineitem.l_discount),
                                   ascalar(lineitem.l_tax),
                                   ascalar(lineitem.l_returnflag),
                                   ascalar(lineitem.l_linestatus),
                                   ascalar(lineitem.l_shipdate),
                                   ascalar(lineitem.l_commitdate),
                                   ascalar(lineitem.l_receiptdate),
                                   ascalar(lineitem.l_shipinstruct),
                                   ascalar(lineitem.l_shipmode),
                                   ascalar(lineitem.l_comment),
                                   ascalar(supplier.s_suppkey),
                                   ascalar(supplier.s_name),
                                   ascalar(supplier.s_address),
                                   ascalar(supplier.s_nationkey),
                                   ascalar(supplier.s_phone),
                                   ascalar(supplier.s_acctbal),
                                   ascalar(supplier.s_comment),
                                   ascalar(nation.n_nationkey),
                                   ascalar(nation.n_name),
                                   ascalar(nation.n_regionkey),
                                   ascalar(nation.n_comment)],
                             cross)))],
                 cross),
               param0))),
         (param1 + day(1)),
         ((param1 + year(1)) + day(1))))))|}
    in
    Logs.Src.set_level test (Some Logs.Debug) ;
    Logs.(set_reporter (format_reporter ())) ;
    project
      ~params:
        (Set.of_list
           (module Name)
           [ Name.create ~type_:(StringT {nullable= false}) "param0"
           ; Name.create ~type_:(DateT {nullable= false}) "param1" ])
      r
    |> Format.printf "%a@." pp
end
