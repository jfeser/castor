open Core
open Abslayout
open Name
open Resolve

module C = struct
  let conn = Db.create "postgresql:///tpch_1k"
end

module M = Abslayout_db.Make (C)
module T = Make (C)
open T

let pp, _ = mk_pp ~pp_name:pp_with_stage ()

let pp_with_refcount, _ =
  mk_pp ~pp_name:pp_with_stage
    ~pp_meta:(fun fmt meta ->
      let open Format in
      match Univ_map.find meta refcnt with
      | Some r ->
          fprintf fmt "@[<hv 2>{" ;
          Map.iteri r ~f:(fun ~key:n ~data:c ->
              if c > 0 then fprintf fmt "%a=%d,@ " Name.pp n c ) ;
          fprintf fmt "}@]"
      | None -> () )
    ()

let%expect_test "" =
  let r =
    {|
      select([l_receiptdate],
                         alist(join((o_orderkey = l_orderkey),
                                 lineitem,
                                 orders) as k,
                           atuple([ascalar(k.l_orderkey),
                                   ascalar(k.l_commitdate),
                                   ascalar(k.l_receiptdate),
                                   ascalar(k.o_comment)],
                             cross)))
    |}
    |> M.load_string
  in
  Format.printf "%a@." pp_with_refcount r ;
  [%expect
    {|
      select([k.l_receiptdate@run],
        alist(join((orders.o_orderkey@comp = lineitem.l_orderkey@comp),
                lineitem#,
                orders#)# as k#,
          atuple([ascalar(k.l_orderkey@comp)#,
                  ascalar(k.l_commitdate@comp)#,
                  ascalar(k.l_receiptdate@comp)#,
                  ascalar(k.o_comment@comp)#],
            cross)#)#)# |}]

let%expect_test "" =
  let r =
    {|
      alist(orderby([l_shipmode],
         select([lineitem.l_shipmode],
           dedup(select([lineitem.l_shipmode], lineitem)))) as k0,
   select([lineitem.l_shipmode,
           sum((if ((orders.o_orderpriority = "1-URGENT") ||
                   (orders.o_orderpriority = "2-HIGH")) then 1 else 0)) as high_line_count,
           sum((if (not((orders.o_orderpriority = "1-URGENT")) &&
                   not((orders.o_orderpriority = "2-HIGH"))) then 1 else 0)) as low_line_count],
     filter(((lineitem.l_shipmode = k0.l_shipmode) &&
            (((lineitem.l_shipmode = param1) ||
             (lineitem.l_shipmode = param2)) &&
            ((lineitem.l_commitdate < lineitem.l_receiptdate) &&
            ((lineitem.l_shipdate < lineitem.l_commitdate) &&
            ((lineitem.l_receiptdate >= param3) &&
            (lineitem.l_receiptdate < (param3 + year(1)))))))),
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
           cross)))))
|}
    |> M.load_string
         ~params:
           (Set.of_list
              (module Name)
              [ create ~type_:(StringT {nullable= false}) "param1"
              ; create ~type_:(StringT {nullable= false}) "param2"
              ; create ~type_:(DateT {nullable= false}) "param3" ])
  in
  Format.printf "%a@." pp r ;
  [%expect
    {|
      alist(orderby([lineitem.l_shipmode@comp],
              select([lineitem.l_shipmode@comp],
                dedup(select([lineitem.l_shipmode@comp], lineitem)))) as k0,
        select([lineitem.l_shipmode@run,
                sum((if ((orders.o_orderpriority@run = "1-URGENT") ||
                        (orders.o_orderpriority@run = "2-HIGH")) then 1 else 0)) as high_line_count,
                sum((if (not((orders.o_orderpriority@run = "1-URGENT")) &&
                        not((orders.o_orderpriority@run = "2-HIGH"))) then 1 else 0)) as low_line_count],
          filter(((lineitem.l_shipmode@run = k0.l_shipmode@comp) &&
                 (((lineitem.l_shipmode@run = param1@run) ||
                  (lineitem.l_shipmode@run = param2@run)) &&
                 ((lineitem.l_commitdate@run < lineitem.l_receiptdate@run) &&
                 ((lineitem.l_shipdate@run < lineitem.l_commitdate@run) &&
                 ((lineitem.l_receiptdate@run >= param3@run) &&
                 (lineitem.l_receiptdate@run < (param3@run + year(1)))))))),
            alist(join((orders.o_orderkey@comp = lineitem.l_orderkey@comp),
                    lineitem,
                    orders),
              atuple([ascalar(lineitem.l_orderkey@comp),
                      ascalar(lineitem.l_partkey@comp),
                      ascalar(lineitem.l_suppkey@comp),
                      ascalar(lineitem.l_linenumber@comp),
                      ascalar(lineitem.l_quantity@comp),
                      ascalar(lineitem.l_extendedprice@comp),
                      ascalar(lineitem.l_discount@comp),
                      ascalar(lineitem.l_tax@comp),
                      ascalar(lineitem.l_returnflag@comp),
                      ascalar(lineitem.l_linestatus@comp),
                      ascalar(lineitem.l_shipdate@comp),
                      ascalar(lineitem.l_commitdate@comp),
                      ascalar(lineitem.l_receiptdate@comp),
                      ascalar(lineitem.l_shipinstruct@comp),
                      ascalar(lineitem.l_shipmode@comp),
                      ascalar(lineitem.l_comment@comp),
                      ascalar(orders.o_orderkey@comp),
                      ascalar(orders.o_custkey@comp),
                      ascalar(orders.o_orderstatus@comp),
                      ascalar(orders.o_totalprice@comp),
                      ascalar(orders.o_orderdate@comp),
                      ascalar(orders.o_orderpriority@comp),
                      ascalar(orders.o_clerk@comp),
                      ascalar(orders.o_shippriority@comp),
                      ascalar(orders.o_comment@comp)],
                cross))))) |}]

let%expect_test "" =
  let r =
    M.load_string
      ~params:
        (Set.of_list
           (module Name)
           [ create ~type_:(StringT {nullable= false}) "param0"
           ; create ~type_:(DateT {nullable= false}) "param1" ])
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
  Format.printf "%a@." pp r ;
  [%expect
    {|
      select([nation.n_name@run, revenue@run],
        alist(select([nation.n_name@comp],
                dedup(select([nation.n_name@comp], nation))) as k0,
          select([nation.n_name@run, sum(agg3@run) as revenue],
            aorderedidx(dedup(
                          select([orders.o_orderdate@comp as k2],
                            dedup(select([orders.o_orderdate@comp], orders)))),
              filter((count4@run > 0),
                select([count() as count4,
                        sum((lineitem.l_extendedprice@run *
                            (1 - lineitem.l_discount@run))) as agg3,
                        k1@comp,
                        region.r_regionkey@run,
                        region.r_name@run,
                        region.r_comment@run,
                        customer.c_custkey@run,
                        customer.c_name@run,
                        customer.c_address@run,
                        customer.c_nationkey@run,
                        customer.c_phone@run,
                        customer.c_acctbal@run,
                        customer.c_mktsegment@run,
                        customer.c_comment@run,
                        orders.o_orderkey@run,
                        orders.o_custkey@run,
                        orders.o_orderstatus@run,
                        orders.o_totalprice@run,
                        orders.o_orderdate@run,
                        orders.o_orderpriority@run,
                        orders.o_clerk@run,
                        orders.o_shippriority@run,
                        orders.o_comment@run,
                        lineitem.l_orderkey@run,
                        lineitem.l_partkey@run,
                        lineitem.l_suppkey@run,
                        lineitem.l_linenumber@run,
                        lineitem.l_quantity@run,
                        lineitem.l_extendedprice@run,
                        lineitem.l_discount@run,
                        lineitem.l_tax@run,
                        lineitem.l_returnflag@run,
                        lineitem.l_linestatus@run,
                        lineitem.l_shipdate@run,
                        lineitem.l_commitdate@run,
                        lineitem.l_receiptdate@run,
                        lineitem.l_shipinstruct@run,
                        lineitem.l_shipmode@run,
                        lineitem.l_comment@run,
                        supplier.s_suppkey@run,
                        supplier.s_name@run,
                        supplier.s_address@run,
                        supplier.s_nationkey@run,
                        supplier.s_phone@run,
                        supplier.s_acctbal@run,
                        supplier.s_comment@run,
                        nation.n_nationkey@run,
                        nation.n_name@run,
                        nation.n_regionkey@run,
                        nation.n_comment@run],
                  ahashidx(dedup(
                             select([region.r_name@comp as k1],
                               atuple([alist(region,
                                         atuple([ascalar(region.r_regionkey@comp),
                                                 ascalar(region.r_name@comp),
                                                 ascalar(region.r_comment@comp)],
                                           cross)),
                                       filter((nation.n_regionkey@comp =
                                              region.r_regionkey@comp),
                                         alist(join((supplier.s_nationkey@comp =
                                                    nation.n_nationkey@comp),
                                                 join(((lineitem.l_suppkey@comp =
                                                       supplier.s_suppkey@comp) &&
                                                      (customer.c_nationkey@comp =
                                                      supplier.s_nationkey@comp)),
                                                   join((lineitem.l_orderkey@comp =
                                                        orders.o_orderkey@comp),
                                                     join((customer.c_custkey@comp
                                                          = orders.o_custkey@comp),
                                                       customer,
                                                       orders),
                                                     lineitem),
                                                   supplier),
                                                 nation),
                                           atuple([ascalar(customer.c_custkey@comp),
                                                   ascalar(customer.c_name@comp),
                                                   ascalar(customer.c_address@comp),
                                                   ascalar(customer.c_nationkey@comp),
                                                   ascalar(customer.c_phone@comp),
                                                   ascalar(customer.c_acctbal@comp),
                                                   ascalar(customer.c_mktsegment@comp),
                                                   ascalar(customer.c_comment@comp),
                                                   ascalar(orders.o_orderkey@comp),
                                                   ascalar(orders.o_custkey@comp),
                                                   ascalar(orders.o_orderstatus@comp),
                                                   ascalar(orders.o_totalprice@comp),
                                                   ascalar(orders.o_orderdate@comp),
                                                   ascalar(orders.o_orderpriority@comp),
                                                   ascalar(orders.o_clerk@comp),
                                                   ascalar(orders.o_shippriority@comp),
                                                   ascalar(orders.o_comment@comp),
                                                   ascalar(lineitem.l_orderkey@comp),
                                                   ascalar(lineitem.l_partkey@comp),
                                                   ascalar(lineitem.l_suppkey@comp),
                                                   ascalar(lineitem.l_linenumber@comp),
                                                   ascalar(lineitem.l_quantity@comp),
                                                   ascalar(lineitem.l_extendedprice@comp),
                                                   ascalar(lineitem.l_discount@comp),
                                                   ascalar(lineitem.l_tax@comp),
                                                   ascalar(lineitem.l_returnflag@comp),
                                                   ascalar(lineitem.l_linestatus@comp),
                                                   ascalar(lineitem.l_shipdate@comp),
                                                   ascalar(lineitem.l_commitdate@comp),
                                                   ascalar(lineitem.l_receiptdate@comp),
                                                   ascalar(lineitem.l_shipinstruct@comp),
                                                   ascalar(lineitem.l_shipmode@comp),
                                                   ascalar(lineitem.l_comment@comp),
                                                   ascalar(supplier.s_suppkey@comp),
                                                   ascalar(supplier.s_name@comp),
                                                   ascalar(supplier.s_address@comp),
                                                   ascalar(supplier.s_nationkey@comp),
                                                   ascalar(supplier.s_phone@comp),
                                                   ascalar(supplier.s_acctbal@comp),
                                                   ascalar(supplier.s_comment@comp),
                                                   ascalar(nation.n_nationkey@comp),
                                                   ascalar(nation.n_name@comp),
                                                   ascalar(nation.n_regionkey@comp),
                                                   ascalar(nation.n_comment@comp)],
                                             cross)))],
                                 cross))),
                    atuple([alist(filter((k1@comp = region.r_name@comp), region),
                              atuple([ascalar(region.r_regionkey@comp),
                                      ascalar(region.r_name@comp),
                                      ascalar(region.r_comment@comp)],
                                cross)),
                            filter((nation.n_regionkey@run =
                                   region.r_regionkey@run),
                              alist(filter(((nation.n_name@comp = k0.n_name@comp)
                                           && (k2@comp = orders.o_orderdate@comp)),
                                      join((supplier.s_nationkey@comp =
                                           nation.n_nationkey@comp),
                                        join(((lineitem.l_suppkey@comp =
                                              supplier.s_suppkey@comp) &&
                                             (customer.c_nationkey@comp =
                                             supplier.s_nationkey@comp)),
                                          join((lineitem.l_orderkey@comp =
                                               orders.o_orderkey@comp),
                                            join((customer.c_custkey@comp =
                                                 orders.o_custkey@comp),
                                              customer,
                                              orders),
                                            lineitem),
                                          supplier),
                                        nation)),
                                atuple([ascalar(customer.c_custkey@comp),
                                        ascalar(customer.c_name@comp),
                                        ascalar(customer.c_address@comp),
                                        ascalar(customer.c_nationkey@comp),
                                        ascalar(customer.c_phone@comp),
                                        ascalar(customer.c_acctbal@comp),
                                        ascalar(customer.c_mktsegment@comp),
                                        ascalar(customer.c_comment@comp),
                                        ascalar(orders.o_orderkey@comp),
                                        ascalar(orders.o_custkey@comp),
                                        ascalar(orders.o_orderstatus@comp),
                                        ascalar(orders.o_totalprice@comp),
                                        ascalar(orders.o_orderdate@comp),
                                        ascalar(orders.o_orderpriority@comp),
                                        ascalar(orders.o_clerk@comp),
                                        ascalar(orders.o_shippriority@comp),
                                        ascalar(orders.o_comment@comp),
                                        ascalar(lineitem.l_orderkey@comp),
                                        ascalar(lineitem.l_partkey@comp),
                                        ascalar(lineitem.l_suppkey@comp),
                                        ascalar(lineitem.l_linenumber@comp),
                                        ascalar(lineitem.l_quantity@comp),
                                        ascalar(lineitem.l_extendedprice@comp),
                                        ascalar(lineitem.l_discount@comp),
                                        ascalar(lineitem.l_tax@comp),
                                        ascalar(lineitem.l_returnflag@comp),
                                        ascalar(lineitem.l_linestatus@comp),
                                        ascalar(lineitem.l_shipdate@comp),
                                        ascalar(lineitem.l_commitdate@comp),
                                        ascalar(lineitem.l_receiptdate@comp),
                                        ascalar(lineitem.l_shipinstruct@comp),
                                        ascalar(lineitem.l_shipmode@comp),
                                        ascalar(lineitem.l_comment@comp),
                                        ascalar(supplier.s_suppkey@comp),
                                        ascalar(supplier.s_name@comp),
                                        ascalar(supplier.s_address@comp),
                                        ascalar(supplier.s_nationkey@comp),
                                        ascalar(supplier.s_phone@comp),
                                        ascalar(supplier.s_acctbal@comp),
                                        ascalar(supplier.s_comment@comp),
                                        ascalar(nation.n_nationkey@comp),
                                        ascalar(nation.n_name@comp),
                                        ascalar(nation.n_regionkey@comp),
                                        ascalar(nation.n_comment@comp)],
                                  cross)))],
                      cross),
                    param0@run))),
              (param1@run + day(1)),
              ((param1@run + year(1)) + day(1)))))) |}]

let%expect_test "" =
  let r =
    M.load_string
      {|
  alist(select([n1_name,
                                                           n2_name,
                                                           l_year,
                                                           revenue],
                                                     orderby([n1_name,
                                                              n2_name,
                                                              l_year],
                                                       groupby([n1_name,
                                                                n2_name,
                                                                l_year,
                                                                sum(volume) as revenue],
                                                         [n1_name,
                                                          n2_name,
                                                          l_year],
                                                         select([n1_name,
                                                                 n2_name,
                                                                 to_year(lineitem.l_shipdate) as l_year,
                                                                 (lineitem.l_extendedprice
                                                                 *
                                                                 (1 -
                                                                 lineitem.l_discount)) as volume],
                                                           atuple([alist(
                                                                    select(
                                                                    [nation.n_name as n1_name,
                                                                    nation.n_nationkey as n1_nationkey],
                                                                    nation),
                                                                    atuple(
                                                                    [ascalar(n1_name),
                                                                    ascalar(n1_nationkey)],
                                                                    cross)),
                                                                   filter(
                                                                    (supplier.s_nationkey
                                                                    =
                                                                    n1_nationkey),
                                                                    alist(
                                                                    join(
                                                                    (customer.c_nationkey
                                                                    =
                                                                    n2_nationkey),
                                                                    join(
                                                                    (supplier.s_suppkey
                                                                    =
                                                                    lineitem.l_suppkey),
                                                                    join(
                                                                    (customer.c_custkey
                                                                    =
                                                                    orders.o_custkey),
                                                                    join(
                                                                    (orders.o_orderkey
                                                                    =
                                                                    lineitem.l_orderkey),
                                                                    filter(
                                                                    (
                                                                    (lineitem.l_shipdate
                                                                    >=
                                                                    date("1995-01-01"))
                                                                    &&
                                                                    (lineitem.l_shipdate
                                                                    <=
                                                                    date("1996-12-31"))),
                                                                    lineitem),
                                                                    orders),
                                                                    customer),
                                                                    supplier),
                                                                    select(
                                                                    [nation.n_name as n2_name,
                                                                    nation.n_nationkey as n2_nationkey],
                                                                    nation)),
                                                                    atuple(
                                                                    [ascalar(lineitem.l_orderkey),
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
                                                                    ascalar(orders.o_comment),
                                                                    ascalar(customer.c_custkey),
                                                                    ascalar(customer.c_name),
                                                                    ascalar(customer.c_address),
                                                                    ascalar(customer.c_nationkey),
                                                                    ascalar(customer.c_phone),
                                                                    ascalar(customer.c_acctbal),
                                                                    ascalar(customer.c_mktsegment),
                                                                    ascalar(customer.c_comment),
                                                                    ascalar(supplier.s_suppkey),
                                                                    ascalar(supplier.s_name),
                                                                    ascalar(supplier.s_address),
                                                                    ascalar(supplier.s_nationkey),
                                                                    ascalar(supplier.s_phone),
                                                                    ascalar(supplier.s_acctbal),
                                                                    ascalar(supplier.s_comment),
                                                                    ascalar(n2_name),
                                                                    ascalar(n2_nationkey)],
                                                                    cross)))],
                                                             cross))))) as k,
                                               atuple([ascalar(k.n1_name),
                                                       ascalar(k.n2_name),
                                                       ascalar(k.l_year),
                                                       ascalar(k.revenue)],
                                                 cross))
|}
  in
  Format.printf "%a@." pp r ;
  [%expect
    {|
      alist(select([n1_name@comp, n2_name@comp, l_year@comp, revenue@comp],
              orderby([n1_name@comp, n2_name@comp, l_year@comp],
                groupby([n1_name@comp,
                         n2_name@comp,
                         l_year@comp,
                         sum(volume@comp) as revenue],
                  [n1_name@comp, n2_name@comp, l_year@comp],
                  select([n1_name@comp,
                          n2_name@comp,
                          to_year(lineitem.l_shipdate@comp) as l_year,
                          (lineitem.l_extendedprice@comp *
                          (1 - lineitem.l_discount@comp)) as volume],
                    atuple([alist(select([nation.n_name@comp as n1_name,
                                          nation.n_nationkey@comp as n1_nationkey],
                                    nation),
                              atuple([ascalar(n1_name@comp),
                                      ascalar(n1_nationkey@comp)],
                                cross)),
                            filter((supplier.s_nationkey@comp = n1_nationkey@comp),
                              alist(join((customer.c_nationkey@comp =
                                         n2_nationkey@comp),
                                      join((supplier.s_suppkey@comp =
                                           lineitem.l_suppkey@comp),
                                        join((customer.c_custkey@comp =
                                             orders.o_custkey@comp),
                                          join((orders.o_orderkey@comp =
                                               lineitem.l_orderkey@comp),
                                            filter(((lineitem.l_shipdate@comp >=
                                                    date("1995-01-01")) &&
                                                   (lineitem.l_shipdate@comp <=
                                                   date("1996-12-31"))),
                                              lineitem),
                                            orders),
                                          customer),
                                        supplier),
                                      select([nation.n_name@comp as n2_name,
                                              nation.n_nationkey@comp as n2_nationkey],
                                        nation)),
                                atuple([ascalar(lineitem.l_orderkey@comp),
                                        ascalar(lineitem.l_partkey@comp),
                                        ascalar(lineitem.l_suppkey@comp),
                                        ascalar(lineitem.l_linenumber@comp),
                                        ascalar(lineitem.l_quantity@comp),
                                        ascalar(lineitem.l_extendedprice@comp),
                                        ascalar(lineitem.l_discount@comp),
                                        ascalar(lineitem.l_tax@comp),
                                        ascalar(lineitem.l_returnflag@comp),
                                        ascalar(lineitem.l_linestatus@comp),
                                        ascalar(lineitem.l_shipdate@comp),
                                        ascalar(lineitem.l_commitdate@comp),
                                        ascalar(lineitem.l_receiptdate@comp),
                                        ascalar(lineitem.l_shipinstruct@comp),
                                        ascalar(lineitem.l_shipmode@comp),
                                        ascalar(lineitem.l_comment@comp),
                                        ascalar(orders.o_orderkey@comp),
                                        ascalar(orders.o_custkey@comp),
                                        ascalar(orders.o_orderstatus@comp),
                                        ascalar(orders.o_totalprice@comp),
                                        ascalar(orders.o_orderdate@comp),
                                        ascalar(orders.o_orderpriority@comp),
                                        ascalar(orders.o_clerk@comp),
                                        ascalar(orders.o_shippriority@comp),
                                        ascalar(orders.o_comment@comp),
                                        ascalar(customer.c_custkey@comp),
                                        ascalar(customer.c_name@comp),
                                        ascalar(customer.c_address@comp),
                                        ascalar(customer.c_nationkey@comp),
                                        ascalar(customer.c_phone@comp),
                                        ascalar(customer.c_acctbal@comp),
                                        ascalar(customer.c_mktsegment@comp),
                                        ascalar(customer.c_comment@comp),
                                        ascalar(supplier.s_suppkey@comp),
                                        ascalar(supplier.s_name@comp),
                                        ascalar(supplier.s_address@comp),
                                        ascalar(supplier.s_nationkey@comp),
                                        ascalar(supplier.s_phone@comp),
                                        ascalar(supplier.s_acctbal@comp),
                                        ascalar(supplier.s_comment@comp),
                                        ascalar(n2_name@comp),
                                        ascalar(n2_nationkey@comp)],
                                  cross)))],
                      cross))))) as k,
        atuple([ascalar(k.n1_name@comp),
                ascalar(k.n2_name@comp),
                ascalar(k.l_year@comp),
                ascalar(k.revenue@comp)],
          cross)) |}]
