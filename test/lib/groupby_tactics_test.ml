open Abslayout
open Abslayout_load

module C = struct
  let conn = Db.create "postgresql:///tpch_1k"
  let cost_conn = conn
  let verbose = false
  let validate = false

  let params =
    let open Prim_type in
    Set.of_list
      (module Name)
      [
        Name.create ~type_:string_t "param1";
        Name.create ~type_:string_t "param2";
        Name.create ~type_:string_t "param3";
      ]

  let param_ctx = Map.empty (module Name)
  let fresh = Fresh.create ()
  let simplify = None
end

open C
open Groupby_tactics.Make (C)
open Ops.Make (C)
open Tactics_util.Make (C)

let with_logs src f =
  Logs.(set_reporter (format_reporter ()));
  Logs.Src.set_level src (Some Debug);
  let ret = f () in
  Logs.Src.set_level src (Some Error);
  Logs.(set_reporter nop_reporter);
  ret

let%expect_test "" =
  let r =
    load_string_exn ~params conn
      {|
groupby([o_year,
         (sum((if (nation_name = param1) then volume else 0.0)) /
         sum(volume)) as mkt_share],
  [o_year],
  select([to_year(o_orderdate) as o_year,
          (l_extendedprice * (1 - l_discount)) as volume,
          n2_name as nation_name],
    join((p_partkey = l_partkey),
      join((s_suppkey = l_suppkey),
        join((l_orderkey = o_orderkey),
          join((o_custkey = c_custkey),
            join((c_nationkey = n1_nationkey),
              join((n1_regionkey = r_regionkey),
                select([n_regionkey as n1_regionkey, n_nationkey as n1_nationkey],
                  nation),
                filter((r_name = param2), region)),
              customer),
            filter(((o_orderdate >= date("1995-01-01")) &&
                   (o_orderdate <= date("1996-12-31"))),
              orders)),
          lineitem),
        join((s_nationkey = n2_nationkey),
          select([n_nationkey as n2_nationkey, n_name as n2_name],
            nation),
          supplier)),
      filter((p_type = param3), part))))
|}
  in
  ( with_logs Groupby_tactics.src @@ fun () ->
    with_logs Join_elim.src @@ fun () ->
    apply elim_groupby Path.root r |> Option.iter ~f:(Fmt.pr "%a@." pp) );
  [%expect
    {|
      alist(dedup(
              select([to_year(o_orderdate) as o_year],
                dedup(select([o_orderdate], orders)))) as k0,
        select([o_year,
                (sum((if (nation_name = param1) then volume else 0.0)) / sum(volume)) as mkt_share],
          filter((o_year = k0.o_year),
            select([to_year(o_orderdate) as o_year,
                    (l_extendedprice * (1 - l_discount)) as volume,
                    n2_name as nation_name],
              join((p_partkey = l_partkey),
                join((s_suppkey = l_suppkey),
                  join((l_orderkey = o_orderkey),
                    join((o_custkey = c_custkey),
                      join((c_nationkey = n1_nationkey),
                        join((n1_regionkey = r_regionkey),
                          select([n_regionkey as n1_regionkey,
                                  n_nationkey as n1_nationkey],
                            nation),
                          filter((r_name = param2), region)),
                        customer),
                      filter(((o_orderdate >= date("1995-01-01")) &&
                             (o_orderdate <= date("1996-12-31"))),
                        orders)),
                    lineitem),
                  join((s_nationkey = n2_nationkey),
                    select([n_nationkey as n2_nationkey, n_name as n2_name],
                      nation),
                    supplier)),
                filter((p_type = param3), part)))))) |}]

let%expect_test "" =
  let params =
    let open Prim_type in
    Set.of_list
      (module Name)
      [
        Name.create ~type_:string_t "param0"; Name.create ~type_:date_t "param1";
      ]
  in
  let module C = struct
    let conn = Db.create "postgresql:///tpch_1k"
    let cost_conn = conn
    let params = params
  end in
  let open Groupby_tactics.Make (C) in
  let open Ops.Make (C) in
  let r =
    load_string_exn ~params conn
      {|
groupby([l_orderkey, sum((l_extendedprice * (1 - l_discount))) as revenue, o_orderdate, o_shippriority],
    [l_orderkey, o_orderdate, o_shippriority],
    join((c_custkey = o_custkey),
      join((l_orderkey = o_orderkey),
        filter((o_orderdate < param1),
          alist(orders as s1,
            atuple([ascalar(s1.o_orderkey), ascalar(s1.o_custkey), ascalar(s1.o_orderstatus), 
                    ascalar(s1.o_totalprice), ascalar(s1.o_orderdate), 
                    ascalar(s1.o_orderpriority), ascalar(s1.o_clerk), 
                    ascalar(s1.o_shippriority), ascalar(s1.o_comment)],
              cross))),
        filter((l_shipdate > param1),
          alist(lineitem as s2,
            atuple([ascalar(s2.l_orderkey), ascalar(s2.l_partkey), ascalar(s2.l_suppkey), 
                    ascalar(s2.l_linenumber), ascalar(s2.l_quantity), 
                    ascalar(s2.l_extendedprice), ascalar(s2.l_discount), 
                    ascalar(s2.l_tax), ascalar(s2.l_returnflag), ascalar(s2.l_linestatus), 
                    ascalar(s2.l_shipdate), ascalar(s2.l_commitdate), 
                    ascalar(s2.l_receiptdate), ascalar(s2.l_shipinstruct), 
                    ascalar(s2.l_shipmode), ascalar(s2.l_comment)],
              cross)))),
      filter((c_mktsegment = param0),
        alist(customer as s0,
          atuple([ascalar(s0.c_custkey), ascalar(s0.c_name), ascalar(s0.c_address), 
                  ascalar(s0.c_nationkey), ascalar(s0.c_phone), ascalar(s0.c_acctbal), 
                  ascalar(s0.c_mktsegment), ascalar(s0.c_comment)],
            cross)))))
|}
  in
  apply elim_groupby Path.root r |> Option.iter ~f:(Fmt.pr "%a@." pp);
  [%expect
    {|
    alist(dedup(
            dedup(
              select([l_orderkey, o_orderdate, o_shippriority],
                join((c_custkey = o_custkey),
                  join((l_orderkey = o_orderkey),
                    alist(orders as s1,
                      atuple([ascalar(s1.o_orderkey), ascalar(s1.o_custkey),
                              ascalar(s1.o_orderstatus),
                              ascalar(s1.o_totalprice), ascalar(s1.o_orderdate),
                              ascalar(s1.o_orderpriority), ascalar(s1.o_clerk),
                              ascalar(s1.o_shippriority), ascalar(s1.o_comment)],
                        cross)),
                    alist(lineitem as s2,
                      atuple([ascalar(s2.l_orderkey), ascalar(s2.l_partkey),
                              ascalar(s2.l_suppkey), ascalar(s2.l_linenumber),
                              ascalar(s2.l_quantity),
                              ascalar(s2.l_extendedprice),
                              ascalar(s2.l_discount), ascalar(s2.l_tax),
                              ascalar(s2.l_returnflag), ascalar(s2.l_linestatus),
                              ascalar(s2.l_shipdate), ascalar(s2.l_commitdate),
                              ascalar(s2.l_receiptdate),
                              ascalar(s2.l_shipinstruct), ascalar(s2.l_shipmode),
                              ascalar(s2.l_comment)],
                        cross))),
                  alist(customer as s0,
                    atuple([ascalar(s0.c_custkey), ascalar(s0.c_name),
                            ascalar(s0.c_address), ascalar(s0.c_nationkey),
                            ascalar(s0.c_phone), ascalar(s0.c_acctbal),
                            ascalar(s0.c_mktsegment), ascalar(s0.c_comment)],
                      cross)))))) as k1,
      select([l_orderkey, sum((l_extendedprice * (1 - l_discount))) as revenue,
              o_orderdate, o_shippriority],
        filter(((l_orderkey = k1.l_orderkey) &&
               ((o_orderdate = k1.o_orderdate) &&
               (o_shippriority = k1.o_shippriority))),
          join((c_custkey = o_custkey),
            join((l_orderkey = o_orderkey),
              filter((o_orderdate < param1),
                alist(orders as s1,
                  atuple([ascalar(s1.o_orderkey), ascalar(s1.o_custkey),
                          ascalar(s1.o_orderstatus), ascalar(s1.o_totalprice),
                          ascalar(s1.o_orderdate), ascalar(s1.o_orderpriority),
                          ascalar(s1.o_clerk), ascalar(s1.o_shippriority),
                          ascalar(s1.o_comment)],
                    cross))),
              filter((l_shipdate > param1),
                alist(lineitem as s2,
                  atuple([ascalar(s2.l_orderkey), ascalar(s2.l_partkey),
                          ascalar(s2.l_suppkey), ascalar(s2.l_linenumber),
                          ascalar(s2.l_quantity), ascalar(s2.l_extendedprice),
                          ascalar(s2.l_discount), ascalar(s2.l_tax),
                          ascalar(s2.l_returnflag), ascalar(s2.l_linestatus),
                          ascalar(s2.l_shipdate), ascalar(s2.l_commitdate),
                          ascalar(s2.l_receiptdate), ascalar(s2.l_shipinstruct),
                          ascalar(s2.l_shipmode), ascalar(s2.l_comment)],
                    cross)))),
            filter((c_mktsegment = param0),
              alist(customer as s0,
                atuple([ascalar(s0.c_custkey), ascalar(s0.c_name),
                        ascalar(s0.c_address), ascalar(s0.c_nationkey),
                        ascalar(s0.c_phone), ascalar(s0.c_acctbal),
                        ascalar(s0.c_mktsegment), ascalar(s0.c_comment)],
                  cross))))))) |}]

let%expect_test "" =
  let params =
    Set.of_list (module Name)
    @@ List.init 7 ~f:(fun i -> Name.create @@ sprintf "param%d" i)
  in
  let r =
    load_string_exn ~params conn
      {|
select([substring(c1_phone, 0, 2) as cntrycode],
    filter(((substring(c1_phone, 0, 2) = param0) ||
           ((substring(c1_phone, 0, 2) = param1) ||
           ((substring(c1_phone, 0, 2) = param2) ||
           ((substring(c1_phone, 0, 2) = param3) ||
           ((substring(c1_phone, 0, 2) = param4) ||
           ((substring(c1_phone, 0, 2) = param5) ||
           (substring(c1_phone, 0, 2) = param6))))))),
      filter((c1_acctbal >
             (select([avg(c_acctbal) as avgbal],
                filter(((c_acctbal > 0.0) &&
                       ((substring(c_phone, 0, 2) = param0) ||
                       ((substring(c_phone, 0, 2) = param1) ||
                       ((substring(c_phone, 0, 2) = param2) ||
                       ((substring(c_phone, 0, 2) = param3) ||
                       ((substring(c_phone, 0, 2) = param4) ||
                       ((substring(c_phone, 0, 2) = param5) ||
                       (substring(c_phone, 0, 2) = param6)))))))),
                  customer)))),
        filter(not(exists(filter((o_custkey = c1_custkey), orders))),
          select([c_phone as c1_phone, c_acctbal as c1_acctbal, c_custkey as c1_custkey], customer)))))
|}
  in
  let ps = match r.node with Select (ps, _) -> ps | _ -> assert false in
  all_values_approx ps r |> Result.iter ~f:(Fmt.pr "%a@." pp);
  [%expect
    {|
    select([substring(c_phone, 0, 2) as cntrycode],
      dedup(select([c_phone], customer))) |}]
