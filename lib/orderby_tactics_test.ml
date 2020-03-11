open Castor_test.Test_util
module A = Abslayout

module Config = struct
  let cost_conn = Db.create "postgresql:///tpch_1k"

  let conn = cost_conn

  let validate = false

  let param_ctx = Map.empty (module Name)

  let params = Set.empty (module Name)

  let verbose = false

  let simplify = None
end

open Orderby_tactics.Make (Config)

module O = Ops.Make (Config)

let%expect_test "" =
  let r =
    {|
    orderby([s_suppkey desc],
      depjoin(alist(dedup(select([l_suppkey as l1_suppkey], lineitem)) as k0,
                select([l_suppkey as supplier_no, sum(agg0) as total_revenue],
                  aorderedidx(dedup(select([l_shipdate], lineitem)) as s4,
                    filter((count0 > 0),
                      select([count() as count0, sum((l_extendedprice * (1 - l_discount))) as agg0, 
                              l_suppkey, l_extendedprice, l_discount],
                        atuple([ascalar(s4.l_shipdate),
                                alist(select([l_suppkey, l_extendedprice, l_discount],
                                        filter(((l_suppkey = k0.l1_suppkey) && (l_shipdate = s4.l_shipdate)), lineitem)) as s5,
                                  atuple([ascalar(s5.l_suppkey), ascalar(s5.l_extendedprice), ascalar(s5.l_discount)],
                                    cross))],
                          cross))),
                    >= date("0000-01-01"), < (date("0000-01-01") + month(3))))) as s1,
        select([s_address, s_name, s_phone, s_suppkey, s1.total_revenue],
          ahashidx(dedup(select([s_suppkey], supplier)) as s2,
            alist(select([s_suppkey, s_name, s_address, s_phone], filter((s2.s_suppkey = s_suppkey), supplier)) as s0,
              atuple([ascalar(s0.s_suppkey), ascalar(s0.s_name), ascalar(s0.s_address), ascalar(s0.s_phone)], cross)),
            s1.supplier_no))))
|}
    |> Abslayout_load.load_string (Lazy.force tpch_conn)
  in
  Format.printf "%a" (Fmt.Dump.option A.pp) @@ O.apply push_orderby Path.root r;
  [%expect {|
    Some
      depjoin(orderby([supplier_no desc],
                alist(dedup(select([l_suppkey as l1_suppkey], lineitem)) as k0,
                  select([l_suppkey as supplier_no, sum(agg0) as total_revenue],
                    aorderedidx(dedup(select([l_shipdate], lineitem)) as s4,
                      filter((count0 > 0),
                        select([count() as count0,
                                sum((l_extendedprice * (1 - l_discount))) as agg0,
                                l_suppkey, l_extendedprice, l_discount],
                          atuple([ascalar(s4.l_shipdate),
                                  alist(select([l_suppkey, l_extendedprice,
                                                l_discount],
                                          filter(((l_suppkey = k0.l1_suppkey) &&
                                                 (l_shipdate = s4.l_shipdate)),
                                            lineitem)) as s5,
                                    atuple([ascalar(s5.l_suppkey),
                                            ascalar(s5.l_extendedprice),
                                            ascalar(s5.l_discount)],
                                      cross))],
                            cross))),
                      >= date("0000-01-01"), < (date("0000-01-01") + month(3)))))) as s1,
        select([s_address, s_name, s_phone, s_suppkey, s1.total_revenue],
          ahashidx(dedup(select([s_suppkey], supplier)) as s2,
            alist(select([s_suppkey, s_name, s_address, s_phone],
                    filter((s2.s_suppkey = s_suppkey), supplier)) as s0,
              atuple([ascalar(s0.s_suppkey), ascalar(s0.s_name),
                      ascalar(s0.s_address), ascalar(s0.s_phone)],
                cross)),
            s1.supplier_no))) |}]

let%expect_test "" =
  let r =
    {|
orderby([supplier_no desc],
                alist(dedup(select([l_suppkey as l1_suppkey], lineitem)) as k0,
                  select([l_suppkey as supplier_no, sum(agg0) as total_revenue],
                    aorderedidx(dedup(select([l_shipdate], lineitem)) as s4,
                      filter((count0 > 0),
                        select([count() as count0,
                                sum((l_extendedprice * (1 - l_discount))) as agg0,
                                l_suppkey, l_extendedprice, l_discount],
                          atuple([ascalar(s4.l_shipdate),
                                  alist(select([l_suppkey, l_extendedprice,
                                                l_discount],
                                          filter(((l_suppkey = k0.l1_suppkey) &&
                                                 (l_shipdate = s4.l_shipdate)),
                                            lineitem)) as s5,
                                    atuple([ascalar(s5.l_suppkey),
                                            ascalar(s5.l_extendedprice),
                                            ascalar(s5.l_discount)],
                                      cross))],
                            cross))),
                      >= date("0000-01-01"), < (date("0000-01-01") + month(3))))))|}
    |> Abslayout_load.load_string (Lazy.force tpch_conn)
  in
  Format.printf "%a" (Fmt.Dump.option A.pp) @@ O.apply push_orderby Path.root r;
  [%expect {|
    Some
      alist(orderby([l1_suppkey desc],
              dedup(select([l_suppkey as l1_suppkey], lineitem))) as k0,
        select([l_suppkey as supplier_no, sum(agg0) as total_revenue],
          aorderedidx(dedup(select([l_shipdate], lineitem)) as s4,
            filter((count0 > 0),
              select([count() as count0,
                      sum((l_extendedprice * (1 - l_discount))) as agg0,
                      l_suppkey, l_extendedprice, l_discount],
                atuple([ascalar(s4.l_shipdate),
                        alist(select([l_suppkey, l_extendedprice, l_discount],
                                filter(((l_suppkey = k0.l1_suppkey) &&
                                       (l_shipdate = s4.l_shipdate)),
                                  lineitem)) as s5,
                          atuple([ascalar(s5.l_suppkey),
                                  ascalar(s5.l_extendedprice),
                                  ascalar(s5.l_discount)],
                            cross))],
                  cross))),
            >= date("0000-01-01"), < (date("0000-01-01") + month(3))))) |}]
