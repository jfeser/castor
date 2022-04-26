open Abslayout
open Simplify_tactic

module Config = struct
  let conn = Db.create "postgresql:///tpch_1k"
  let params = Set.empty (module Name)
  let param_ctx = Map.empty (module Name)
  let validate = false
  let simplify = None
end

module S = Make (Config)
open S
module O = Ops.Make (Config)
open O

let load_string ?params s = Abslayout_load.load_string_exn ?params Config.conn s

let%expect_test "" =
  let r =
    load_string
      {|
      depjoin(select([l_quantity as l_quantity,
                l_extendedprice as l_extendedprice,
                l_discount as l_discount,
                l_shipdate as l_shipdate],
          lineitem) as s3,
  select([s3.l_quantity as x77,
          s3.l_extendedprice as x78,
          s3.l_discount as x79,
          s3.l_shipdate as x80,
          l_extendedprice as x81,
          l_discount as x82],
    atuple([ascalar(s3.l_extendedprice as l_extendedprice), ascalar(s3.l_discount as l_discount)], cross)))
|}
  in
  Option.iter
    (apply elim_depjoin Path.root r)
    ~f:(Format.printf "%a" Abslayout.pp);
  [%expect
    {|
    select([l_quantity as x77, l_extendedprice as x78, l_discount as x79,
            l_shipdate as x80, l_extendedprice as x81, l_discount as x82],
      select([l_extendedprice as l_extendedprice, l_discount as l_discount,
              l_quantity, l_shipdate],
        select([l_quantity as l_quantity, l_extendedprice as l_extendedprice,
                l_discount as l_discount, l_shipdate as l_shipdate],
          lineitem))) |}]

let%expect_test "" =
  let r =
    load_string
      {| depjoin(ascalar(0 as f) as k, select([f], select([k.f], ascalar(0 as g)))) |}
  in
  Option.iter
    (apply
       (at_ flatten_select Path.(all >>? is_select >>| shallowest))
       Path.root r)
    ~f:(Format.printf "%a" Abslayout.pp);
  [%expect {| depjoin(ascalar(0 as f) as k, select([k.f], ascalar(0 as g))) |}]

let%expect_test "" =
  let r =
    load_string
      {|
groupby([min(ct0) as x0, max(ct0) as x1],
  [],
  groupby([count() as ct0], [], select([c_mktsegment as k0], dedup(select([c_mktsegment], customer)))))
|}
  in
  Simplify_tactic.simplify (Lazy.force Test_util.tpch_conn) r |> Fmt.pr "%a" pp;
  [%expect
    {|
    groupby([min(ct0) as x0, max(ct0) as x1],
      [],
      groupby([count() as ct0],
        [],
        select([c_mktsegment as k0], dedup(select([c_mktsegment], customer))))) |}]

let%expect_test "" =
  let r =
    load_string
      {|
join((((l_partkey = s2_ps_partkey) &&
      (l_suppkey = s2_ps_suppkey)) && true),
  dedup(
    select([ps_availqty as s2_ps_availqty,
            ps_comment as s2_ps_comment,
            ps_partkey as s2_ps_partkey,
            ps_suppkey as s2_ps_suppkey,
            ps_supplycost as s2_ps_supplycost],
      partsupp)),
  lineitem)
|}
  in
  apply (at_ elim_select Path.(all >>? is_select >>| shallowest)) Path.root r
  |> Option.iter ~f:(Fmt.pr "%a@." pp);
  [%expect
    {|
    join((((l_partkey = ps_partkey) && (l_suppkey = ps_suppkey)) && true),
      dedup(partsupp),
      lineitem) |}]

let%expect_test "" =
  let r =
    load_string
      {|
select([min(c) as min_, max(c) as max_, avg(c) as avg_],
  groupby([count() as c], [],
    alist(nation as s5, atuple([ascalar(true), ascalar(true), ascalar(true), ascalar(true)], cross))))
|}
  in
  apply unnest_and_simplify Path.root r |> Option.iter ~f:(Fmt.pr "%a@." pp);
  [%expect
    {|
    select([min(c) as min_, max(c) as max_, avg(c) as avg_],
      groupby([count() as c],
        [],
        select([],
          join(true,
            select([n_nationkey as s5_n_nationkey, n_name as s5_n_name,
                    n_regionkey as s5_n_regionkey, n_comment as s5_n_comment],
              nation),
            select([], ascalar(0 as x0)))))) |}]

let%expect_test "" =
  let r =
    load_string
      {|
      dedup(
        select([s_name, s_address],
          orderby([s_name],
            join((s_nationkey = n_nationkey),
              filter((n_name = "test"), nation),
              select([s_suppkey, s_name, s_address, s_nationkey, s_phone,
                      s_acctbal, s_comment],
                groupby([s0_s_acctbal, s0_s_address, s0_s_comment, s0_s_name,
                         s0_s_nationkey, s0_s_phone, s0_s_suppkey, s_suppkey,
                         s_name, s_address, s_nationkey, s_phone, s_acctbal,
                         s_comment],
                  [s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal,
                   s_comment, s0_s_acctbal, s0_s_address, s0_s_comment, s0_s_name,
                   s0_s_nationkey, s0_s_phone, s0_s_suppkey],
                  select([s0_s_acctbal, s0_s_address, s0_s_comment, s0_s_name,
                          s0_s_nationkey, s0_s_phone, s0_s_suppkey,
                          s0_s_suppkey as s_suppkey, s0_s_name as s_name,
                          s0_s_address as s_address, s0_s_nationkey as s_nationkey,
                          s0_s_phone as s_phone, s0_s_acctbal as s_acctbal,
                          s0_s_comment as s_comment],
                    join(((s0_s_suppkey = ps_suppkey) && true),
                      dedup(
                        select([s_acctbal as s0_s_acctbal,
                                s_address as s0_s_address,
                                s_comment as s0_s_comment, s_name as s0_s_name,
                                s_nationkey as s0_s_nationkey,
                                s_phone as s0_s_phone, s_suppkey as s0_s_suppkey],
                          supplier)),
                      select([ps_partkey, ps_suppkey, ps_availqty, ps_supplycost,
                              ps_comment],
                        groupby([s1_ps_availqty, s1_ps_comment, s1_ps_partkey,
                                 s1_ps_suppkey, s1_ps_supplycost, ps_partkey,
                                 ps_suppkey, ps_availqty, ps_supplycost, ps_comment],
                          [ps_partkey, ps_suppkey, ps_availqty, ps_supplycost,
                           ps_comment, s1_ps_availqty, s1_ps_comment,
                           s1_ps_partkey, s1_ps_suppkey, s1_ps_supplycost],
                          select([s1_ps_availqty, s1_ps_comment, s1_ps_partkey,
                                  s1_ps_suppkey, s1_ps_supplycost,
                                  s1_ps_partkey as ps_partkey,
                                  s1_ps_suppkey as ps_suppkey,
                                  s1_ps_availqty as ps_availqty,
                                  s1_ps_supplycost as ps_supplycost,
                                  s1_ps_comment as ps_comment],
                            join(((s1_ps_partkey = p_partkey) && true),
                              dedup(
                                select([s2_ps_availqty as s1_ps_availqty,
                                        s2_ps_comment as s1_ps_comment,
                                        s2_ps_partkey as s1_ps_partkey,
                                        s2_ps_suppkey as s1_ps_suppkey,
                                        s2_ps_supplycost as s1_ps_supplycost],
                                  filter((s2_ps_availqty > q2),
                                    groupby([min(s2_ps_availqty) as s2_ps_availqty,
                                             min(s2_ps_comment) as s2_ps_comment,
                                             min(s2_ps_partkey) as s2_ps_partkey,
                                             min(s2_ps_suppkey) as s2_ps_suppkey,
                                             min(s2_ps_supplycost) as s2_ps_supplycost,
                                             (0.5 * sum(l_quantity)) as q2],
                                      [s2_ps_availqty, s2_ps_comment,
                                       s2_ps_partkey, s2_ps_suppkey,
                                       s2_ps_supplycost],
                                      join((((l_partkey = s2_ps_partkey) &&
                                            (l_suppkey = s2_ps_suppkey)) && true),
                                        dedup(
                                          select([ps_availqty as s2_ps_availqty,
                                                  ps_comment as s2_ps_comment,
                                                  ps_partkey as s2_ps_partkey,
                                                  ps_suppkey as s2_ps_suppkey,
                                                  ps_supplycost as s2_ps_supplycost],
                                            partsupp)),
                                        lineitem))))),
                              filter((strpos(p_name, "test") = 1), part)))))))))))))
|}
  in
  apply (fix (first elim_select Path.(all >>? is_select))) Path.root r
  |> Option.iter ~f:(Fmt.pr "%a@." pp);
  [%expect
    {|
    dedup(
      select([s_name, s_address],
        orderby([s_name],
          join((s_nationkey = n_nationkey),
            filter((n_name = "test"), nation),
            select([s_suppkey, s_name, s_address, s_nationkey, s_phone,
                    s_acctbal, s_comment],
              groupby([s_acctbal, s_address, s_comment, s_name, s_nationkey,
                       s_phone, s_suppkey],
                [s_acctbal, s_address, s_comment, s_name, s_nationkey, s_phone,
                 s_suppkey],
                select([s_acctbal, s_address, s_comment, s_name, s_nationkey,
                        s_phone, s_suppkey],
                  join(((s_suppkey = ps_suppkey) && true),
                    dedup(supplier),
                    select([ps_partkey, ps_suppkey, ps_availqty, ps_supplycost,
                            ps_comment],
                      groupby([ps_availqty, ps_comment, ps_partkey, ps_suppkey,
                               ps_supplycost],
                        [ps_availqty, ps_comment, ps_partkey, ps_suppkey,
                         ps_supplycost],
                        select([ps_availqty, ps_comment, ps_partkey, ps_suppkey,
                                ps_supplycost],
                          join(((ps_partkey = p_partkey) && true),
                            dedup(
                              filter((ps_availqty > q2),
                                groupby([min(ps_availqty) as s2_ps_availqty,
                                         min(ps_comment) as s2_ps_comment,
                                         min(ps_partkey) as s2_ps_partkey,
                                         min(ps_suppkey) as s2_ps_suppkey,
                                         min(ps_supplycost) as s2_ps_supplycost,
                                         (0.5 * sum(l_quantity)) as q2],
                                  [ps_availqty, ps_comment, ps_partkey,
                                   ps_suppkey, ps_supplycost],
                                  join((((l_partkey = ps_partkey) &&
                                        (l_suppkey = ps_suppkey)) && true),
                                    dedup(partsupp),
                                    lineitem)))),
                            filter((strpos(p_name, "test") = 1), part))))))))))))) |}]
