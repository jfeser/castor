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
      select([l_extendedprice, l_discount, l_quantity, l_shipdate],
        select([l_quantity, l_extendedprice, l_discount, l_shipdate], lineitem))) |}]

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
select([min(c) as min_, max(c) as max_, avg(c) as avg_],
  groupby([count() as c], [],
    alist(nation as s5, atuple([ascalar(true as t1), ascalar(true as t2), ascalar(true as t3), ascalar(true as t4)], cross))))
|}
  in
  apply unnest_and_simplify Path.root r |> Option.iter ~f:(Fmt.pr "%a@." pp);
  [%expect
    {|
    select([min(c) as min_, max(c) as max_, avg(c) as avg_],
      groupby([count() as c],
        [],
        select([t1, t2, t3, t4],
          join(true,
            select([n_nationkey as s5_n_nationkey, n_name as s5_n_name,
                    n_regionkey as s5_n_regionkey, n_comment as s5_n_comment],
              nation),
            select([true as t1, true as t2, true as t3, true as t4],
              ascalar(0 as x0)))))) |}]
