open Abslayout
open Abslayout_load
open Select_tactics
open Test_util

module C = struct
  let params = Set.empty (module Name)
  let fresh = Fresh.create ()
  let verbose = false
  let validate = true
  let param_ctx = Map.empty (module Name)
  let conn = Lazy.force tpch_conn
  let cost_conn = conn
  let simplify = None
end

open Make (C)
open Ops.Make (C)

let () =
  Log.setup_stderr ();
  Logs.Src.set_level Check.src (Some Error)

let%expect_test "push-select-index" =
  let r =
    load_string_exn (Lazy.force tpch_conn)
      {|
select([sum(o_totalprice) as revenue],
  aorderedidx(select([o_orderdate], dedup(select([o_orderdate], orders))) as s1,
    filter(o_orderdate = s1.o_orderdate, orders),
    >= date("0001-01-01"), < (date("0001-01-01") + year(1))))
|}
  in
  let r' = Option.value_exn (apply push_select Path.root r) in
  Format.printf "%a\n" pp r';
  [%expect
    {|
    select([sum(agg0) as revenue],
      aorderedidx(select([o_orderdate], dedup(select([o_orderdate], orders))) as s1,
        select([sum(o_totalprice) as agg0, o_orderkey, o_custkey, o_orderstatus,
                o_totalprice, o_orderpriority, o_clerk, o_shippriority, o_comment],
          filter((o_orderdate = s1.o_orderdate), orders)),
        >= date("0001-01-01"), < (date("0001-01-01") + year(1)))) |}]

let%expect_test "" =
  let r =
    load_string_exn (Lazy.force tpch_conn)
      {|
select([substring(c1_phone, 0, 2) as x669, c1_phone, c1_acctbal, c1_custkey],
        filter((c1_acctbal >
               (select([avg(c_acctbal) as avgbal],
                  filter(((c_acctbal > 0.0) &&
                         ((substring(c_phone, 0, 2) = "") ||
                         ((substring(c_phone, 0, 2) = "") ||
                         ((substring(c_phone, 0, 2) = "") ||
                         ((substring(c_phone, 0, 2) = "") ||
                         ((substring(c_phone, 0, 2) = "") ||
                         ((substring(c_phone, 0, 2) = "") || (substring(c_phone, 0, 2) = "")))))))),
                    customer)))),
          filter(not(exists(filter((o_custkey = c1_custkey), orders))),
            select([c_phone as c1_phone, c_acctbal as c1_acctbal, c_custkey as c1_custkey], customer))))
|}
  in
  apply push_select_filter Path.root r |> Option.iter ~f:(Fmt.pr "%a@." pp);
  [%expect
    {|
    filter((c1_acctbal >
           (select([avg(c_acctbal) as avgbal],
              filter(((c_acctbal > 0.0) &&
                     ((substring(c_phone, 0, 2) = "") ||
                     ((substring(c_phone, 0, 2) = "") ||
                     ((substring(c_phone, 0, 2) = "") ||
                     ((substring(c_phone, 0, 2) = "") ||
                     ((substring(c_phone, 0, 2) = "") ||
                     ((substring(c_phone, 0, 2) = "") ||
                     (substring(c_phone, 0, 2) = "")))))))),
                customer)))),
      select([substring(c1_phone, 0, 2) as x669, c1_phone, c1_acctbal, c1_custkey],
        filter(not(exists(filter((o_custkey = c1_custkey), orders))),
          select([c_phone as c1_phone, c_acctbal as c1_acctbal,
                  c_custkey as c1_custkey],
            customer)))) |}]

let%expect_test "" =
  let r =
    load_string_exn (Lazy.force tpch_conn)
      {|
select([substring(c1_phone, 0, 2) as x669, c1_phone, c1_custkey],
        filter((c1_acctbal >
               (select([avg(c_acctbal) as avgbal],
                  filter(((c_acctbal > 0.0) &&
                         ((substring(c_phone, 0, 2) = "") ||
                         ((substring(c_phone, 0, 2) = "") ||
                         ((substring(c_phone, 0, 2) = "") ||
                         ((substring(c_phone, 0, 2) = "") ||
                         ((substring(c_phone, 0, 2) = "") ||
                         ((substring(c_phone, 0, 2) = "") || (substring(c_phone, 0, 2) = "")))))))),
                    customer)))),
          filter(not(exists(filter((o_custkey = c1_custkey), orders))),
            select([c_phone as c1_phone, c_acctbal as c1_acctbal, c_custkey as c1_custkey], customer))))
|}
  in
  apply push_select_filter Path.root r |> Option.iter ~f:(Fmt.pr "%a@." pp)