open Abslayout
open Select_tactics
open Castor_test.Test_util

module C = struct
  let params = Set.empty (module Name)

  let fresh = Fresh.create ()

  let verbose = false

  let validate = true

  let param_ctx = Map.empty (module Name)

  let conn = Lazy.force test_db_conn

  let simplify = None
end

open Make (C)

open Ops.Make (C)

let () =
  Log.setup_stderr ();
  Logs.Src.set_level Inv.src (Some Error)

let%expect_test "push-select-index" =
  let r =
    Abslayout_load.load_string_exn (Lazy.force tpch_conn)
      {|
select([sum(o_totalprice) as revenue],
  aorderedidx(select([o_orderdate], dedup(select([o_orderdate], orders))) as s1,
    filter(o_orderdate = s1.o_orderdate, orders),
    >= date("0001-01-01"), < (date("0001-01-01") + year(1))))
|}
  in
  let r' = Option.value_exn (apply push_select Path.root r) in
  Inv.resolve r r';
  Format.printf "%a\n" pp r';
  [%expect
    {|
    select([sum(agg0) as revenue],
      aorderedidx(select([o_orderdate], dedup(select([o_orderdate], orders))) as s1,
        filter((count0 > 0),
          select([count() as count0, sum(o_totalprice) as agg0, o_orderkey,
                  o_custkey, o_orderstatus, o_totalprice, o_orderpriority,
                  o_clerk, o_shippriority, o_comment],
            atuple([filter((o_orderdate = s1.o_orderdate), orders)], cross))),
        >= date("0001-01-01"), < (date("0001-01-01") + year(1)))) |}]
