open Groupby_tactics

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

module T = Make (C)

open Tactics_util.Make (C)

open C
open T
open Ops

let with_logs f =
  Logs.(set_reporter (format_reporter ()));
  Logs.Src.set_level src (Some Debug);
  let ret = f () in
  Logs.Src.set_level src (Some Error);
  Logs.(set_reporter nop_reporter);
  ret

let%expect_test "" =
  let r =
    Abslayout_load.load_string_exn ~params conn
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
  with_logs (fun () ->
      apply elim_groupby Path.root r
      |> Option.iter ~f:(Format.printf "%a@." Abslayout.pp));
  [%expect
    {|
      alist(select([to_year(o_orderdate) as o_year],
              dedup(select([o_orderdate], orders))) as k0,
        select([o_year,
                (sum((if (nation_name = param1) then volume else 0.0)) /
                sum(volume)) as mkt_share],
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
    Set.of_list (module Name)
    @@ List.init 7 ~f:(fun i -> Name.create @@ sprintf "param%d" i)
  in
  let r =
    Abslayout_load.load_string_exn ~params conn
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
  all_values_approx ps r |> Result.iter ~f:(Format.printf "%a@." Abslayout.pp);
  [%expect
    {|
    select([substring(c_phone, 0, 2) as cntrycode],
      dedup(select([c_phone], customer))) |}]
