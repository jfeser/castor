open Abslayout
open Abslayout_load
open Test_util

let conn = Lazy.force tpch_conn

let pp x =
  let pp, _ = mk_pp ~pp_name:Name.pp () in
  pp x

let pp_with_refcount, _ =
  mk_pp ~pp_name:Name.pp
    ~pp_meta:(fun fmt meta ->
      let open Format in
      fprintf fmt "@[<hov 2>{";
      Map.iteri meta#refs ~f:(fun ~key:n ~data:c ->
          if c then fprintf fmt "%a,@ " Name.pp n);
      fprintf fmt "}@]")
    ()

(* let pp_with_stage, _ =
 *   mk_pp ~pp_name:Name.pp
 *     ~pp_meta:(fun fmt meta ->
 *       let open Format in
 *       let compile, run =
 *         Map.to_alist meta#stage
 *         |> List.partition_map ~f:(fun (n, s) ->
 *                match s with `Compile -> `Fst n | `Run -> `Snd n)
 *       in
 *       let pp_names = Fmt.Dump.list Name.pp in
 *       Fmt.pf fmt "@[<hov 2>{compile=%a,@ run=%a}@]" pp_names compile pp_names
 *         run)
 *     () *)

let load s = load_string conn s |> Resolve.resolve

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
    |> load
  in
  Format.printf "%a@." pp_with_refcount r;
  [%expect
    {|
      {l_receiptdate, }#
        select([l_receiptdate],
        {l_receiptdate, }#
          alist({l_commitdate, l_orderkey, l_receiptdate, o_comment, o_orderkey, }#
                  join((o_orderkey = l_orderkey),
                  {l_commitdate, l_orderkey, l_receiptdate, }#lineitem,
                  {o_comment, o_orderkey, }#orders) as k,
          {l_receiptdate, }#
            atuple([{}#ascalar(k.l_orderkey), {}#ascalar(k.l_commitdate),
                    {l_receiptdate, }#ascalar(k.l_receiptdate),
                    {}#ascalar(k.o_comment)],
            cross))) |}]

let%expect_test "" =
  let r =
    {|
      select([f], atuple([atuple([ascalar(0 as f), ascalar(1 as g)], cross),
                          atuple([ascalar(2 as g), ascalar(3 as f)], cross)], concat))
    |}
    |> load
  in
  Format.printf "%a@." pp_with_refcount r;
  [%expect
    {|
    {f, }#
      select([f],
      {f, }#
        atuple([{f, }#atuple([{f, }#ascalar(0 as f), {}#ascalar(1 as g)], cross),
                {f, }#atuple([{}#ascalar(2 as g), {f, }#ascalar(3 as f)], cross)],
        concat)) |}]

let%expect_test "" =
  let r =
    {|
      alist(lineitem as k2, select([l_shipmode], atuple([ascalar(k2.l_shipmode), ascalar(0)], cross)))
    |}
    |> load
  in
  Format.printf "%a@." pp_with_refcount r;
  [%expect
    {|
    {l_shipmode, }#
      alist({l_shipmode, }#lineitem as k2,
      {l_shipmode, }#
        select([l_shipmode],
        {l_shipmode, }#
          atuple([{l_shipmode, }#ascalar(k2.l_shipmode), {}#ascalar(0)],
          cross))) |}]

(* let%expect_test "" =
 *   let r =
 *     {|
 * select([max(total_revenue_i) as tot],
 *   alist(dedup(select([l_suppkey], lineitem)) as k1,
 *     select([sum(agg2) as total_revenue_i],
 *       aorderedidx(dedup(select([l_shipdate], lineitem)) as s46,
 *         filter((count2 > 0),
 *           select([count() as count2, sum((l_extendedprice * (1 - l_discount))) as agg2, l_extendedprice, l_discount],
 *             atuple([ascalar(s46.l_shipdate),
 *                     alist(select([l_extendedprice, l_discount],
 *                             filter(((l_suppkey = k1.l_suppkey) && (l_shipdate = s46.l_shipdate)), lineitem)) as s47,
 *                       atuple([ascalar(s47.l_extendedprice), ascalar(s47.l_discount)], cross))],
 *               cross))),
 *         >= date("0000-01-01"), < (date("0000-01-01") + month(3))))))
 * |}
 *     |> load
 *   in
 *   Format.printf "%a@." pp_with_stage r *)
