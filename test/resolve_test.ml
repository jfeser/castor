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

let load s =
  load_string_exn conn s
  |> Resolve.resolve_exn ~params:(Set.empty (module Name))

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

let%test_unit "" =
  {|
          filter(not(exists(filter(l3_orderkey = l1_orderkey,
                              select([l_orderkey as l3_orderkey],
                                lineitem)))),
            select([l_orderkey as l1_orderkey],
              lineitem))
|}
  |> load |> ignore
