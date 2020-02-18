open Abslayout
open Abslayout_load
open Test_util

let conn = Lazy.force tpch_conn

let pp x =
  let pp, _ = mk_pp ~pp_name:Name.pp_with_stage () in
  pp x

let pp_with_refcount, _ =
  mk_pp ~pp_name:Name.pp
    ~pp_meta:(fun fmt meta ->
      let open Format in
      fprintf fmt "@[<hv 2>{";
      Map.iteri meta#refs ~f:(fun ~key:n ~data:c ->
          if c then fprintf fmt "%a,@ " Name.pp n);
      fprintf fmt "}@]")
    ()

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
      select([l_receiptdate],
        alist(join((o_orderkey = l_orderkey),
                lineitem#{l_commitdate, l_orderkey, l_receiptdate, },
                orders#{o_comment, o_orderkey, })#{l_commitdate,
                                                    l_orderkey,
                                                    l_receiptdate,
                                                    o_comment,
                                                    o_orderkey,
                                                    } as k#{k.l_commitdate,
                                                             k.l_orderkey,
                                                             k.l_receiptdate,
                                                             k.o_comment,
                                                             k.o_orderkey,
                                                             },
          atuple([ascalar(k.l_orderkey)#{}, ascalar(k.l_commitdate)#{},
                  ascalar(k.l_receiptdate)#{l_receiptdate, },
                  ascalar(k.o_comment)#{}],
            cross)#{l_receiptdate, })#{l_receiptdate, })#{l_receiptdate, } |}]

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
    select([f],
      atuple([atuple([ascalar(0 as f)#{f, }, ascalar(1 as g)#{}], cross)#{f, },
              atuple([ascalar(2 as g)#{}, ascalar(3 as f)#{f, }], cross)#{f, }],
        concat)#{f, })#{f, } |}]

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
    alist(lineitem#{l_shipmode, } as k2#{k2.l_shipmode, },
      select([l_shipmode],
        atuple([ascalar(k2.l_shipmode)#{l_shipmode, }, ascalar(0)#{}],
          cross)#{l_shipmode, })#{l_shipmode, })#{l_shipmode, } |}]
