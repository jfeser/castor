open Abslayout
open Abslayout_load
open Test_util

let conn = Lazy.force tpch_conn

let pp_refcount fmt meta =
  Fmt.pf fmt "@[<hov 2>{";
  Map.iteri meta#refs ~f:(fun ~key:n ~data:c ->
      if c then Fmt.pf fmt "%a,@ " Name.pp n);
  Fmt.pf fmt "}@]"

let pp_with_refcount = Abslayout_pp.pp_with_meta pp_refcount

let load s =
  load_string_exn conn s
  |> Resolve.resolve_exn ~params:(Set.empty (module Name))

let%expect_test "" =
  let r =
    {|
         select([l_receiptdate],
                            alist(join((o_orderkey = l_orderkey),
                                    lineitem,
                                    orders),
                              atuple([ascalar(0.l_orderkey),
                                      ascalar(0.l_commitdate),
                                      ascalar(0.l_receiptdate),
                                      ascalar(0.o_comment)],
                                cross)))
       |}
    |> load
  in
  Format.printf "%a@." pp_with_refcount r;
  [%expect
    {|
         {l_receiptdate, }#select([l_receiptdate],
           {l_receiptdate, }#
             alist({l_commitdate, l_orderkey, l_receiptdate, o_comment, o_orderkey, }#
                     join((o_orderkey = l_orderkey),
                     {l_commitdate, l_orderkey, l_receiptdate, }#lineitem,
                     {o_comment, o_orderkey, }#orders),
             {l_receiptdate, }#
               atuple([{}#ascalar(0.l_orderkey), {}#ascalar(0.l_commitdate),
                       {l_receiptdate, }#ascalar(0.l_receiptdate),
                       {}#ascalar(0.o_comment)], cross))) |}]

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
       {f, }#select([f],
         {f, }#
           atuple([{f, }#atuple([{f, }#ascalar(0 as f), {}#ascalar(1 as g)], cross),
                   {f, }#atuple([{}#ascalar(2 as g), {f, }#ascalar(3 as f)], cross)],
           concat)) |}]

let%expect_test "" =
  let r =
    {|
         alist(lineitem, select([l_shipmode], atuple([ascalar(0.l_shipmode), ascalar(0 as z)], cross)))
       |}
    |> load
  in
  Format.printf "%a@." pp_with_refcount r;
  [%expect
    {|
       {l_shipmode, }#alist({l_shipmode, }#lineitem,
         {l_shipmode, }#select([l_shipmode],
           {l_shipmode, }#
             atuple([{l_shipmode, }#ascalar(0.l_shipmode), {}#ascalar(0 as z)],
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

let%expect_test "" =
  let r =
    {|
         alist(lineitem, alist(orders, atuple([ascalar(1.l_shipmode), ascalar(0.o_orderkey)], cross)))
       |}
    |> load
  in
  Format.printf "%a@." pp_with_refcount r;
  [%expect
    {|
       {l_shipmode, o_orderkey, }#alist({l_shipmode, }#lineitem,
         {l_shipmode, o_orderkey, }#alist({o_orderkey, }#orders,
           {l_shipmode, o_orderkey, }#
             atuple([{l_shipmode, }#ascalar(1.l_shipmode),
                     {o_orderkey, }#ascalar(0.o_orderkey)], cross))) |}]
