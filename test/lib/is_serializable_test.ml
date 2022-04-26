open Is_serializable

let%expect_test "" =
  let r =
    {|
select([l_orderkey, sum_l_quantity],
  ahashidx(dedup(select([l_orderkey as x305], dedup(select([l_orderkey], lineitem)))) as s48,
    select([l_orderkey, sum_l_quantity],
      aorderedidx(dedup(
                    select([sum_l_quantity],
                      depjoin(dedup(select([l_orderkey], lineitem)) as s44,
                        select([sum(l_quantity) as sum_l_quantity], filter((l_orderkey = s44.l_orderkey), lineitem))))) as s43,
        alist(alist(filter((l_orderkey = s48.x305), dedup(select([l_orderkey], lineitem))) as k1,
                filter((sum_l_quantity = s43.sum_l_quantity),
                  select([l_orderkey, sum(l_quantity) as sum_l_quantity],
                    alist(select([l_orderkey, l_quantity], filter((l_orderkey = k1.l_orderkey), lineitem)) as s45,
                      atuple([ascalar(s45.l_orderkey), ascalar(s45.l_quantity)], cross))))) as s55,
          atuple([ascalar(s55.l_orderkey), ascalar(s55.sum_l_quantity)], cross)),
        > param1, )),
    o_orderkey))
|}
    |> Abslayout_load.load_string_exn
         ~params:
           (Set.of_list
              (module Name)
              [
                Name.create ~type_:Prim_type.int_t "param1";
                Name.create ~type_:Prim_type.string_t "o_orderkey";
              ])
         (Lazy.force Test_util.tpch_conn)
  in
  match is_serializeable r with Ok () -> () | Error str -> print_endline str

let%expect_test "" =
  let r =
    {|
select([s_name, s_address],
  ahashidx(select([n_name as k0], dedup(select([n_name], nation))) as s0,
    alist(filter((n_name = s0.k0),
            select([n_name, s_suppkey, s_name, s_address],
              orderby([s_name],
                depjoin(select([n_nationkey, n_name], nation) as s3,
                  depjoin(dedup(select([s_nationkey], supplier)) as s4,
                    select([s3.n_name, s_suppkey, s_name, s_address],
                      filter((s4.s_nationkey = s3.n_nationkey),
                        select([s_suppkey, s_name, s_address],
                          filter((s4.s_nationkey = s_nationkey), supplier))))))))) as s9,
      select([s_name, s_address],
        filter(exists(select([ps_partkey, ps_suppkey, ps_availqty,
                              ps_supplycost, ps_comment],
                        ahashidx(dedup(
                                   select([ps_suppkey as x1228],
                                     dedup(select([ps_suppkey], partsupp)))) as s80,
                          alist(filter((s80.x1228 = ps_suppkey), partsupp) as s78,
                            filter(((ps_availqty >
                                    (select([(0.5 * sum(l_quantity))],
                                       filter(((l_partkey = ps_partkey) &&
                                              ((l_suppkey = ps_suppkey) &&
                                              ((l_shipdate >= date("2020-01-01")) &&
                                              (l_shipdate <
                                              (date("2020-01-01") + year(1)))))),
                                         lineitem)))) &&
                                   exists(filter((ps_partkey = p_partkey),
                                            filter((strpos(p_name, "") =
                                                   1),
                                              part)))),
                              atuple([ascalar(s78.ps_partkey),
                                      ascalar(s78.ps_suppkey),
                                      ascalar(s78.ps_availqty),
                                      ascalar(s78.ps_supplycost),
                                      ascalar(s78.ps_comment)],
                                cross))),
                          s_suppkey))),
          atuple([ascalar(s9.s_suppkey), ascalar(s9.s_name),
                  ascalar(s9.s_address)],
            cross)))),
    ""))
|}
    |> Abslayout_load.load_string_exn @@ Lazy.force Test_util.tpch_conn
  in
  match is_serializeable r with
  | Ok () -> ()
  | Error str ->
      print_endline str;
      [%expect
        {| Cannot serialize: Bad operator in run-time position lineitem |}]
