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
