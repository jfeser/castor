open Type
open Collections

let%expect_test "byte-width-1" =
  [%sexp_of: int] (Abs_int.byte_width ~nullable:false (Interval (0, 149)))
  |> print_s;
  [%expect {| 2 |}]

let%expect_test "byte-width-2" =
  [%sexp_of: int]
    (Abs_int.byte_width ~nullable:false (Interval (1159859652, 1839958092)))
  |> print_s;
  [%expect {| 4 |}]

let%expect_test "absfixed-unify" =
  List.reduce_exn
    [
      AbsFixed.(of_fixed { value = 3; scale = 1 });
      AbsFixed.(of_fixed { value = 34; scale = 100 });
      AbsFixed.(of_fixed { value = 7; scale = 1 });
      AbsFixed.(of_fixed { value = 7999; scale = 10000 });
    ]
    ~f:AbsFixed.join
  |> [%sexp_of: AbsFixed.t] |> print_s;
  [%expect {| ((range (Interval 3400 70000)) (scale 10000)) |}]

let%expect_test "mult" =
  Abs_int.(Interval (0, 100) * Interval (0, 1000))
  |> [%sexp_of: Abs_int.t] |> print_s;
  [%expect {| (Interval 0 100000) |}]

let%expect_test "len-1" =
  ListT
    ( IntT { range = Interval (1, 1000); nullable = false },
      { count = Interval (0, 100) } )
  |> len |> [%sexp_of: Abs_int.t] |> print_s;
  [%expect {| (Interval 3 203) |}]

let type_test conn q =
  let type_ = Type.Parallel.type_of conn q in
  print_endline "Parallel type (imprecise):";
  [%sexp_of: Type.t] type_ |> print_s;
  let type_ = Type.type_of conn q in
  print_endline "Serial type (precise):";
  [%sexp_of: Type.t] type_ |> print_s

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in
  let q =
    "alist(select([f], r1) as k, ascalar(k.f))"
    |> Abslayout_load.load_string conn
  in
  type_test conn q;
  [%expect
    {|
    (ListT ((IntT ((range (Interval 1 3)))) ((count (Interval 5 5))))) |}]

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in
  let q =
    "ahashidx(select([f], r1) as k, ascalar(k.f), 0)"
    |> Abslayout_load.load_string conn
  in
  type_test conn q;
  [%expect
    {|
    (HashIdxT
     ((IntT ((range (Interval 1 3)))) (IntT ((range (Interval 1 3))))
      ((key_count (Interval 5 5))))) |}]

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in
  let q =
    "ahashidx(select([f], r1) as k, alist(select([g], filter(k.f = f, r1)) as \
     k1, ascalar(k1.g)), 0)"
    |> Abslayout_load.load_string conn
  in
  type_test conn q;
  [%expect
    {|
    (HashIdxT
     ((IntT ((range (Interval 1 3))))
      (ListT ((IntT ((range (Interval 1 4)))) ((count (Interval 1 2)))))
      ((key_count (Interval 5 5))))) |}]

let%expect_test "" =
  let conn = Lazy.force Test_util.tpch_conn in
  let q =
    {|
    alist(orderby([l_returnflag, l_linestatus], dedup(select([l_returnflag, l_linestatus], lineitem))) as k0,
                    select([l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
                            sum(l_extendedprice) as sum_base_price,
                            sum((l_extendedprice * (1 - l_discount))) as sum_disc_price,
                            sum(((l_extendedprice * (1 - l_discount)) * (1 + l_tax))) as sum_charge,
                            avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, 
                            avg(l_discount) as avg_disc, count() as count_order],
                      aorderedidx(dedup(select([l_shipdate], lineitem)) as s0,
                        select([l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus],
                          alist(select([l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus],
                                  filter(((l_returnflag = k0.l_returnflag) &&
                                         ((l_linestatus = k0.l_linestatus) && (l_shipdate = s0.l_shipdate))),
                                    lineitem)) as s1,
                            atuple([ascalar(s1.l_quantity), ascalar(s1.l_extendedprice), 
                                    ascalar(s1.l_discount), ascalar(s1.l_tax), 
                                    ascalar(s1.l_returnflag), ascalar(s1.l_linestatus)],
                              cross))),
                        , <= (date("1998-12-01") - day(param0)))))
|}
    |> Abslayout_load.load_string
         ~params:
           (Set.of_list
              (module Name)
              [ Name.create ~type_:Prim_type.int_t "param0" ])
         conn
  in
  type_test conn q;
  [%expect
    {|
    (ListT
     ((FuncT
       (((OrderedIdxT
          ((DateT ((range (Interval 8037 10552))))
           (FuncT
            (((ListT
               ((TupleT
                 (((IntT ((range (Interval 1 50))))
                   (FixedT ((value ((range Top) (scale 1)))))
                   (FixedT ((value ((range Top) (scale 1)))))
                   (FixedT ((value ((range Top) (scale 1)))))
                   (StringT ((nchars Top))) (StringT ((nchars Top))))
                  ((kind Cross))))
                ((count (Interval 1 5))))))
             (Width 6)))
           ((key_count (Interval 812 812))))))
        (Width 10)))
      ((count (Interval 4 4))))) |}]

let%expect_test "" =
  let conn = Lazy.force Test_util.tpch_conn in
  let q =
    {|
    aorderedidx(select([o_orderdate], dedup(select([o_orderdate], orders))) as s7,
                      atuple([alist(select([l_extendedprice, l_discount],
                                      join(((o_orderdate = s7.o_orderdate) &&
                                           (((l_returnflag = "R")) &&
                                           (l_orderkey = o_orderkey))),
                                        lineitem,
                                        orders)) as s2,
                                atuple([ascalar(s2.l_extendedprice),
                                        ascalar(s2.l_discount)],
                                  cross))],
                        cross),
      >= param0, < (param0 + month(3)))
|}
    |> Abslayout_load.load_string
         ~params:
           (Set.of_list
              (module Name)
              [ Name.create ~type_:Prim_type.date_t "param0" ])
         conn
  in
  type_test conn q

let%expect_test "" =
  Logs.set_reporter (Logs.format_reporter ());
  Logs.Src.set_level Unnest.src (Some Info);
  let conn = Lazy.force Test_util.tpch_conn in
  let q =
    {|
    aorderedidx(select([o_orderdate], dedup(select([o_orderdate], orders))) as s7,
                      atuple([ascalar(0),
                              alist(select([l_extendedprice, l_discount],
                                      join(((o_orderdate = s7.o_orderdate) &&
                                           (((l_returnflag = "R")) &&
                                           (l_orderkey = o_orderkey))),
                                        lineitem,
                                        orders)) as s2,
                                ascalar(s2.l_discount))],
                        cross),
      >= param0, < (param0 + month(3)))
|}
    |> Abslayout_load.load_string
         ~params:
           (Set.of_list
              (module Name)
              [ Name.create ~type_:Prim_type.date_t "param0" ])
         conn
  in
  type_test conn q
