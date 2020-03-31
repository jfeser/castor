open Type
open Collections
open Test_util

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
  let type_ =
    Type.Parallel.type_of conn q
    |> Result.map_error ~f:(fun _ -> Failure "")
    |> Result.ok_exn
  in
  print_endline "Parallel type (imprecise):";
  [%sexp_of: Type.t] type_ |> print_s;
  let type_ = Type.type_of conn q in
  print_endline "Serial type (precise):";
  [%sexp_of: Type.t] type_ |> print_s

let%expect_test "" =
  let conn = Lazy.force test_db_conn in
  let q =
    "alist(select([f], r1) as k, ascalar(k.f))"
    |> Abslayout_load.load_string conn
  in
  type_test conn q;
  [%expect
    {|
    Parallel type (imprecise):
    (ListT ((IntT ((range (Interval 1 3)))) ((count (Interval 5 5)))))
    Serial type (precise):
    (ListT ((IntT ((range (Interval 1 3)))) ((count (Interval 5 5))))) |}]

let%expect_test "" =
  let conn = Lazy.force test_db_conn in
  let q =
    "ahashidx(select([f], r1) as k, ascalar(k.f), 0)"
    |> Abslayout_load.load_string conn
  in
  type_test conn q;
  [%expect
    {|
    Parallel type (imprecise):
    (HashIdxT
     ((IntT ((range (Interval 1 3)))) (IntT ((range (Interval 1 3))))
      ((key_count (Interval 5 5)))))
    Serial type (precise):
    (HashIdxT
     ((IntT ((range (Interval 1 3)))) (IntT ((range (Interval 1 3))))
      ((key_count (Interval 3 3))))) |}]

let%expect_test "" =
  let conn = Lazy.force test_db_conn in
  let q =
    "ahashidx(select([f], r1) as k, alist(select([g], filter(k.f = f, r1)) as \
     k1, ascalar(k1.g)), 0)"
    |> Abslayout_load.load_string conn
  in
  type_test conn q;
  [%expect
    {|
    Parallel type (imprecise):
    (HashIdxT
     ((IntT ((range (Interval 1 3))))
      (ListT ((IntT ((range (Interval 1 4)))) ((count (Interval 1 2)))))
      ((key_count (Interval 5 5)))))
    Serial type (precise):
    (HashIdxT
     ((IntT ((range (Interval 1 3))))
      (ListT ((IntT ((range (Interval 1 4)))) ((count (Interval 1 2)))))
      ((key_count (Interval 3 3))))) |}]

let%expect_test "" =
  let conn = Lazy.force tpch_conn in
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
    Parallel type (imprecise):
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
      ((count (Interval 4 4)))))
    Serial type (precise):
    (ListT
     ((FuncT
       (((OrderedIdxT
          ((DateT ((range (Interval 8037 10552))))
           (FuncT
            (((ListT
               ((TupleT
                 (((IntT ((range (Interval 1 50))))
                   (FixedT
                    ((value ((range (Interval 98606 10084102)) (scale 100)))))
                   (FixedT ((value ((range (Interval 0 10)) (scale 100)))))
                   (FixedT ((value ((range (Interval 0 8)) (scale 100)))))
                   (StringT ((nchars (Interval 1 1))))
                   (StringT ((nchars (Interval 1 1)))))
                  ((kind Cross))))
                ((count (Interval 1 5))))))
             (Width 6)))
           ((key_count (Interval 3 412))))))
        (Width 10)))
      ((count (Interval 4 4))))) |}]

let%expect_test "" =
  let conn = Lazy.force tpch_conn in
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
  type_test conn q;
  [%expect
    {|
    Parallel type (imprecise):
    (OrderedIdxT
     ((DateT ((range (Interval 8036 10437))))
      (TupleT
       (((ListT
          ((TupleT
            (((FixedT ((value ((range Top) (scale 1)))))
              (FixedT ((value ((range Top) (scale 1))))))
             ((kind Cross))))
           ((count (Interval 1 3))))))
        ((kind Cross))))
      ((key_count (Interval 822 822)))))
    Serial type (precise):
    (OrderedIdxT
     ((DateT ((range (Interval 8036 9239))))
      (TupleT
       (((ListT
          ((TupleT
            (((FixedT ((value ((range (Interval 98606 9479520)) (scale 100)))))
              (FixedT ((value ((range (Interval 0 10)) (scale 100))))))
             ((kind Cross))))
           ((count (Interval 1 3))))))
        ((kind Cross))))
      ((key_count (Interval 230 230))))) |}]

let%expect_test "" =
  let conn = Lazy.force tpch_conn in
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
  type_test conn q;
  [%expect
    {|
    Parallel type (imprecise):
    (OrderedIdxT
     ((DateT ((range (Interval 8036 10437))))
      (TupleT
       (((IntT ((range (Interval 0 0))))
         (ListT
          ((FixedT ((value ((range Top) (scale 1))))) ((count (Interval 1 3))))))
        ((kind Cross))))
      ((key_count (Interval 822 822)))))
    Serial type (precise):
    (OrderedIdxT
     ((DateT ((range (Interval 8036 10437))))
      (TupleT
       (((IntT ((range (Interval 0 0))))
         (ListT
          ((FixedT ((value ((range (Interval 0 10)) (scale 100)))))
           ((count (Interval 0 3))))))
        ((kind Cross))))
      ((key_count (Interval 822 822))))) |}]

let%expect_test "" =
  let conn = Lazy.force tpch_conn in
  let r =
    Abslayout_load.load_string conn
      {|
select([s1_acctbal, s1_name, n1_name, p1_partkey, p1_mfgr, s1_address,
           s1_phone, s1_comment],
     ahashidx(depjoin(select([min(p_size) as lo, max(p_size) as hi],
                        dedup(select([p_size], part))) as k1,
                select([range as k0], range(k1.lo, k1.hi))) as s0,
       select([s1_acctbal, s1_name, n1_name, p1_partkey, p1_mfgr, s1_address,
               s1_phone, s1_comment],
         alist(filter((p1_size = s0.k0),
                 orderby([s1_acctbal desc, n1_name, s1_name, p1_partkey],
                   join((((r1_name = r_name) &&
                         (((ps_partkey = ps1_partkey) &&
                          (ps1_supplycost = min_cost)) &&
                         (ps1_supplycost = min_cost))) && true),
                     join((n1_regionkey = r1_regionkey),
                       select([r_name as r1_name, r_regionkey as r1_regionkey],
                         region),
                       join((s1_nationkey = n1_nationkey),
                         select([n_name as n1_name, n_nationkey as n1_nationkey,
                                 n_regionkey as n1_regionkey],
                           nation),
                         join((s1_suppkey = ps1_suppkey),
                           select([s_nationkey as s1_nationkey,
                                   s_suppkey as s1_suppkey,
                                   s_acctbal as s1_acctbal, s_name as s1_name,
                                   s_address as s1_address, s_phone as s1_phone,
                                   s_comment as s1_comment],
                             supplier),
                           join((p1_partkey = ps1_partkey),
                             select([p_size as p1_size, p_type as p1_type,
                                     p_partkey as p1_partkey, p_mfgr as p1_mfgr],
                               part),
                             select([ps_supplycost as ps1_supplycost,
                                     ps_partkey as ps1_partkey,
                                     ps_suppkey as ps1_suppkey],
                               partsupp))))),
                     depjoin(dedup(
                               select([r_name, ps_partkey],
                                 join((s_suppkey = ps_suppkey),
                                   join((s_nationkey = n_nationkey),
                                     join((n_regionkey = r_regionkey),
                                       nation,
                                       region),
                                     supplier),
                                   partsupp))) as k2,
                       select([r_name, ps_partkey,
                               min(ps_supplycost) as min_cost],
                         join((((r_name = k2.r_name) &&
                               (ps_partkey = k2.ps_partkey)) &&
                              (s_suppkey = ps_suppkey)),
                           join((s_nationkey = n_nationkey),
                             join((n_regionkey = r_regionkey), nation, region),
                             supplier),
                           partsupp)))))) as s1,
           filter(((r1_name = "") &&
                  (strpos(p1_type, "") =
                  ((strlen(p1_type) - strlen("")) + 1))),
             atuple([ascalar(s1.r1_name), ascalar(s1.n1_name),
                     ascalar(s1.s1_acctbal), ascalar(s1.s1_name),
                     ascalar(s1.s1_address), ascalar(s1.s1_phone),
                     ascalar(s1.s1_comment), ascalar(s1.p1_type),
                     ascalar(s1.p1_partkey), ascalar(s1.p1_mfgr)],
               cross)))),
       0))
|}
  in
  ( match Parallel.type_of conn r with
  | Ok t -> [%sexp_of: Type.t] t |> print_s
  | _ -> () );
  [%expect
    {|
    (FuncT
     (((HashIdxT
        ((IntT ((range (Interval 1 50))))
         (FuncT
          (((ListT
             ((FuncT
               (((TupleT
                  (((StringT ((nchars Top))) (StringT ((nchars Top)))
                    (FixedT ((value ((range Top) (scale 1)))))
                    (StringT ((nchars Top))) (StringT ((nchars Top)))
                    (StringT ((nchars Top))) (StringT ((nchars Top)))
                    (StringT ((nchars Top)))
                    (IntT ((range (Interval 71 199682))))
                    (StringT ((nchars Top))))
                   ((kind Cross)))))
                Child_sum))
              ((count (Interval 14 36))))))
           (Width 8)))
         ((key_count (Interval 50 50))))))
      (Width 8))) |}]
