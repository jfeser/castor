open Test_util

let run_test ?(params = []) ?(print_code = true) layout_str =
  let (module I), (module C) = Setup.make_modules ~code_only:true () in
  let open Abslayout_load in
  let conn = Lazy.force test_db_conn in

  try
    let layout =
      let params =
        List.map params ~f:(fun (n, t, _) -> Name.copy ~type_:(Some t) n)
        |> Set.of_list (module Name)
      in
      load_string conn ~params layout_str |> Type.annotate conn
    in
    print_endline (Sexp.to_string_hum ([%sexp_of: Type.t] layout.meta#type_));
    let layout, len = Serialize.serialize conn "/tmp/buf" layout in
    let params = List.map params ~f:(fun (n, t, _) -> (n, t)) in
    let ir = I.irgen ~params ~len layout in
    if print_code then I.pp Caml.Format.std_formatter ir
  with exn ->
    Backtrace.(
      elide := false;
      Exn.most_recent () |> to_string |> print_endline);
    Exn.(to_string exn |> print_endline)

let%expect_test "tuple-simple-cross" =
  run_test "ATuple([AScalar(1), AScalar(2)], cross)";
  [%expect
    {|
    (TupleT
     (((IntT ((range (Interval 1 1)))) (IntT ((range (Interval 2 2)))))
      ((kind Cross))))
    // Locals:
    // cstart3 : int (persists=true)
    // cstart2 : int (persists=true)
    fun printer () : void {
        cstart2 = 0;
        cstart3 = cstart2 + 1;
        print(tuple[int, int], (buf[cstart2 : 1], buf[cstart3 : 1]));
    }
    // Locals:
    // cstart1 : int (persists=true)
    // cstart0 : int (persists=true)
    fun consumer () : void {
        cstart0 = 0;
        cstart1 = cstart0 + 1;
        consume(tuple[int, int], (buf[cstart0 : 1], buf[cstart1 : 1]));
    } |}]

let%expect_test "sum-complex" =
  run_test sum_complex;
  [%expect
    {|
    (FuncT
     (((ListT
        ((TupleT
          (((IntT ((range (Interval 1 3)))) (IntT ((range (Interval -1 2)))))
           ((kind Cross))))
         ((count (Interval 5 5))))))
      (Width 2)))
    // Locals:
    // found_tup17 : bool (persists=false)
    // cstart24 : int (persists=true)
    // cstart25 : int (persists=true)
    // cstart21 : int (persists=true)
    // tup16 : tuple[int, int] (persists=false)
    // i22 : int (persists=true)
    // count23 : int (persists=true)
    // sum20 : fixed (persists=false)
    // sum18 : int (persists=false)
    // count19 : int (persists=false)
    fun printer () : void {
        found_tup17 = false;
        sum18 = 0;
        count19 = 0;
        sum20 = 0.0;
        cstart21 = 0;
        i22 = 0;
        count23 = 5;
        loop (i22 < count23) {
            cstart24 = cstart21;
            cstart25 = cstart24 + 1;
            tup16 = (buf[cstart24 : 1], buf[cstart25 : 1]);
            sum18 = sum18 + tup16[0];
            count19 = count19 + 1;
            sum20 = sum20 + int2fl(tup16[0]) / int2fl(2);
            found_tup17 = true;
            cstart21 = cstart21 + 2;
            i22 = i22 + 1;
        }
        if (found_tup17) {
            print(tuple[int, fixed], (sum18 + 5, int2fl(count19) + sum20));
        } else {

        }
    }
    // Locals:
    // tup3 : tuple[int, int] (persists=false)
    // cstart12 : int (persists=true)
    // count6 : int (persists=false)
    // sum5 : int (persists=false)
    // cstart8 : int (persists=true)
    // cstart11 : int (persists=true)
    // count10 : int (persists=true)
    // sum7 : fixed (persists=false)
    // i9 : int (persists=true)
    // found_tup4 : bool (persists=false)
    fun consumer () : void {
        found_tup4 = false;
        sum5 = 0;
        count6 = 0;
        sum7 = 0.0;
        cstart8 = 0;
        i9 = 0;
        count10 = 5;
        loop (i9 < count10) {
            cstart11 = cstart8;
            cstart12 = cstart11 + 1;
            tup3 = (buf[cstart11 : 1], buf[cstart12 : 1]);
            sum5 = sum5 + tup3[0];
            count6 = count6 + 1;
            sum7 = sum7 + int2fl(tup3[0]) / int2fl(2);
            found_tup4 = true;
            cstart8 = cstart8 + 2;
            i9 = i9 + 1;
        }
        if (found_tup4) {
            consume(tuple[int, fixed], (sum5 + 5, int2fl(count6) + sum7));
        } else {

        }
    } |}]

let%expect_test "sum" =
  run_test
    "Select([sum(f), count()], AList(r1 as k, ATuple([AScalar(k.f), \
     AScalar(k.g - k.f)], cross)))";
  [%expect
    {|
    (FuncT
     (((ListT
        ((TupleT
          (((IntT ((range (Interval 1 3)))) (IntT ((range (Interval -1 2)))))
           ((kind Cross))))
         ((count (Interval 5 5))))))
      (Width 2)))
    // Locals:
    // i18 : int (persists=true)
    // count16 : int (persists=false)
    // cstart21 : int (persists=true)
    // found_tup14 : bool (persists=false)
    // cstart17 : int (persists=true)
    // tup13 : tuple[int, int] (persists=false)
    // sum15 : int (persists=false)
    // count19 : int (persists=true)
    // cstart20 : int (persists=true)
    fun printer () : void {
        found_tup14 = false;
        sum15 = 0;
        count16 = 0;
        cstart17 = 0;
        i18 = 0;
        count19 = 5;
        loop (i18 < count19) {
            cstart20 = cstart17;
            cstart21 = cstart20 + 1;
            tup13 = (buf[cstart20 : 1], buf[cstart21 : 1]);
            sum15 = sum15 + tup13[0];
            count16 = count16 + 1;
            found_tup14 = true;
            cstart17 = cstart17 + 2;
            i18 = i18 + 1;
        }
        if (found_tup14) {
            print(tuple[int, int], (sum15, count16));
        } else {

        }
    }
    // Locals:
    // count8 : int (persists=true)
    // i7 : int (persists=true)
    // cstart9 : int (persists=true)
    // sum4 : int (persists=false)
    // count5 : int (persists=false)
    // tup2 : tuple[int, int] (persists=false)
    // found_tup3 : bool (persists=false)
    // cstart10 : int (persists=true)
    // cstart6 : int (persists=true)
    fun consumer () : void {
        found_tup3 = false;
        sum4 = 0;
        count5 = 0;
        cstart6 = 0;
        i7 = 0;
        count8 = 5;
        loop (i7 < count8) {
            cstart9 = cstart6;
            cstart10 = cstart9 + 1;
            tup2 = (buf[cstart9 : 1], buf[cstart10 : 1]);
            sum4 = sum4 + tup2[0];
            count5 = count5 + 1;
            found_tup3 = true;
            cstart6 = cstart6 + 2;
            i7 = i7 + 1;
        }
        if (found_tup3) {
            consume(tuple[int, int], (sum4, count5));
        } else {

        }
    } |}]

let%expect_test "cross-tuple" =
  run_test "AList(r1 as k, ATuple([AScalar(k.f), AScalar(k.g - k.f)], cross))";
  [%expect
    {|
    (ListT
     ((TupleT
       (((IntT ((range (Interval 1 3)))) (IntT ((range (Interval -1 2)))))
        ((kind Cross))))
      ((count (Interval 5 5)))))
    // Locals:
    // cstart8 : int (persists=true)
    // cstart9 : int (persists=true)
    // count7 : int (persists=true)
    // cstart5 : int (persists=true)
    // i6 : int (persists=true)
    fun printer () : void {
        cstart5 = 0;
        i6 = 0;
        count7 = 5;
        loop (i6 < count7) {
            cstart8 = cstart5;
            cstart9 = cstart8 + 1;
            print(tuple[int, int], (buf[cstart8 : 1], buf[cstart9 : 1]));
            cstart5 = cstart5 + 2;
            i6 = i6 + 1;
        }
    }
    // Locals:
    // i1 : int (persists=true)
    // cstart4 : int (persists=true)
    // cstart3 : int (persists=true)
    // cstart0 : int (persists=true)
    // count2 : int (persists=true)
    fun consumer () : void {
        cstart0 = 0;
        i1 = 0;
        count2 = 5;
        loop (i1 < count2) {
            cstart3 = cstart0;
            cstart4 = cstart3 + 1;
            consume(tuple[int, int], (buf[cstart3 : 1], buf[cstart4 : 1]));
            cstart0 = cstart0 + 2;
            i1 = i1 + 1;
        }
    } |}]

let%expect_test "hash-idx" =
  run_test
    "depjoin(AList(r1 as kl, AScalar(kl.f)) as k, AHashIdx(dedup(select([f], \
     r1)) as kh, ascalar(kh.f + 1), k.f))";
  [%expect
    {|
    (FuncT
     (((ListT ((IntT ((range (Interval 1 3)))) ((count (Interval 5 5)))))
       (HashIdxT
        ((IntT ((range (Interval 1 3)))) (IntT ((range (Interval 2 4))))
         ((key_count (Interval 3 3))))))
      Child_sum))
    // Locals:
    // kstart10 : int (persists=false)
    // i8 : int (persists=true)
    // cstart7 : int (persists=true)
    // count9 : int (persists=true)
    // key12 : tuple[int] (persists=false)
    // vstart11 : int (persists=false)
    // hash13 : int (persists=false)
    fun printer () : void {
        cstart7 = 1;
        i8 = 0;
        count9 = 5;
        loop (i8 < count9) {
            hash13 = buf[cstart7 : 1] * 1;
            if (hash13 < 0 || buf[7 : 1] - 1 < hash13) {

            } else {
                 kstart10 = buf[8 + hash13 : 1] + 8 + buf[7 : 1];
                 key12 = (buf[kstart10 : 1]);
                 vstart11 = buf[8 + hash13 : 1] + 8 + buf[7 : 1] + 1;
                 if (true && key12[0] == buf[cstart7 : 1]) {
                     print(tuple[int, int], (key12[0], buf[vstart11 : 1]));
                 } else {

                 }
            }
            cstart7 = cstart7 + 1;
            i8 = i8 + 1;
        }
    }
    // Locals:
    // kstart3 : int (persists=false)
    // key5 : tuple[int] (persists=false)
    // i1 : int (persists=true)
    // vstart4 : int (persists=false)
    // cstart0 : int (persists=true)
    // count2 : int (persists=true)
    // hash6 : int (persists=false)
    fun consumer () : void {
        cstart0 = 1;
        i1 = 0;
        count2 = 5;
        loop (i1 < count2) {
            hash6 = buf[cstart0 : 1] * 1;
            if (hash6 < 0 || buf[7 : 1] - 1 < hash6) {

            } else {
                 kstart3 = buf[8 + hash6 : 1] + 8 + buf[7 : 1];
                 key5 = (buf[kstart3 : 1]);
                 vstart4 = buf[8 + hash6 : 1] + 8 + buf[7 : 1] + 1;
                 if (true && key5[0] == buf[cstart0 : 1]) {
                     consume(tuple[int, int], (key5[0], buf[vstart4 : 1]));
                 } else {

                 }
            }
            cstart0 = cstart0 + 1;
            i1 = i1 + 1;
        }
    } |}]

let%expect_test "ordered-idx" =
  run_test
    "depjoin(AList(r1 as kl, AScalar(kl.f)) as k, AOrderedIdx(dedup(select([f \
     as kf], r1)) as k1, ascalar(k1.kf+1), k.f, k.f+1))";
  [%expect
    {|
    (FuncT
     (((ListT ((IntT ((range (Interval 1 3)))) ((count (Interval 5 5)))))
       (OrderedIdxT
        ((IntT ((range (Interval 1 3)))) (IntT ((range (Interval 2 4))))
         ((key_count (Interval 3 3))))))
      Child_sum))
    // Locals:
    // count16 : int (persists=true)
    // mid22 : int (persists=true)
    // key23 : tuple[int] (persists=false)
    // key19 : tuple[int] (persists=true)
    // kstart17 : int (persists=true)
    // key27 : tuple[int] (persists=false)
    // key25 : tuple[int] (persists=false)
    // key26 : tuple[int] (persists=true)
    // idx24 : int (persists=true)
    // vstart18 : int (persists=true)
    // i15 : int (persists=true)
    // cstart14 : int (persists=true)
    // low20 : int (persists=true)
    // high21 : int (persists=true)
    fun printer () : void {
        cstart14 = 0;
        i15 = 0;
        count16 = 5;
        loop (i15 < count16) {
            low20 = 0;
            high21 = 6 / 2;
            loop (low20 < high21) {
                mid22 = low20 + high21 / 2;
                kstart17 = 5 + mid22 * 2;
                key23 = (buf[kstart17 : 1]);
                if (not(key23[0] < buf[cstart14 : 1])) {
                    high21 = mid22;
                } else {
                     low20 = mid22 + 1;
                }
            }
            idx24 = low20;
            if (idx24 < 6 / 2) {
                kstart17 = 5 + idx24 * 2;
                key25 = (buf[kstart17 : 1]);
                key26 = key25;
                loop (key26[0] < buf[cstart14 : 1] + 1 && idx24 < 6 / 2) {
                    if (key26[0] < buf[cstart14 : 1] + 1 &&
                        not(key26[0] < buf[cstart14 : 1])) {
                        vstart18 = buf[5 + idx24 * 2 + 1 : 1] + 6 + 5;
                        key19 = key26;
                        print(tuple[int, int], (key19[0], buf[vstart18 : 1]));
                    } else {

                    }
                    idx24 = idx24 + 1;
                    kstart17 = 5 + idx24 * 2;
                    key27 = (buf[kstart17 : 1]);
                    key26 = key27;
                }
            } else {

            }
            cstart14 = cstart14 + 1;
            i15 = i15 + 1;
        }
    }
    // Locals:
    // idx10 : int (persists=true)
    // i1 : int (persists=true)
    // key13 : tuple[int] (persists=false)
    // vstart4 : int (persists=true)
    // mid8 : int (persists=true)
    // key9 : tuple[int] (persists=false)
    // key11 : tuple[int] (persists=false)
    // count2 : int (persists=true)
    // high7 : int (persists=true)
    // kstart3 : int (persists=true)
    // key5 : tuple[int] (persists=true)
    // low6 : int (persists=true)
    // key12 : tuple[int] (persists=true)
    // cstart0 : int (persists=true)
    fun consumer () : void {
        cstart0 = 0;
        i1 = 0;
        count2 = 5;
        loop (i1 < count2) {
            low6 = 0;
            high7 = 6 / 2;
            loop (low6 < high7) {
                mid8 = low6 + high7 / 2;
                kstart3 = 5 + mid8 * 2;
                key9 = (buf[kstart3 : 1]);
                if (not(key9[0] < buf[cstart0 : 1])) {
                    high7 = mid8;
                } else {
                     low6 = mid8 + 1;
                }
            }
            idx10 = low6;
            if (idx10 < 6 / 2) {
                kstart3 = 5 + idx10 * 2;
                key11 = (buf[kstart3 : 1]);
                key12 = key11;
                loop (key12[0] < buf[cstart0 : 1] + 1 && idx10 < 6 / 2) {
                    if (key12[0] < buf[cstart0 : 1] + 1 &&
                        not(key12[0] < buf[cstart0 : 1])) {
                        vstart4 = buf[5 + idx10 * 2 + 1 : 1] + 6 + 5;
                        key5 = key12;
                        consume(tuple[int, int], (key5[0], buf[vstart4 : 1]));
                    } else {

                    }
                    idx10 = idx10 + 1;
                    kstart3 = 5 + idx10 * 2;
                    key13 = (buf[kstart3 : 1]);
                    key12 = key13;
                }
            } else {

            }
            cstart0 = cstart0 + 1;
            i1 = i1 + 1;
        }
    } |}]

let%expect_test "ordered-idx-date" =
  run_test ~print_code:false
    {|AOrderedIdx(dedup(select([f], r_date)) as k, ascalar(k.f as ff), date("2018-01-01"), date("2018-01-01"))|};
  [%expect
    {|
    (OrderedIdxT
     ((DateT ((range (Interval 17136 17775))))
      (DateT ((range (Interval 17136 17775)))) ((key_count (Interval 5 5))))) |}]

let%expect_test "example-1" =
  Demomatch.(run_test ~params:Demomatch.example_params (example1 "log"));
  [%expect
    {|
    (FuncT
     (((FuncT
        (((ListT
           ((TupleT
             (((IntT ((range (Interval 1 1)))) (IntT ((range (Interval 1 4))))
               (ListT
                ((TupleT
                  (((IntT ((range (Interval 2 3))))
                    (IntT ((range (Interval 2 5)))))
                   ((kind Cross))))
                 ((count (Interval 1 2))))))
              ((kind Cross))))
            ((count (Interval 2 2))))))
         Child_sum)))
      (Width 2)))
    // Locals:
    // i18 : int (persists=true)
    // cstart21 : int (persists=true)
    // cstart16 : int (persists=true)
    // count13 : int (persists=true)
    // cstart17 : int (persists=true)
    // cstart15 : int (persists=true)
    // cstart11 : int (persists=true)
    // i12 : int (persists=true)
    // cstart14 : int (persists=true)
    // count19 : int (persists=true)
    // cstart20 : int (persists=true)
    fun printer () : void {
        cstart11 = 1;
        i12 = 0;
        count13 = 2;
        loop (i12 < count13) {
            cstart14 = cstart11 + 1;
            cstart15 = cstart14 + 1;
            cstart16 = cstart15 + 1;
            cstart17 = cstart16 + 1 + 1;
            i18 = 0;
            count19 = buf[cstart16 : 1];
            loop (i18 < count19) {
                cstart20 = cstart17;
                cstart21 = cstart20 + 1;
                if (buf[cstart20 : 1] == id_c && buf[cstart14 : 1] == id_p) {
                    print(tuple[int, int],
                    (buf[cstart15 : 1], buf[cstart21 : 1]));
                } else {

                }
                cstart17 = cstart17 + 2;
                i18 = i18 + 1;
            }
            cstart11 = cstart11 + buf[cstart11 : 1];
            i12 = i12 + 1;
        }
    }
    // Locals:
    // i1 : int (persists=true)
    // count8 : int (persists=true)
    // i7 : int (persists=true)
    // cstart9 : int (persists=true)
    // count2 : int (persists=true)
    // cstart4 : int (persists=true)
    // cstart3 : int (persists=true)
    // cstart10 : int (persists=true)
    // cstart0 : int (persists=true)
    // cstart6 : int (persists=true)
    // cstart5 : int (persists=true)
    fun consumer () : void {
        cstart0 = 1;
        i1 = 0;
        count2 = 2;
        loop (i1 < count2) {
            cstart3 = cstart0 + 1;
            cstart4 = cstart3 + 1;
            cstart5 = cstart4 + 1;
            cstart6 = cstart5 + 1 + 1;
            i7 = 0;
            count8 = buf[cstart5 : 1];
            loop (i7 < count8) {
                cstart9 = cstart6;
                cstart10 = cstart9 + 1;
                if (buf[cstart9 : 1] == id_c && buf[cstart3 : 1] == id_p) {
                    consume(tuple[int, int],
                    (buf[cstart4 : 1], buf[cstart10 : 1]));
                } else {

                }
                cstart6 = cstart6 + 2;
                i7 = i7 + 1;
            }
            cstart0 = cstart0 + buf[cstart0 : 1];
            i1 = i1 + 1;
        }
    } |}]

let%expect_test "example-2" =
  Demomatch.(run_test ~params:example_params (example2 "log"));
  [%expect
    {|
    (FuncT
     (((HashIdxT
        ((TupleT
          (((IntT ((range (Interval 1 1)))) (IntT ((range (Interval 2 3)))))
           ((kind Cross))))
         (ListT
          ((TupleT
            (((IntT ((range (Interval 1 4)))) (IntT ((range (Interval 2 5)))))
             ((kind Cross))))
           ((count (Interval 1 2)))))
         ((key_count (Interval 2 2))))))
      (Width 2)))
    // Locals:
    // i18 : int (persists=true)
    // key13 : tuple[int, int] (persists=false)
    // cstart21 : int (persists=true)
    // cstart16 : int (persists=true)
    // hash14 : int (persists=false)
    // cstart17 : int (persists=true)
    // kstart11 : int (persists=false)
    // cstart15 : int (persists=true)
    // vstart12 : int (persists=false)
    // count19 : int (persists=true)
    // cstart20 : int (persists=true)
    fun printer () : void {
        hash14 = <tuplehash> * 1;
        if (hash14 < 0 || buf[4 + buf[2 : 2] : 1] - 1 < hash14) {

        } else {
             kstart11 =
             buf[4 + buf[2 : 2] + 1 + hash14 : 1] +
             4 + buf[2 : 2] + 1 + buf[4 + buf[2 : 2] : 1];
             cstart15 = kstart11;
             cstart16 = cstart15 + 1;
             key13 = (buf[cstart15 : 1], buf[cstart16 : 1]);
             vstart12 =
             buf[4 + buf[2 : 2] + 1 + hash14 : 1] +
             4 + buf[2 : 2] + 1 + buf[4 + buf[2 : 2] : 1] + 2;
             if (true && key13[0] == id_p && key13[1] == id_c) {
                 cstart17 = vstart12 + 1 + 1;
                 i18 = 0;
                 count19 = buf[vstart12 : 1];
                 loop (i18 < count19) {
                     cstart20 = cstart17;
                     cstart21 = cstart20 + 1;
                     print(tuple[int, int],
                     (buf[cstart20 : 1], buf[cstart21 : 1]));
                     cstart17 = cstart17 + 2;
                     i18 = i18 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // count8 : int (persists=true)
    // i7 : int (persists=true)
    // cstart9 : int (persists=true)
    // kstart0 : int (persists=false)
    // hash3 : int (persists=false)
    // vstart1 : int (persists=false)
    // key2 : tuple[int, int] (persists=false)
    // cstart4 : int (persists=true)
    // cstart10 : int (persists=true)
    // cstart6 : int (persists=true)
    // cstart5 : int (persists=true)
    fun consumer () : void {
        hash3 = <tuplehash> * 1;
        if (hash3 < 0 || buf[4 + buf[2 : 2] : 1] - 1 < hash3) {

        } else {
             kstart0 =
             buf[4 + buf[2 : 2] + 1 + hash3 : 1] +
             4 + buf[2 : 2] + 1 + buf[4 + buf[2 : 2] : 1];
             cstart4 = kstart0;
             cstart5 = cstart4 + 1;
             key2 = (buf[cstart4 : 1], buf[cstart5 : 1]);
             vstart1 =
             buf[4 + buf[2 : 2] + 1 + hash3 : 1] +
             4 + buf[2 : 2] + 1 + buf[4 + buf[2 : 2] : 1] + 2;
             if (true && key2[0] == id_p && key2[1] == id_c) {
                 cstart6 = vstart1 + 1 + 1;
                 i7 = 0;
                 count8 = buf[vstart1 : 1];
                 loop (i7 < count8) {
                     cstart9 = cstart6;
                     cstart10 = cstart9 + 1;
                     consume(tuple[int, int],
                     (buf[cstart9 : 1], buf[cstart10 : 1]));
                     cstart6 = cstart6 + 2;
                     i7 = i7 + 1;
                 }
             } else {

             }
        }
    } |}]

let%expect_test "example-3" =
  Demomatch.(run_test ~params:example_params (example3 "log"));
  [%expect
    {|
    (FuncT
     (((FuncT
        (((HashIdxT
           ((IntT ((range (Interval 1 3))))
            (ListT
             ((TupleT
               (((IntT ((range (Interval 1 5)))) (IntT ((range (Interval 3 6)))))
                ((kind Cross))))
              ((count (Interval 1 2)))))
            ((key_count (Interval 3 3)))))
          (FuncT
           (((FuncT
              (((OrderedIdxT
                 ((IntT ((range (Interval 1 5))))
                  (ListT
                   ((TupleT
                     (((IntT ((range (Interval 1 3))))
                       (IntT ((range (Interval 1 5)))))
                      ((kind Cross))))
                    ((count (Interval 1 1)))))
                  ((key_count (Interval 5 5))))))
               Child_sum)))
            (Width 2))))
         Child_sum)))
      (Width 2)))
    // Locals:
    // i30 : int (persists=true)
    // cstart29 : int (persists=true)
    // vstart35 : int (persists=true)
    // cstart47 : int (persists=true)
    // i45 : int (persists=true)
    // cstart44 : int (persists=true)
    // vstart26 : int (persists=false)
    // key42 : tuple[int] (persists=false)
    // cstart48 : int (persists=true)
    // low37 : int (persists=true)
    // key36 : tuple[int] (persists=true)
    // kstart25 : int (persists=false)
    // key43 : tuple[int] (persists=true)
    // count31 : int (persists=true)
    // idx41 : int (persists=true)
    // cstart33 : int (persists=true)
    // mid39 : int (persists=true)
    // key27 : tuple[int] (persists=false)
    // key49 : tuple[int] (persists=false)
    // count46 : int (persists=true)
    // hash28 : int (persists=false)
    // kstart34 : int (persists=true)
    // high38 : int (persists=true)
    // key40 : tuple[int] (persists=false)
    // cstart32 : int (persists=true)
    fun printer () : void {
        hash28 = id_p * 1;
        if (hash28 < 0 || buf[2 : 1] - 1 < hash28) {

        } else {
             kstart25 = buf[3 + hash28 : 1] + 3 + buf[2 : 1];
             key27 = (buf[kstart25 : 1]);
             vstart26 = buf[3 + hash28 : 1] + 3 + buf[2 : 1] + 1;
             if (true && key27[0] == id_p) {
                 cstart29 = vstart26 + 1 + 1;
                 i30 = 0;
                 count31 = buf[vstart26 : 1];
                 loop (i30 < count31) {
                     cstart32 = cstart29;
                     cstart33 = cstart32 + 1;
                     low37 = 0;
                     high38 = 10 / 2;
                     loop (low37 < high38) {
                         mid39 = low37 + high38 / 2;
                         kstart34 = 1 + buf[1 : 1] + mid39 * 2;
                         key40 = (buf[kstart34 : 1]);
                         if (not(key40[0] < buf[cstart32 : 1])) {
                             high38 = mid39;
                         } else {
                              low37 = mid39 + 1;
                         }
                     }
                     idx41 = low37;
                     if (idx41 < 10 / 2) {
                         kstart34 = 1 + buf[1 : 1] + idx41 * 2;
                         key42 = (buf[kstart34 : 1]);
                         key43 = key42;
                         loop (key43[0] < buf[cstart33 : 1] && idx41 < 10 / 2) {
                             if (key43[0] < buf[cstart33 : 1] &&
                                 not(key43[0] < buf[cstart32 : 1])) {
                                 vstart35 =
                                 buf[1 + buf[1 : 1] + idx41 * 2 + 1 : 1] + 10 +
                                 1 + buf[1 : 1];
                                 key36 = key43;
                                 cstart44 = vstart35;
                                 i45 = 0;
                                 count46 = 1;
                                 loop (i45 < count46) {
                                     cstart47 = cstart44;
                                     cstart48 = cstart47 + 1;
                                     if (buf[cstart47 : 1] == id_c) {
                                         print(tuple[int, int],
                                         (buf[cstart32 : 1], buf[cstart48 : 1]));
                                     } else {

                                     }
                                     cstart44 = cstart44 + 2;
                                     i45 = i45 + 1;
                                 }
                             } else {

                             }
                             idx41 = idx41 + 1;
                             kstart34 = 1 + buf[1 : 1] + idx41 * 2;
                             key49 = (buf[kstart34 : 1]);
                             key43 = key49;
                         }
                     } else {

                     }
                     cstart29 = cstart29 + 2;
                     i30 = i30 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // i5 : int (persists=true)
    // vstart10 : int (persists=true)
    // key17 : tuple[int] (persists=false)
    // key11 : tuple[int] (persists=true)
    // cstart22 : int (persists=true)
    // cstart8 : int (persists=true)
    // key24 : tuple[int] (persists=false)
    // key2 : tuple[int] (persists=false)
    // cstart4 : int (persists=true)
    // cstart23 : int (persists=true)
    // cstart19 : int (persists=true)
    // cstart7 : int (persists=true)
    // mid14 : int (persists=true)
    // count6 : int (persists=true)
    // key15 : tuple[int] (persists=false)
    // kstart0 : int (persists=false)
    // i20 : int (persists=true)
    // hash3 : int (persists=false)
    // count21 : int (persists=true)
    // vstart1 : int (persists=false)
    // idx16 : int (persists=true)
    // high13 : int (persists=true)
    // key18 : tuple[int] (persists=true)
    // low12 : int (persists=true)
    // kstart9 : int (persists=true)
    fun consumer () : void {
        hash3 = id_p * 1;
        if (hash3 < 0 || buf[2 : 1] - 1 < hash3) {

        } else {
             kstart0 = buf[3 + hash3 : 1] + 3 + buf[2 : 1];
             key2 = (buf[kstart0 : 1]);
             vstart1 = buf[3 + hash3 : 1] + 3 + buf[2 : 1] + 1;
             if (true && key2[0] == id_p) {
                 cstart4 = vstart1 + 1 + 1;
                 i5 = 0;
                 count6 = buf[vstart1 : 1];
                 loop (i5 < count6) {
                     cstart7 = cstart4;
                     cstart8 = cstart7 + 1;
                     low12 = 0;
                     high13 = 10 / 2;
                     loop (low12 < high13) {
                         mid14 = low12 + high13 / 2;
                         kstart9 = 1 + buf[1 : 1] + mid14 * 2;
                         key15 = (buf[kstart9 : 1]);
                         if (not(key15[0] < buf[cstart7 : 1])) {
                             high13 = mid14;
                         } else {
                              low12 = mid14 + 1;
                         }
                     }
                     idx16 = low12;
                     if (idx16 < 10 / 2) {
                         kstart9 = 1 + buf[1 : 1] + idx16 * 2;
                         key17 = (buf[kstart9 : 1]);
                         key18 = key17;
                         loop (key18[0] < buf[cstart8 : 1] && idx16 < 10 / 2) {
                             if (key18[0] < buf[cstart8 : 1] &&
                                 not(key18[0] < buf[cstart7 : 1])) {
                                 vstart10 =
                                 buf[1 + buf[1 : 1] + idx16 * 2 + 1 : 1] + 10 +
                                 1 + buf[1 : 1];
                                 key11 = key18;
                                 cstart19 = vstart10;
                                 i20 = 0;
                                 count21 = 1;
                                 loop (i20 < count21) {
                                     cstart22 = cstart19;
                                     cstart23 = cstart22 + 1;
                                     if (buf[cstart22 : 1] == id_c) {
                                         consume(tuple[int, int],
                                         (buf[cstart7 : 1], buf[cstart23 : 1]));
                                     } else {

                                     }
                                     cstart19 = cstart19 + 2;
                                     i20 = i20 + 1;
                                 }
                             } else {

                             }
                             idx16 = idx16 + 1;
                             kstart9 = 1 + buf[1 : 1] + idx16 * 2;
                             key24 = (buf[kstart9 : 1]);
                             key18 = key24;
                         }
                     } else {

                     }
                     cstart4 = cstart4 + 2;
                     i5 = i5 + 1;
                 }
             } else {

             }
        }
    } |}]

let%expect_test "subquery-first" =
  run_test ~params:Demomatch.example_params
    {|
    select([id], filter((select([min(counter)],
 alist(log as l, ascalar(l.counter)))) = id, alist(log as ll, ascalar(ll.id))))
|};
  [%expect
    {|
    (FuncT
     (((FuncT
        (((ListT ((IntT ((range (Interval 1 3)))) ((count (Interval 5 5))))))
         Child_sum)))
      (Width 1)))
    // Locals:
    // found_tup17 : bool (persists=false)
    // tup16 : tuple[int] (persists=false)
    // count13 : int (persists=true)
    // min18 : int (persists=false)
    // first14 : int (persists=true)
    // i20 : int (persists=true)
    // cstart11 : int (persists=true)
    // i12 : int (persists=true)
    // count21 : int (persists=true)
    // cstart19 : int (persists=true)
    fun printer () : void {
        cstart11 = 0;
        i12 = 0;
        count13 = 5;
        loop (i12 < count13) {
            found_tup17 = false;
            min18 = 4611686018427387903;
            cstart19 = 0;
            i20 = 0;
            count21 = 5;
            loop (i20 < count21) {
                tup16 = (buf[cstart19 : 1]);
                min18 = tup16[0] < min18 ? tup16[0] : min18;
                found_tup17 = true;
                cstart19 = cstart19 + 1;
                i20 = i20 + 1;
            }
            if (found_tup17) {
                first14 = min18;
            } else {

            }
            if (first14 == buf[cstart11 : 1]) {
                print(tuple[int], (buf[cstart11 : 1]));
            } else {

            }
            cstart11 = cstart11 + 1;
            i12 = i12 + 1;
        }
    }
    // Locals:
    // i1 : int (persists=true)
    // first3 : int (persists=true)
    // count2 : int (persists=true)
    // min7 : int (persists=false)
    // cstart8 : int (persists=true)
    // count10 : int (persists=true)
    // tup5 : tuple[int] (persists=false)
    // i9 : int (persists=true)
    // found_tup6 : bool (persists=false)
    // cstart0 : int (persists=true)
    fun consumer () : void {
        cstart0 = 0;
        i1 = 0;
        count2 = 5;
        loop (i1 < count2) {
            found_tup6 = false;
            min7 = 4611686018427387903;
            cstart8 = 0;
            i9 = 0;
            count10 = 5;
            loop (i9 < count10) {
                tup5 = (buf[cstart8 : 1]);
                min7 = tup5[0] < min7 ? tup5[0] : min7;
                found_tup6 = true;
                cstart8 = cstart8 + 1;
                i9 = i9 + 1;
            }
            if (found_tup6) {
                first3 = min7;
            } else {

            }
            if (first3 == buf[cstart0 : 1]) {
                consume(tuple[int], (buf[cstart0 : 1]));
            } else {

            }
            cstart0 = cstart0 + 1;
            i1 = i1 + 1;
        }
    } |}]

let%expect_test "example-3-str" =
  Demomatch.(run_test ~params:example_db_params (example3 "log_str"));
  [%expect
    {|
    (FuncT
     (((FuncT
        (((HashIdxT
           ((StringT ((nchars (Interval 3 8))))
            (ListT
             ((TupleT
               (((IntT ((range (Interval 1 5)))) (IntT ((range (Interval 3 6)))))
                ((kind Cross))))
              ((count (Interval 1 2)))))
            ((key_count (Interval 3 3)))))
          (FuncT
           (((FuncT
              (((OrderedIdxT
                 ((IntT ((range (Interval 1 5))))
                  (ListT
                   ((TupleT
                     (((StringT ((nchars (Interval 3 8))))
                       (IntT ((range (Interval 1 5)))))
                      ((kind Cross))))
                    ((count (Interval 1 1)))))
                  ((key_count (Interval 5 5))))))
               Child_sum)))
            (Width 2))))
         Child_sum)))
      (Width 2)))
    // Locals:
    // i30 : int (persists=true)
    // cstart29 : int (persists=true)
    // vstart35 : int (persists=true)
    // cstart47 : int (persists=true)
    // i45 : int (persists=true)
    // cstart44 : int (persists=true)
    // vstart26 : int (persists=false)
    // key42 : tuple[int] (persists=false)
    // cstart48 : int (persists=true)
    // low37 : int (persists=true)
    // key36 : tuple[int] (persists=true)
    // kstart25 : int (persists=false)
    // key43 : tuple[int] (persists=true)
    // count31 : int (persists=true)
    // idx41 : int (persists=true)
    // cstart33 : int (persists=true)
    // mid39 : int (persists=true)
    // key27 : tuple[string] (persists=false)
    // key49 : tuple[int] (persists=false)
    // count46 : int (persists=true)
    // hash28 : int (persists=false)
    // kstart34 : int (persists=true)
    // high38 : int (persists=true)
    // key40 : tuple[int] (persists=false)
    // cstart32 : int (persists=true)
    fun printer () : void {
        hash28 = hash(6, id_p) * 1;
        if (hash28 < 0 || buf[6 + buf[4 : 2] : 1] - 1 < hash28) {

        } else {
             kstart25 =
             buf[6 + buf[4 : 2] + 1 + hash28 : 1] +
             6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1];
             key27 = (load_str(kstart25 + 1, buf[kstart25 : 1]));
             vstart26 =
             buf[6 + buf[4 : 2] + 1 + hash28 : 1] +
             6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1] +
             1 +
             buf[buf[6 + buf[4 : 2] + 1 + hash28 : 1] +
                 6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1] : 1];
             if (true && key27[0] == id_p) {
                 cstart29 = vstart26 + 1 + 1;
                 i30 = 0;
                 count31 = buf[vstart26 : 1];
                 loop (i30 < count31) {
                     cstart32 = cstart29;
                     cstart33 = cstart32 + 1;
                     low37 = 0;
                     high38 = 10 / 2;
                     loop (low37 < high38) {
                         mid39 = low37 + high38 / 2;
                         kstart34 = 2 + buf[2 : 2] + 1 + mid39 * 2;
                         key40 = (buf[kstart34 : 1]);
                         if (not(key40[0] < buf[cstart32 : 1])) {
                             high38 = mid39;
                         } else {
                              low37 = mid39 + 1;
                         }
                     }
                     idx41 = low37;
                     if (idx41 < 10 / 2) {
                         kstart34 = 2 + buf[2 : 2] + 1 + idx41 * 2;
                         key42 = (buf[kstart34 : 1]);
                         key43 = key42;
                         loop (key43[0] < buf[cstart33 : 1] && idx41 < 10 / 2) {
                             if (key43[0] < buf[cstart33 : 1] &&
                                 not(key43[0] < buf[cstart32 : 1])) {
                                 vstart35 =
                                 buf[2 + buf[2 : 2] + 1 + idx41 * 2 + 1 : 1] + 10 +
                                 2 + buf[2 : 2] + 1;
                                 key36 = key43;
                                 cstart44 = vstart35 + 1;
                                 i45 = 0;
                                 count46 = 1;
                                 loop (i45 < count46) {
                                     cstart47 = cstart44 + 1;
                                     cstart48 = cstart47 + 1 + buf[cstart47 : 1];
                                     if (load_str(cstart47 + 1, buf[cstart47 :
                                         1]) == id_c) {
                                         print(tuple[int, int],
                                         (buf[cstart32 : 1], buf[cstart48 : 1]));
                                     } else {

                                     }
                                     cstart44 = cstart44 + buf[cstart44 : 1];
                                     i45 = i45 + 1;
                                 }
                             } else {

                             }
                             idx41 = idx41 + 1;
                             kstart34 = 2 + buf[2 : 2] + 1 + idx41 * 2;
                             key49 = (buf[kstart34 : 1]);
                             key43 = key49;
                         }
                     } else {

                     }
                     cstart29 = cstart29 + 2;
                     i30 = i30 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // i5 : int (persists=true)
    // vstart10 : int (persists=true)
    // key17 : tuple[int] (persists=false)
    // key11 : tuple[int] (persists=true)
    // cstart22 : int (persists=true)
    // cstart8 : int (persists=true)
    // key24 : tuple[int] (persists=false)
    // key2 : tuple[string] (persists=false)
    // cstart4 : int (persists=true)
    // cstart23 : int (persists=true)
    // cstart19 : int (persists=true)
    // cstart7 : int (persists=true)
    // mid14 : int (persists=true)
    // count6 : int (persists=true)
    // key15 : tuple[int] (persists=false)
    // kstart0 : int (persists=false)
    // i20 : int (persists=true)
    // hash3 : int (persists=false)
    // count21 : int (persists=true)
    // vstart1 : int (persists=false)
    // idx16 : int (persists=true)
    // high13 : int (persists=true)
    // key18 : tuple[int] (persists=true)
    // low12 : int (persists=true)
    // kstart9 : int (persists=true)
    fun consumer () : void {
        hash3 = hash(6, id_p) * 1;
        if (hash3 < 0 || buf[6 + buf[4 : 2] : 1] - 1 < hash3) {

        } else {
             kstart0 =
             buf[6 + buf[4 : 2] + 1 + hash3 : 1] +
             6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1];
             key2 = (load_str(kstart0 + 1, buf[kstart0 : 1]));
             vstart1 =
             buf[6 + buf[4 : 2] + 1 + hash3 : 1] +
             6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1] +
             1 +
             buf[buf[6 + buf[4 : 2] + 1 + hash3 : 1] +
                 6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1] : 1];
             if (true && key2[0] == id_p) {
                 cstart4 = vstart1 + 1 + 1;
                 i5 = 0;
                 count6 = buf[vstart1 : 1];
                 loop (i5 < count6) {
                     cstart7 = cstart4;
                     cstart8 = cstart7 + 1;
                     low12 = 0;
                     high13 = 10 / 2;
                     loop (low12 < high13) {
                         mid14 = low12 + high13 / 2;
                         kstart9 = 2 + buf[2 : 2] + 1 + mid14 * 2;
                         key15 = (buf[kstart9 : 1]);
                         if (not(key15[0] < buf[cstart7 : 1])) {
                             high13 = mid14;
                         } else {
                              low12 = mid14 + 1;
                         }
                     }
                     idx16 = low12;
                     if (idx16 < 10 / 2) {
                         kstart9 = 2 + buf[2 : 2] + 1 + idx16 * 2;
                         key17 = (buf[kstart9 : 1]);
                         key18 = key17;
                         loop (key18[0] < buf[cstart8 : 1] && idx16 < 10 / 2) {
                             if (key18[0] < buf[cstart8 : 1] &&
                                 not(key18[0] < buf[cstart7 : 1])) {
                                 vstart10 =
                                 buf[2 + buf[2 : 2] + 1 + idx16 * 2 + 1 : 1] + 10 +
                                 2 + buf[2 : 2] + 1;
                                 key11 = key18;
                                 cstart19 = vstart10 + 1;
                                 i20 = 0;
                                 count21 = 1;
                                 loop (i20 < count21) {
                                     cstart22 = cstart19 + 1;
                                     cstart23 = cstart22 + 1 + buf[cstart22 : 1];
                                     if (load_str(cstart22 + 1, buf[cstart22 :
                                         1]) == id_c) {
                                         consume(tuple[int, int],
                                         (buf[cstart7 : 1], buf[cstart23 : 1]));
                                     } else {

                                     }
                                     cstart19 = cstart19 + buf[cstart19 : 1];
                                     i20 = i20 + 1;
                                 }
                             } else {

                             }
                             idx16 = idx16 + 1;
                             kstart9 = 2 + buf[2 : 2] + 1 + idx16 * 2;
                             key24 = (buf[kstart9 : 1]);
                             key18 = key24;
                         }
                     } else {

                     }
                     cstart4 = cstart4 + 2;
                     i5 = i5 + 1;
                 }
             } else {

             }
        }
    } |}]
