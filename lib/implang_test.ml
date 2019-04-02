open Core
open Base
open Abslayout
open Test_util

let run_test ?(params = []) ?(print_code = true) layout_str =
  let (module M), (module S), (module I), (module C) =
    make_modules ~code_only:true ()
  in
  try
    let param_names = List.map params ~f:(fun (n, _) -> n) in
    let sparams = Set.of_list (module Name) param_names in
    let layout = of_string_exn layout_str |> M.resolve ~params:sparams in
    M.annotate_schema layout ;
    let layout = M.annotate_key_layouts layout in
    M.annotate_subquery_types layout ;
    let type_ = M.to_type layout in
    Stdio.print_endline (Sexp.to_string_hum ([%sexp_of: Type.t] type_)) ;
    let ir = I.irgen ~params:param_names ~data_fn:"/tmp/buf" layout in
    if print_code then I.pp Caml.Format.std_formatter ir
  with exn ->
    Backtrace.(
      elide := false ;
      Exn.most_recent () |> to_string |> Stdio.print_endline) ;
    Exn.(to_string exn |> Stdio.print_endline)

let%expect_test "tuple-simple-cross" =
  run_test "ATuple([AScalar(1), AScalar(2)], cross)" ;
  [%expect
    {|
    (TupleT
     (((IntT ((range (Interval 1 1)) (nullable false)))
       (IntT ((range (Interval 2 2)) (nullable false))))
      ((count (Interval 1 1)))))
    // Locals:
    // cstart3 : Int[nonnull] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart2 = 0;
        cstart3 = cstart2 + 1;
        print(Tuple[Int[nonnull], Int[nonnull]],
        (buf[cstart2 : 1], buf[cstart3 : 1]));
    }
    // Locals:
    // cstart1 : Int[nonnull] (persists=true)
    // cstart0 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        cstart0 = 0;
        cstart1 = cstart0 + 1;
        consume(Tuple[Int[nonnull], Int[nonnull]],
        (buf[cstart0 : 1], buf[cstart1 : 1]));
    } |}]

let%expect_test "tuple-simple-zip" =
  run_test "ATuple([AScalar(1), AScalar(2)], zip)" ;
  [%expect
    {|
    (TupleT
     (((IntT ((range (Interval 1 1)) (nullable false)))
       (IntT ((range (Interval 2 2)) (nullable false))))
      ((count (Interval 1 1)))))
    // Locals:
    // start : Int[nonnull] (persists=true)
    fun zt_9 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // start : Int[nonnull] (persists=true)
    fun zt_8 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // start : Int[nonnull] (persists=true)
    fun zt_2 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // start : Int[nonnull] (persists=true)
    fun zt_1 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // i12 : Int[nonnull] (persists=true)
    // cstart7 : Int[nonnull] (persists=true)
    // tup10 : Tuple[Int[nonnull]] (persists=true)
    // tup11 : Tuple[Int[nonnull]] (persists=true)
    // count13 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart7 = 0;
        cstart7 = 0;
        init zt_8(cstart7);
        cstart7 = cstart7 + 1;
        init zt_9(cstart7);
        cstart7 = cstart7 + 1;
        i12 = 0;
        count13 = 1;
        loop (i12 < count13) {
            tup10 = next(zt_8);
            tup11 = next(zt_9);
            print(Tuple[Int[nonnull], Int[nonnull]], (tup10[0], tup11[0]));
            i12 = i12 + 1;
        }
    }
    // Locals:
    // tup4 : Tuple[Int[nonnull]] (persists=true)
    // i5 : Int[nonnull] (persists=true)
    // tup3 : Tuple[Int[nonnull]] (persists=true)
    // count6 : Int[nonnull] (persists=true)
    // cstart0 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        cstart0 = 0;
        cstart0 = 0;
        init zt_1(cstart0);
        cstart0 = cstart0 + 1;
        init zt_2(cstart0);
        cstart0 = cstart0 + 1;
        i5 = 0;
        count6 = 1;
        loop (i5 < count6) {
            tup3 = next(zt_1);
            tup4 = next(zt_2);
            consume(Tuple[Int[nonnull], Int[nonnull]], (tup3[0], tup4[0]));
            i5 = i5 + 1;
        }
    } |}]

let%expect_test "sum-complex" =
  run_test
    "Select([sum(r1.f) + 5, count() + sum(r1.f / 2)], AList(r1, \
     ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross)))" ;
  [%expect
    {|
    [WARNING] Cross-stage shadowing of r1.f.
    (FuncT
     (((ListT
        ((TupleT
          (((IntT ((range (Interval 1 3)) (nullable false)))
            (IntT ((range (Interval -1 2)) (nullable false))))
           ((count (Interval 1 1)))))
         ((count (Interval 5 5))))))
      (Width 2)))
    // Locals:
    // found_tup17 : Bool[nonnull] (persists=false)
    // cstart24 : Int[nonnull] (persists=true)
    // cstart25 : Int[nonnull] (persists=true)
    // cstart21 : Int[nonnull] (persists=true)
    // tup16 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // i22 : Int[nonnull] (persists=true)
    // count23 : Int[nonnull] (persists=true)
    // sum20 : Fixed[nonnull] (persists=false)
    // sum18 : Int[nonnull] (persists=false)
    // count19 : Int[nonnull] (persists=false)
    fun printer () : Void {
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
            sum20 = sum20 + int2fl tup16[0] / int2fl 2;
            found_tup17 = true;
            cstart21 = cstart21 + 2;
            i22 = i22 + 1;
        }
        if (found_tup17) {
            print(Tuple[Int[nonnull], Fixed[nonnull]],
            (sum18 + 5, int2fl count19 + sum20));
        } else {

        }
    }
    // Locals:
    // tup3 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // cstart12 : Int[nonnull] (persists=true)
    // count6 : Int[nonnull] (persists=false)
    // sum5 : Int[nonnull] (persists=false)
    // cstart8 : Int[nonnull] (persists=true)
    // cstart11 : Int[nonnull] (persists=true)
    // count10 : Int[nonnull] (persists=true)
    // sum7 : Fixed[nonnull] (persists=false)
    // i9 : Int[nonnull] (persists=true)
    // found_tup4 : Bool[nonnull] (persists=false)
    fun consumer () : Void {
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
            sum7 = sum7 + int2fl tup3[0] / int2fl 2;
            found_tup4 = true;
            cstart8 = cstart8 + 2;
            i9 = i9 + 1;
        }
        if (found_tup4) {
            consume(Tuple[Int[nonnull], Fixed[nonnull]],
            (sum5 + 5, int2fl count6 + sum7));
        } else {

        }
    } |}]

let%expect_test "sum" =
  run_test
    "Select([sum(r1.f), count()], AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g - \
     r1.f)], cross)))" ;
  [%expect
    {|
    [WARNING] Cross-stage shadowing of r1.f.
    (FuncT
     (((ListT
        ((TupleT
          (((IntT ((range (Interval 1 3)) (nullable false)))
            (IntT ((range (Interval -1 2)) (nullable false))))
           ((count (Interval 1 1)))))
         ((count (Interval 5 5))))))
      (Width 2)))
    // Locals:
    // i18 : Int[nonnull] (persists=true)
    // count16 : Int[nonnull] (persists=false)
    // cstart21 : Int[nonnull] (persists=true)
    // found_tup14 : Bool[nonnull] (persists=false)
    // cstart17 : Int[nonnull] (persists=true)
    // tup13 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // sum15 : Int[nonnull] (persists=false)
    // count19 : Int[nonnull] (persists=true)
    // cstart20 : Int[nonnull] (persists=true)
    fun printer () : Void {
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
            print(Tuple[Int[nonnull], Int[nonnull]], (sum15, count16));
        } else {

        }
    }
    // Locals:
    // count8 : Int[nonnull] (persists=true)
    // i7 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // sum4 : Int[nonnull] (persists=false)
    // count5 : Int[nonnull] (persists=false)
    // tup2 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // found_tup3 : Bool[nonnull] (persists=false)
    // cstart10 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    fun consumer () : Void {
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
            consume(Tuple[Int[nonnull], Int[nonnull]], (sum4, count5));
        } else {

        }
    } |}]

let%expect_test "cross-tuple" =
  run_test "AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross))" ;
  [%expect
    {|
    [WARNING] Cross-stage shadowing of r1.f.
    (ListT
     ((TupleT
       (((IntT ((range (Interval 1 3)) (nullable false)))
         (IntT ((range (Interval -1 2)) (nullable false))))
        ((count (Interval 1 1)))))
      ((count (Interval 5 5)))))
    // Locals:
    // cstart8 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // count7 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    // i6 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart5 = 0;
        i6 = 0;
        count7 = 5;
        loop (i6 < count7) {
            cstart8 = cstart5;
            cstart9 = cstart8 + 1;
            print(Tuple[Int[nonnull], Int[nonnull]],
            (buf[cstart8 : 1], buf[cstart9 : 1]));
            cstart5 = cstart5 + 2;
            i6 = i6 + 1;
        }
    }
    // Locals:
    // i1 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // cstart0 : Int[nonnull] (persists=true)
    // count2 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        cstart0 = 0;
        i1 = 0;
        count2 = 5;
        loop (i1 < count2) {
            cstart3 = cstart0;
            cstart4 = cstart3 + 1;
            consume(Tuple[Int[nonnull], Int[nonnull]],
            (buf[cstart3 : 1], buf[cstart4 : 1]));
            cstart0 = cstart0 + 2;
            i1 = i1 + 1;
        }
    } |}]

let%expect_test "hash-idx" =
  run_test
    "ATuple([AList(r1, AScalar(r1.f)) as f, AHashIdx(dedup(select([r1.f], r1)) as \
     k, ascalar(k.f+1), f.f)], cross)" ;
  [%expect
    {|
    (TupleT
     (((ListT
        ((IntT ((range (Interval 1 3)) (nullable false)))
         ((count (Interval 5 5)))))
       (HashIdxT
        ((IntT ((range (Interval 1 3)) (nullable false)))
         (IntT ((range (Interval 2 4)) (nullable false)))
         ((key_count (Interval 3 3)) (value_count (Interval 1 1))))))
      ((count (Interval 5 5)))))
    // Locals:
    // cstart8 : Int[nonnull] (persists=true)
    // count12 : Int[nonnull] (persists=true)
    // vstart14 : Int[nonnull] (persists=false)
    // kstart13 : Int[nonnull] (persists=false)
    // i11 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // key15 : Tuple[Int[nonnull]] (persists=false)
    fun printer () : Void {
        cstart8 = 4;
        cstart9 = cstart8 + 5;
        cstart10 = cstart8;
        i11 = 0;
        count12 = 5;
        loop (i11 < count12) {
            if (buf[cstart10 : 1] * 8 < 0 || buf[cstart9 + 4 + 0 + 0 : 8] - 1 <
                buf[cstart10 : 1] * 8 || buf[cstart9 + 4 + 0 + 0 + 8 +
                buf[cstart10 : 1] * 8 : 8] == 0) {

            } else {
                 kstart13 = buf[cstart9 + 4 + 0 + 0 + 8 + buf[cstart10 : 1] * 8 :
                 8];
                 key15 = (buf[kstart13 : 1]);
                 vstart14 = buf[cstart9 + 4 + 0 + 0 + 8 + buf[cstart10 : 1] * 8 :
                 8] + 1;
                 if (true && key15[0] == buf[cstart10 : 1]) {
                     print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull]],
                     (buf[cstart10 : 1], key15[0], buf[vstart14 : 1]));
                 } else {

                 }
            }
            cstart10 = cstart10 + 1;
            i11 = i11 + 1;
        }
    }
    // Locals:
    // i3 : Int[nonnull] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // kstart5 : Int[nonnull] (persists=false)
    // vstart6 : Int[nonnull] (persists=false)
    // cstart0 : Int[nonnull] (persists=true)
    // count4 : Int[nonnull] (persists=true)
    // key7 : Tuple[Int[nonnull]] (persists=false)
    fun consumer () : Void {
        cstart0 = 4;
        cstart1 = cstart0 + 5;
        cstart2 = cstart0;
        i3 = 0;
        count4 = 5;
        loop (i3 < count4) {
            if (buf[cstart2 : 1] * 8 < 0 || buf[cstart1 + 4 + 0 + 0 : 8] - 1 <
                buf[cstart2 : 1] * 8 || buf[cstart1 + 4 + 0 + 0 + 8 +
                buf[cstart2 : 1] * 8 : 8] == 0) {

            } else {
                 kstart5 = buf[cstart1 + 4 + 0 + 0 + 8 + buf[cstart2 : 1] * 8 :
                 8];
                 key7 = (buf[kstart5 : 1]);
                 vstart6 = buf[cstart1 + 4 + 0 + 0 + 8 + buf[cstart2 : 1] * 8 :
                 8] + 1;
                 if (true && key7[0] == buf[cstart2 : 1]) {
                     consume(Tuple[Int[nonnull], Int[nonnull], Int[nonnull]],
                     (buf[cstart2 : 1], key7[0], buf[vstart6 : 1]));
                 } else {

                 }
            }
            cstart2 = cstart2 + 1;
            i3 = i3 + 1;
        }
    } |}]

let%expect_test "ordered-idx" =
  run_test
    "ATuple([AList(r1, AScalar(r1.f)) as f, AOrderedIdx(dedup(select([r1.f], r1)) \
     as k, ascalar(k.f+1), f.f, f.f+1)], cross)" ;
  [%expect
    {|
    (TupleT
     (((ListT
        ((IntT ((range (Interval 1 3)) (nullable false)))
         ((count (Interval 5 5)))))
       (OrderedIdxT
        ((IntT ((range (Interval 1 3)) (nullable false)))
         (IntT ((range (Interval 2 4)) (nullable false))) ((count Top)))))
      ((count Top))))
    // Locals:
    // i18 : Int[nonnull] (persists=true)
    // vstart21 : Int[nonnull] (persists=true)
    // low23 : Int[nonnull] (persists=true)
    // mid25 : Int[nonnull] (persists=true)
    // key22 : Tuple[Int[nonnull]] (persists=true)
    // cstart16 : Int[nonnull] (persists=true)
    // key28 : Tuple[Int[nonnull]] (persists=true)
    // key27 : Tuple[Int[nonnull]] (persists=false)
    // cstart17 : Int[nonnull] (persists=true)
    // key26 : Tuple[Int[nonnull]] (persists=false)
    // high24 : Int[nonnull] (persists=true)
    // cstart15 : Int[nonnull] (persists=true)
    // key29 : Tuple[Int[nonnull]] (persists=false)
    // count19 : Int[nonnull] (persists=true)
    // kstart20 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart15 = 4;
        cstart16 = cstart15 + 5;
        cstart17 = cstart15;
        i18 = 0;
        count19 = 5;
        loop (i18 < count19) {
            low23 = 0;
            high24 = buf[cstart16 + 4 : 8] / 9;
            loop (low23 < high24) {
                mid25 = low23 + high24 / 2;
                kstart20 = cstart16 + 4 + 8 + mid25 * 9;
                key26 = (buf[kstart20 : 1]);
                if (key26[0] < buf[cstart17 : 1]) {
                    low23 = mid25 + 1;
                } else {
                     high24 = mid25;
                }
            }
            if (low23 < buf[cstart16 + 4 : 8] / 9) {
                kstart20 = cstart16 + 4 + 8 + low23 * 9;
                key27 = (buf[kstart20 : 1]);
                key28 = key27;
                loop (key28[0] < buf[cstart17 : 1] + 1 && low23 < buf[cstart16 +
                      4 : 8] / 9) {
                    vstart21 = buf[cstart16 + 4 + 8 + low23 * 9 + 1 : 8];
                    key22 = key28;
                    print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull]],
                    (buf[cstart17 : 1], key22[0], buf[vstart21 : 1]));
                    low23 = low23 + 1;
                    kstart20 = cstart16 + 4 + 8 + low23 * 9;
                    key29 = (buf[kstart20 : 1]);
                    key28 = key29;
                }
            } else {

            }
            cstart17 = cstart17 + 1;
            i18 = i18 + 1;
        }
    }
    // Locals:
    // key13 : Tuple[Int[nonnull]] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    // high9 : Int[nonnull] (persists=true)
    // mid10 : Int[nonnull] (persists=true)
    // key11 : Tuple[Int[nonnull]] (persists=false)
    // count4 : Int[nonnull] (persists=true)
    // low8 : Int[nonnull] (persists=true)
    // key14 : Tuple[Int[nonnull]] (persists=false)
    // i3 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // kstart5 : Int[nonnull] (persists=true)
    // vstart6 : Int[nonnull] (persists=true)
    // key12 : Tuple[Int[nonnull]] (persists=false)
    // cstart0 : Int[nonnull] (persists=true)
    // key7 : Tuple[Int[nonnull]] (persists=true)
    fun consumer () : Void {
        cstart0 = 4;
        cstart1 = cstart0 + 5;
        cstart2 = cstart0;
        i3 = 0;
        count4 = 5;
        loop (i3 < count4) {
            low8 = 0;
            high9 = buf[cstart1 + 4 : 8] / 9;
            loop (low8 < high9) {
                mid10 = low8 + high9 / 2;
                kstart5 = cstart1 + 4 + 8 + mid10 * 9;
                key11 = (buf[kstart5 : 1]);
                if (key11[0] < buf[cstart2 : 1]) {
                    low8 = mid10 + 1;
                } else {
                     high9 = mid10;
                }
            }
            if (low8 < buf[cstart1 + 4 : 8] / 9) {
                kstart5 = cstart1 + 4 + 8 + low8 * 9;
                key12 = (buf[kstart5 : 1]);
                key13 = key12;
                loop (key13[0] < buf[cstart2 : 1] + 1 && low8 < buf[cstart1 + 4 :
                      8] / 9) {
                    vstart6 = buf[cstart1 + 4 + 8 + low8 * 9 + 1 : 8];
                    key7 = key13;
                    consume(Tuple[Int[nonnull], Int[nonnull], Int[nonnull]],
                    (buf[cstart2 : 1], key7[0], buf[vstart6 : 1]));
                    low8 = low8 + 1;
                    kstart5 = cstart1 + 4 + 8 + low8 * 9;
                    key14 = (buf[kstart5 : 1]);
                    key13 = key14;
                }
            } else {

            }
            cstart2 = cstart2 + 1;
            i3 = i3 + 1;
        }
    } |}]

let%expect_test "ordered-idx-date" =
  run_test ~print_code:false
    {|AOrderedIdx(dedup(select([r_date.f as k], r_date)), ascalar(k), date("2018-01-01"), date("2018-01-01"))|} ;
  [%expect
    {|
    (OrderedIdxT
     ((DateT ((range (Interval 17136 17775)) (nullable false)))
      (DateT ((range (Interval 17136 17775)) (nullable false))) ((count Top)))) |}]

let%expect_test "example-1" =
  run_test ~params:Demomatch.example_params
    {|
filter(lc.id = id_c && lp.id = id_p,
alist(filter(succ > counter + 1, log) as lp,
atuple([ascalar(lp.id), ascalar(lp.counter),
alist(filter(lp.counter < log.counter &&
log.counter < lp.succ, log) as lc,
atuple([ascalar(lc.id), ascalar(lc.counter)], cross))], cross)))
|} ;
  [%expect
    {|
    [WARNING] Cross-stage shadowing of lp.counter.
    (FuncT
     (((ListT
        ((TupleT
          (((IntT ((range (Interval 1 1)) (nullable false)))
            (IntT ((range (Interval 1 4)) (nullable false)))
            (ListT
             ((TupleT
               (((IntT ((range (Interval 2 3)) (nullable false)))
                 (IntT ((range (Interval 2 5)) (nullable false))))
                ((count (Interval 1 1)))))
              ((count (Interval 1 2))))))
           ((count (Interval 1 2)))))
         ((count (Interval 2 2))))))
      Child_sum))
    // Locals:
    // i18 : Int[nonnull] (persists=true)
    // cstart21 : Int[nonnull] (persists=true)
    // cstart16 : Int[nonnull] (persists=true)
    // count13 : Int[nonnull] (persists=true)
    // cstart17 : Int[nonnull] (persists=true)
    // cstart15 : Int[nonnull] (persists=true)
    // cstart11 : Int[nonnull] (persists=true)
    // i12 : Int[nonnull] (persists=true)
    // cstart14 : Int[nonnull] (persists=true)
    // count19 : Int[nonnull] (persists=true)
    // cstart20 : Int[nonnull] (persists=true)
    fun printer () : Void {
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
                    print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                    Int[nonnull]],
                    (buf[cstart14 : 1], buf[cstart15 : 1], buf[cstart20 : 1],
                     buf[cstart21 : 1]));
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
    // i1 : Int[nonnull] (persists=true)
    // count8 : Int[nonnull] (persists=true)
    // i7 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // count2 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // cstart0 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    fun consumer () : Void {
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
                    consume(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                    Int[nonnull]],
                    (buf[cstart3 : 1], buf[cstart4 : 1], buf[cstart9 : 1],
                     buf[cstart10 : 1]));
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
  run_test ~params:Demomatch.example_params
    {|
ahashidx(dedup(select([lp.id as lp_k, lc.id as lc_k], 
      join(true, log as lp, log as lc))),
  alist(select([lp.counter, lc.counter], 
    join(lp.counter < lc.counter && 
         lc.counter < lp.succ, 
      filter(log.id = lp_k, log) as lp, 
      filter(log.id = lc_k, log) as lc)),
    atuple([ascalar(lp.counter), ascalar(lc.counter)], cross)),
  (id_p, id_c))
|} ;
  [%expect
    {|
    (HashIdxT
     ((TupleT
       (((IntT ((range (Interval 1 1)) (nullable false)))
         (IntT ((range (Interval 2 3)) (nullable false))))
        ((count (Interval 1 1)))))
      (ListT
       ((TupleT
         (((IntT ((range (Interval 1 4)) (nullable false)))
           (IntT ((range (Interval 2 5)) (nullable false))))
          ((count (Interval 1 1)))))
        ((count (Interval 1 2)))))
      ((key_count (Interval 9 9)) (value_count (Interval 1 2)))))
    // Locals:
    // kstart10 : Int[nonnull] (persists=false)
    // cstart18 : Int[nonnull] (persists=true)
    // i16 : Int[nonnull] (persists=true)
    // vstart11 : Int[nonnull] (persists=false)
    // cstart15 : Int[nonnull] (persists=true)
    // cstart13 : Int[nonnull] (persists=true)
    // count17 : Int[nonnull] (persists=true)
    // key12 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // cstart14 : Int[nonnull] (persists=true)
    // cstart19 : Int[nonnull] (persists=true)
    fun printer () : Void {
        if (<tuplehash> * 8 < 0 || buf[12 + buf[4 : 8] : 8] - 1 < <tuplehash> *
            8 || buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] == 0) {

        } else {
             kstart10 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8];
             cstart13 = kstart10;
             cstart14 = cstart13 + 1;
             key12 = (buf[cstart13 : 1], buf[cstart14 : 1]);
             vstart11 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] + 2;
             if (true && key12[0] == id_p && key12[1] == id_c) {
                 cstart15 = vstart11 + 1 + 1;
                 i16 = 0;
                 count17 = buf[vstart11 : 1];
                 loop (i16 < count17) {
                     cstart18 = cstart15;
                     cstart19 = cstart18 + 1;
                     print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                     Int[nonnull]],
                     (key12[0], key12[1], buf[cstart18 : 1], buf[cstart19 : 1]));
                     cstart15 = cstart15 + 2;
                     i16 = i16 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // cstart9 : Int[nonnull] (persists=true)
    // kstart0 : Int[nonnull] (persists=false)
    // cstart8 : Int[nonnull] (persists=true)
    // vstart1 : Int[nonnull] (persists=false)
    // key2 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // cstart4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // count7 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    // i6 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        if (<tuplehash> * 8 < 0 || buf[12 + buf[4 : 8] : 8] - 1 < <tuplehash> *
            8 || buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] == 0) {

        } else {
             kstart0 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8];
             cstart3 = kstart0;
             cstart4 = cstart3 + 1;
             key2 = (buf[cstart3 : 1], buf[cstart4 : 1]);
             vstart1 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] + 2;
             if (true && key2[0] == id_p && key2[1] == id_c) {
                 cstart5 = vstart1 + 1 + 1;
                 i6 = 0;
                 count7 = buf[vstart1 : 1];
                 loop (i6 < count7) {
                     cstart8 = cstart5;
                     cstart9 = cstart8 + 1;
                     consume(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                     Int[nonnull]],
                     (key2[0], key2[1], buf[cstart8 : 1], buf[cstart9 : 1]));
                     cstart5 = cstart5 + 2;
                     i6 = i6 + 1;
                 }
             } else {

             }
        }
    } |}]

let%expect_test "example-3" =
  run_test ~params:Demomatch.example_params
    {|
select([lp.counter, lc.counter],
  atuple([ahashidx(dedup(select([id as k1], log)), 
    alist(select([counter, succ], 
        filter(k1 = id && counter < succ, log)), 
      atuple([ascalar(counter), ascalar(succ)], cross)), 
    id_p) as lp,
  filter(lc.id = id_c,
    aorderedidx(select([log.counter as k2], log), 
      alist(filter(log.counter = k2, log),
        atuple([ascalar(log.id), ascalar(log.counter)], cross)), 
      lp.counter, lp.succ) as lc)], cross))
|} ;
  [%expect
    {|
    (FuncT
     (((TupleT
        (((HashIdxT
           ((IntT ((range (Interval 1 3)) (nullable false)))
            (ListT
             ((TupleT
               (((IntT ((range (Interval 1 5)) (nullable false)))
                 (IntT ((range (Interval 3 6)) (nullable false))))
                ((count (Interval 1 1)))))
              ((count (Interval 1 2)))))
            ((key_count (Interval 3 3)) (value_count (Interval 1 2)))))
          (FuncT
           (((OrderedIdxT
              ((IntT ((range (Interval 1 5)) (nullable false)))
               (ListT
                ((TupleT
                  (((IntT ((range (Interval 1 3)) (nullable false)))
                    (IntT ((range (Interval 1 5)) (nullable false))))
                   ((count (Interval 1 1)))))
                 ((count (Interval 1 1)))))
               ((count Top)))))
            Child_sum)))
         ((count Top)))))
      (Width 2)))
    // Locals:
    // kstart35 : Int[nonnull] (persists=true)
    // vstart28 : Int[nonnull] (persists=false)
    // cstart25 : Int[nonnull] (persists=true)
    // cstart47 : Int[nonnull] (persists=true)
    // i45 : Int[nonnull] (persists=true)
    // cstart44 : Int[nonnull] (persists=true)
    // cstart34 : Int[nonnull] (persists=true)
    // key42 : Tuple[Int[nonnull]] (persists=false)
    // cstart48 : Int[nonnull] (persists=true)
    // key29 : Tuple[Int[nonnull]] (persists=false)
    // i31 : Int[nonnull] (persists=true)
    // key41 : Tuple[Int[nonnull]] (persists=false)
    // key43 : Tuple[Int[nonnull]] (persists=true)
    // key37 : Tuple[Int[nonnull]] (persists=true)
    // cstart30 : Int[nonnull] (persists=true)
    // cstart33 : Int[nonnull] (persists=true)
    // low38 : Int[nonnull] (persists=true)
    // count32 : Int[nonnull] (persists=true)
    // vstart36 : Int[nonnull] (persists=true)
    // key49 : Tuple[Int[nonnull]] (persists=false)
    // count46 : Int[nonnull] (persists=true)
    // mid40 : Int[nonnull] (persists=true)
    // high39 : Int[nonnull] (persists=true)
    // kstart27 : Int[nonnull] (persists=false)
    // cstart26 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart25 = 4;
        cstart26 = cstart25 + buf[cstart25 : 4];
        if (id_p * 8 < 0 || buf[cstart25 + 4 + 0 + 0 : 8] - 1 < id_p * 8 ||
            buf[cstart25 + 4 + 0 + 0 + 8 + id_p * 8 : 8] == 0) {

        } else {
             kstart27 = buf[cstart25 + 4 + 0 + 0 + 8 + id_p * 8 : 8];
             key29 = (buf[kstart27 : 1]);
             vstart28 = buf[cstart25 + 4 + 0 + 0 + 8 + id_p * 8 : 8] + 1;
             if (true && key29[0] == id_p) {
                 cstart30 = vstart28 + 1 + 1;
                 i31 = 0;
                 count32 = buf[vstart28 : 1];
                 loop (i31 < count32) {
                     cstart33 = cstart30;
                     cstart34 = cstart33 + 1;
                     low38 = 0;
                     high39 = buf[cstart26 + 4 : 8] / 9;
                     loop (low38 < high39) {
                         mid40 = low38 + high39 / 2;
                         kstart35 = cstart26 + 4 + 8 + mid40 * 9;
                         key41 = (buf[kstart35 : 1]);
                         if (key41[0] < buf[cstart33 : 1]) {
                             low38 = mid40 + 1;
                         } else {
                              high39 = mid40;
                         }
                     }
                     if (low38 < buf[cstart26 + 4 : 8] / 9) {
                         kstart35 = cstart26 + 4 + 8 + low38 * 9;
                         key42 = (buf[kstart35 : 1]);
                         key43 = key42;
                         loop (key43[0] < buf[cstart34 : 1] && low38 <
                               buf[cstart26 + 4 : 8] / 9) {
                             vstart36 = buf[cstart26 + 4 + 8 + low38 * 9 + 1 :
                             8];
                             key37 = key43;
                             cstart44 = vstart36;
                             i45 = 0;
                             count46 = 1;
                             loop (i45 < count46) {
                                 cstart47 = cstart44;
                                 cstart48 = cstart47 + 1;
                                 if (buf[cstart47 : 1] == id_c) {
                                     print(Tuple[Int[nonnull], Int[nonnull]],
                                     (buf[cstart33 : 1], buf[cstart48 : 1]));
                                 } else {

                                 }
                                 cstart44 = cstart44 + 2;
                                 i45 = i45 + 1;
                             }
                             low38 = low38 + 1;
                             kstart35 = cstart26 + 4 + 8 + low38 * 9;
                             key49 = (buf[kstart35 : 1]);
                             key43 = key49;
                         }
                     } else {

                     }
                     cstart30 = cstart30 + 2;
                     i31 = i31 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // kstart10 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // key17 : Tuple[Int[nonnull]] (persists=false)
    // cstart22 : Int[nonnull] (persists=true)
    // cstart8 : Int[nonnull] (persists=true)
    // low13 : Int[nonnull] (persists=true)
    // key4 : Tuple[Int[nonnull]] (persists=false)
    // key16 : Tuple[Int[nonnull]] (persists=false)
    // key24 : Tuple[Int[nonnull]] (persists=false)
    // cstart23 : Int[nonnull] (persists=true)
    // cstart0 : Int[nonnull] (persists=true)
    // cstart19 : Int[nonnull] (persists=true)
    // high14 : Int[nonnull] (persists=true)
    // mid15 : Int[nonnull] (persists=true)
    // vstart11 : Int[nonnull] (persists=true)
    // i20 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // count21 : Int[nonnull] (persists=true)
    // vstart3 : Int[nonnull] (persists=false)
    // kstart2 : Int[nonnull] (persists=false)
    // key18 : Tuple[Int[nonnull]] (persists=true)
    // count7 : Int[nonnull] (persists=true)
    // key12 : Tuple[Int[nonnull]] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    // i6 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        cstart0 = 4;
        cstart1 = cstart0 + buf[cstart0 : 4];
        if (id_p * 8 < 0 || buf[cstart0 + 4 + 0 + 0 : 8] - 1 < id_p * 8 ||
            buf[cstart0 + 4 + 0 + 0 + 8 + id_p * 8 : 8] == 0) {

        } else {
             kstart2 = buf[cstart0 + 4 + 0 + 0 + 8 + id_p * 8 : 8];
             key4 = (buf[kstart2 : 1]);
             vstart3 = buf[cstart0 + 4 + 0 + 0 + 8 + id_p * 8 : 8] + 1;
             if (true && key4[0] == id_p) {
                 cstart5 = vstart3 + 1 + 1;
                 i6 = 0;
                 count7 = buf[vstart3 : 1];
                 loop (i6 < count7) {
                     cstart8 = cstart5;
                     cstart9 = cstart8 + 1;
                     low13 = 0;
                     high14 = buf[cstart1 + 4 : 8] / 9;
                     loop (low13 < high14) {
                         mid15 = low13 + high14 / 2;
                         kstart10 = cstart1 + 4 + 8 + mid15 * 9;
                         key16 = (buf[kstart10 : 1]);
                         if (key16[0] < buf[cstart8 : 1]) {
                             low13 = mid15 + 1;
                         } else {
                              high14 = mid15;
                         }
                     }
                     if (low13 < buf[cstart1 + 4 : 8] / 9) {
                         kstart10 = cstart1 + 4 + 8 + low13 * 9;
                         key17 = (buf[kstart10 : 1]);
                         key18 = key17;
                         loop (key18[0] < buf[cstart9 : 1] && low13 <
                               buf[cstart1 + 4 : 8] / 9) {
                             vstart11 = buf[cstart1 + 4 + 8 + low13 * 9 + 1 : 8];
                             key12 = key18;
                             cstart19 = vstart11;
                             i20 = 0;
                             count21 = 1;
                             loop (i20 < count21) {
                                 cstart22 = cstart19;
                                 cstart23 = cstart22 + 1;
                                 if (buf[cstart22 : 1] == id_c) {
                                     consume(Tuple[Int[nonnull], Int[nonnull]],
                                     (buf[cstart8 : 1], buf[cstart23 : 1]));
                                 } else {

                                 }
                                 cstart19 = cstart19 + 2;
                                 i20 = i20 + 1;
                             }
                             low13 = low13 + 1;
                             kstart10 = cstart1 + 4 + 8 + low13 * 9;
                             key24 = (buf[kstart10 : 1]);
                             key18 = key24;
                         }
                     } else {

                     }
                     cstart5 = cstart5 + 2;
                     i6 = i6 + 1;
                 }
             } else {

             }
        }
    } |}]

let%expect_test "subquery-first" =
  run_test ~params:Demomatch.example_params
    {|
    select([log.id], filter((select([min(l.counter)],
 alist(log as l, ascalar(l.counter))))=log.id, alist(log, ascalar(log.id))))
|} ;
  [%expect
    {|
    (FuncT
     (((FuncT
        (((ListT
           ((IntT ((range (Interval 1 3)) (nullable false)))
            ((count (Interval 5 5))))))
         Child_sum)))
      (Width 1)))
    // Locals:
    // found_tup17 : Bool[nonnull] (persists=false)
    // tup16 : Tuple[Int[nonnull]] (persists=false)
    // count13 : Int[nonnull] (persists=true)
    // min18 : Int[nonnull] (persists=false)
    // first14 : Int[nonnull] (persists=true)
    // i20 : Int[nonnull] (persists=true)
    // cstart11 : Int[nonnull] (persists=true)
    // i12 : Int[nonnull] (persists=true)
    // count21 : Int[nonnull] (persists=true)
    // cstart19 : Int[nonnull] (persists=true)
    fun printer () : Void {
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
                print(Tuple[Int[nonnull]], (buf[cstart11 : 1]));
            } else {

            }
            cstart11 = cstart11 + 1;
            i12 = i12 + 1;
        }
    }
    // Locals:
    // i1 : Int[nonnull] (persists=true)
    // first3 : Int[nonnull] (persists=true)
    // count2 : Int[nonnull] (persists=true)
    // min7 : Int[nonnull] (persists=false)
    // cstart8 : Int[nonnull] (persists=true)
    // count10 : Int[nonnull] (persists=true)
    // tup5 : Tuple[Int[nonnull]] (persists=false)
    // i9 : Int[nonnull] (persists=true)
    // found_tup6 : Bool[nonnull] (persists=false)
    // cstart0 : Int[nonnull] (persists=true)
    fun consumer () : Void {
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
                consume(Tuple[Int[nonnull]], (buf[cstart0 : 1]));
            } else {

            }
            cstart0 = cstart0 + 1;
            i1 = i1 + 1;
        }
    } |}]

let%expect_test "example-3-str" =
  run_test ~params:Demomatch.example_db_params
    {|
select([lp.counter, lc.counter],
  atuple([ahashidx(dedup(select([id as k1], log_str)), 
    alist(select([counter, succ], 
        filter(k1 = id && counter < succ, log_str)), 
      atuple([ascalar(counter), ascalar(succ)], cross)), 
    id_p) as lp,
  filter(lc.id = id_c,
    aorderedidx(select([log_str.counter as k2], log_str), 
      alist(filter(log_str.counter = k2, log_str),
        atuple([ascalar(log_str.id), ascalar(log_str.counter)], cross)), 
      lp.counter, lp.succ) as lc)], cross))
|} ;
  [%expect
    {|
    (FuncT
     (((TupleT
        (((HashIdxT
           ((StringT ((nchars (Interval 3 8)) (nullable false)))
            (ListT
             ((TupleT
               (((IntT ((range (Interval 1 5)) (nullable false)))
                 (IntT ((range (Interval 3 6)) (nullable false))))
                ((count (Interval 1 1)))))
              ((count (Interval 1 2)))))
            ((key_count (Interval 3 3)) (value_count (Interval 1 2)))))
          (FuncT
           (((OrderedIdxT
              ((IntT ((range (Interval 1 5)) (nullable false)))
               (ListT
                ((TupleT
                  (((StringT ((nchars (Interval 3 8)) (nullable false)))
                    (IntT ((range (Interval 1 5)) (nullable false))))
                   ((count (Interval 1 1)))))
                 ((count (Interval 1 1)))))
               ((count Top)))))
            Child_sum)))
         ((count Top)))))
      (Width 2)))
    // Locals:
    // kstart35 : Int[nonnull] (persists=true)
    // vstart28 : Int[nonnull] (persists=false)
    // cstart25 : Int[nonnull] (persists=true)
    // cstart47 : Int[nonnull] (persists=true)
    // i45 : Int[nonnull] (persists=true)
    // cstart44 : Int[nonnull] (persists=true)
    // cstart34 : Int[nonnull] (persists=true)
    // key42 : Tuple[Int[nonnull]] (persists=false)
    // cstart48 : Int[nonnull] (persists=true)
    // key29 : Tuple[String[nonnull]] (persists=false)
    // i31 : Int[nonnull] (persists=true)
    // key41 : Tuple[Int[nonnull]] (persists=false)
    // key43 : Tuple[Int[nonnull]] (persists=true)
    // key37 : Tuple[Int[nonnull]] (persists=true)
    // cstart30 : Int[nonnull] (persists=true)
    // cstart33 : Int[nonnull] (persists=true)
    // low38 : Int[nonnull] (persists=true)
    // count32 : Int[nonnull] (persists=true)
    // vstart36 : Int[nonnull] (persists=true)
    // key49 : Tuple[Int[nonnull]] (persists=false)
    // count46 : Int[nonnull] (persists=true)
    // mid40 : Int[nonnull] (persists=true)
    // high39 : Int[nonnull] (persists=true)
    // kstart27 : Int[nonnull] (persists=false)
    // cstart26 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart25 = 4;
        cstart26 = cstart25 + buf[cstart25 : 4];
        if (hash(cstart25 + 4 + 8, id_p) * 8 < 0 || buf[cstart25 + 4 + 8 +
            buf[cstart25 + 4 : 8] : 8] - 1 < hash(cstart25 + 4 + 8, id_p) * 8 ||
            buf[cstart25 + 4 + 8 + buf[cstart25 + 4 : 8] + 8 + hash(cstart25 +
            4 + 8, id_p) * 8 : 8] == 0) {

        } else {
             kstart27 = buf[cstart25 + 4 + 8 + buf[cstart25 + 4 : 8] + 8 +
             hash(cstart25 + 4 + 8, id_p) * 8 : 8];
             key29 = (load_str(kstart27 + 1, buf[kstart27 : 1]));
             vstart28 = buf[cstart25 + 4 + 8 + buf[cstart25 + 4 : 8] + 8 +
             hash(cstart25 + 4 + 8, id_p) * 8 : 8] + 1 + buf[buf[cstart25 + 4 +
             8 + buf[cstart25 + 4 : 8] + 8 + hash(cstart25 + 4 + 8, id_p) * 8 :
             8] : 1];
             if (true && key29[0] == id_p) {
                 cstart30 = vstart28 + 1 + 1;
                 i31 = 0;
                 count32 = buf[vstart28 : 1];
                 loop (i31 < count32) {
                     cstart33 = cstart30;
                     cstart34 = cstart33 + 1;
                     low38 = 0;
                     high39 = buf[cstart26 + 4 : 8] / 9;
                     loop (low38 < high39) {
                         mid40 = low38 + high39 / 2;
                         kstart35 = cstart26 + 4 + 8 + mid40 * 9;
                         key41 = (buf[kstart35 : 1]);
                         if (key41[0] < buf[cstart33 : 1]) {
                             low38 = mid40 + 1;
                         } else {
                              high39 = mid40;
                         }
                     }
                     if (low38 < buf[cstart26 + 4 : 8] / 9) {
                         kstart35 = cstart26 + 4 + 8 + low38 * 9;
                         key42 = (buf[kstart35 : 1]);
                         key43 = key42;
                         loop (key43[0] < buf[cstart34 : 1] && low38 <
                               buf[cstart26 + 4 : 8] / 9) {
                             vstart36 = buf[cstart26 + 4 + 8 + low38 * 9 + 1 :
                             8];
                             key37 = key43;
                             cstart44 = vstart36 + 1;
                             i45 = 0;
                             count46 = 1;
                             loop (i45 < count46) {
                                 cstart47 = cstart44 + 1;
                                 cstart48 = cstart47 + 1 + buf[cstart47 : 1];
                                 if (load_str(cstart47 + 1, buf[cstart47 : 1]) ==
                                     id_c) {
                                     print(Tuple[Int[nonnull], Int[nonnull]],
                                     (buf[cstart33 : 1], buf[cstart48 : 1]));
                                 } else {

                                 }
                                 cstart44 = cstart44 + buf[cstart44 : 1];
                                 i45 = i45 + 1;
                             }
                             low38 = low38 + 1;
                             kstart35 = cstart26 + 4 + 8 + low38 * 9;
                             key49 = (buf[kstart35 : 1]);
                             key43 = key49;
                         }
                     } else {

                     }
                     cstart30 = cstart30 + 2;
                     i31 = i31 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // kstart10 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // key17 : Tuple[Int[nonnull]] (persists=false)
    // cstart22 : Int[nonnull] (persists=true)
    // cstart8 : Int[nonnull] (persists=true)
    // low13 : Int[nonnull] (persists=true)
    // key4 : Tuple[String[nonnull]] (persists=false)
    // key16 : Tuple[Int[nonnull]] (persists=false)
    // key24 : Tuple[Int[nonnull]] (persists=false)
    // cstart23 : Int[nonnull] (persists=true)
    // cstart0 : Int[nonnull] (persists=true)
    // cstart19 : Int[nonnull] (persists=true)
    // high14 : Int[nonnull] (persists=true)
    // mid15 : Int[nonnull] (persists=true)
    // vstart11 : Int[nonnull] (persists=true)
    // i20 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // count21 : Int[nonnull] (persists=true)
    // vstart3 : Int[nonnull] (persists=false)
    // kstart2 : Int[nonnull] (persists=false)
    // key18 : Tuple[Int[nonnull]] (persists=true)
    // count7 : Int[nonnull] (persists=true)
    // key12 : Tuple[Int[nonnull]] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    // i6 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        cstart0 = 4;
        cstart1 = cstart0 + buf[cstart0 : 4];
        if (hash(cstart0 + 4 + 8, id_p) * 8 < 0 || buf[cstart0 + 4 + 8 +
            buf[cstart0 + 4 : 8] : 8] - 1 < hash(cstart0 + 4 + 8, id_p) * 8 ||
            buf[cstart0 + 4 + 8 + buf[cstart0 + 4 : 8] + 8 + hash(cstart0 + 4 +
            8, id_p) * 8 : 8] == 0) {

        } else {
             kstart2 = buf[cstart0 + 4 + 8 + buf[cstart0 + 4 : 8] + 8 +
             hash(cstart0 + 4 + 8, id_p) * 8 : 8];
             key4 = (load_str(kstart2 + 1, buf[kstart2 : 1]));
             vstart3 = buf[cstart0 + 4 + 8 + buf[cstart0 + 4 : 8] + 8 +
             hash(cstart0 + 4 + 8, id_p) * 8 : 8] + 1 + buf[buf[cstart0 + 4 + 8 +
             buf[cstart0 + 4 : 8] + 8 + hash(cstart0 + 4 + 8, id_p) * 8 : 8] :
             1];
             if (true && key4[0] == id_p) {
                 cstart5 = vstart3 + 1 + 1;
                 i6 = 0;
                 count7 = buf[vstart3 : 1];
                 loop (i6 < count7) {
                     cstart8 = cstart5;
                     cstart9 = cstart8 + 1;
                     low13 = 0;
                     high14 = buf[cstart1 + 4 : 8] / 9;
                     loop (low13 < high14) {
                         mid15 = low13 + high14 / 2;
                         kstart10 = cstart1 + 4 + 8 + mid15 * 9;
                         key16 = (buf[kstart10 : 1]);
                         if (key16[0] < buf[cstart8 : 1]) {
                             low13 = mid15 + 1;
                         } else {
                              high14 = mid15;
                         }
                     }
                     if (low13 < buf[cstart1 + 4 : 8] / 9) {
                         kstart10 = cstart1 + 4 + 8 + low13 * 9;
                         key17 = (buf[kstart10 : 1]);
                         key18 = key17;
                         loop (key18[0] < buf[cstart9 : 1] && low13 <
                               buf[cstart1 + 4 : 8] / 9) {
                             vstart11 = buf[cstart1 + 4 + 8 + low13 * 9 + 1 : 8];
                             key12 = key18;
                             cstart19 = vstart11 + 1;
                             i20 = 0;
                             count21 = 1;
                             loop (i20 < count21) {
                                 cstart22 = cstart19 + 1;
                                 cstart23 = cstart22 + 1 + buf[cstart22 : 1];
                                 if (load_str(cstart22 + 1, buf[cstart22 : 1]) ==
                                     id_c) {
                                     consume(Tuple[Int[nonnull], Int[nonnull]],
                                     (buf[cstart8 : 1], buf[cstart23 : 1]));
                                 } else {

                                 }
                                 cstart19 = cstart19 + buf[cstart19 : 1];
                                 i20 = i20 + 1;
                             }
                             low13 = low13 + 1;
                             kstart10 = cstart1 + 4 + 8 + low13 * 9;
                             key24 = (buf[kstart10 : 1]);
                             key18 = key24;
                         }
                     } else {

                     }
                     cstart5 = cstart5 + 2;
                     i6 = i6 + 1;
                 }
             } else {

             }
        }
    } |}]
