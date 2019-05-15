open! Core
open Test_util

let run_test ?(params = []) ?(print_code = true) layout_str =
  let (module M), (module S), (module I), (module C) =
    Setup.make_modules ~code_only:true ()
  in
  try
    let param_names = List.map params ~f:(fun (n, _) -> n) in
    let sparams = Set.of_list (module Name) param_names in
    let layout = M.load_string ~params:sparams layout_str in
    M.annotate_type layout ;
    let type_ = Meta.(find_exn layout type_) in
    print_endline (Sexp.to_string_hum ([%sexp_of: Type.t] type_)) ;
    let ir = I.irgen ~params:param_names ~data_fn:"/tmp/buf" layout in
    if print_code then I.pp Caml.Format.std_formatter ir
  with exn ->
    Backtrace.(
      elide := false ;
      Exn.most_recent () |> to_string |> print_endline) ;
    Exn.(to_string exn |> print_endline)

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
    [ERROR] Tried to get schema of unnamed predicate 1.
    [ERROR] Tried to get schema of unnamed predicate 2.
    [ERROR] Tried to get schema of unnamed predicate 1.
    [ERROR] Tried to get schema of unnamed predicate 2.
    // Locals:
    // start : Int[nonnull] (persists=true)
    fun zt_3 (start) : Tuple[Int[nonnull]] {
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
    // start : Int[nonnull] (persists=true)
    fun zt_0 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // i8 : Int[nonnull] (persists=true)
    // tup7 : Tuple[Int[nonnull]] (persists=true)
    // count9 : Int[nonnull] (persists=true)
    // tup6 : Tuple[Int[nonnull]] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart5 = 0;
        cstart5 = 0;
        init zt_2(cstart5);
        cstart5 = cstart5 + 1;
        init zt_3(cstart5);
        cstart5 = cstart5 + 1;
        i8 = 0;
        count9 = 1;
        loop (i8 < count9) {
            tup6 = next(zt_2);
            tup7 = next(zt_3);
            print(Tuple[Int[nonnull], Int[nonnull]], (tup6[0], tup7[0]));
            i8 = i8 + 1;
        }
    }
    // Locals:
    // i3 : Int[nonnull] (persists=true)
    // tup2 : Tuple[Int[nonnull]] (persists=true)
    // tup1 : Tuple[Int[nonnull]] (persists=true)
    // cstart0 : Int[nonnull] (persists=true)
    // count4 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        cstart0 = 0;
        cstart0 = 0;
        init zt_0(cstart0);
        cstart0 = cstart0 + 1;
        init zt_1(cstart0);
        cstart0 = cstart0 + 1;
        i3 = 0;
        count4 = 1;
        loop (i3 < count4) {
            tup1 = next(zt_0);
            tup2 = next(zt_1);
            consume(Tuple[Int[nonnull], Int[nonnull]], (tup1[0], tup2[0]));
            i3 = i3 + 1;
        }
    } |}]

let%expect_test "sum-complex" =
  run_test sum_complex ;
  [%expect
    {|
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
    "Select([sum(f), count()], AList(r1 as k, ATuple([AScalar(k.f), AScalar(k.g - \
     k.f)], cross)))" ;
  [%expect
    {|
    (FuncT
     (((ListT
        ((TupleT
          (((IntT ((range (Interval 1 3)) (nullable false)))
            (IntT ((range (Interval -1 2)) (nullable false))))
           ((count (Interval 1 1)))))
         ((count (Interval 5 5))))))
      (Width 2)))
    [ERROR] Tried to get schema of unnamed predicate (k.g - k.f).
    [ERROR] Tried to get schema of unnamed predicate (k.g - k.f).
    [ERROR] Tried to get schema of unnamed predicate (k.g - k.f).
    [ERROR] Tried to get schema of unnamed predicate (k.g - k.f).
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
  run_test "AList(r1 as k, ATuple([AScalar(k.f), AScalar(k.g - k.f)], cross))" ;
  [%expect
    {|
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
    "depjoin(AList(r1 as kl, AScalar(kl.f)) as k, AHashIdx(dedup(select([f], r1)) \
     as kh, ascalar(kh.f + 1), k.f))" ;
  [%expect
    {|
    (FuncT
     (((ListT
        ((IntT ((range (Interval 1 3)) (nullable false)))
         ((count (Interval 5 5)))))
       (HashIdxT
        ((IntT ((range (Interval 1 3)) (nullable false)))
         (IntT ((range (Interval 2 4)) (nullable false)))
         ((key_count (Interval 3 3)) (value_count (Interval 1 1))))))
      Child_sum))
    // Locals:
    // count8 : Int[nonnull] (persists=true)
    // i7 : Int[nonnull] (persists=true)
    // vstart10 : Int[nonnull] (persists=false)
    // key11 : Tuple[Int[nonnull]] (persists=false)
    // cstart6 : Int[nonnull] (persists=true)
    // kstart9 : Int[nonnull] (persists=false)
    fun printer () : Void {
        cstart6 = 4;
        i7 = 0;
        count8 = 5;
        loop (i7 < count8) {
            if (buf[cstart6 : 1] * 8 < 0 || buf[13 : 8] - 1 < buf[cstart6 : 1] *
                8) {

            } else {
                 kstart9 = buf[21 + buf[cstart6 : 1] * 8 : 8] + 21 + buf[13 : 8];
                 key11 = (buf[kstart9 : 1]);
                 vstart10 = buf[21 + buf[cstart6 : 1] * 8 : 8] + 21 + buf[13 :
                 8] + 1;
                 if (true && key11[0] == buf[cstart6 : 1]) {
                     print(Tuple[Int[nonnull], Int[nonnull]],
                     (key11[0], buf[vstart10 : 1]));
                 } else {

                 }
            }
            cstart6 = cstart6 + 1;
            i7 = i7 + 1;
        }
    }
    // Locals:
    // kstart3 : Int[nonnull] (persists=false)
    // key5 : Tuple[Int[nonnull]] (persists=false)
    // i1 : Int[nonnull] (persists=true)
    // vstart4 : Int[nonnull] (persists=false)
    // cstart0 : Int[nonnull] (persists=true)
    // count2 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        cstart0 = 4;
        i1 = 0;
        count2 = 5;
        loop (i1 < count2) {
            if (buf[cstart0 : 1] * 8 < 0 || buf[13 : 8] - 1 < buf[cstart0 : 1] *
                8) {

            } else {
                 kstart3 = buf[21 + buf[cstart0 : 1] * 8 : 8] + 21 + buf[13 : 8];
                 key5 = (buf[kstart3 : 1]);
                 vstart4 = buf[21 + buf[cstart0 : 1] * 8 : 8] + 21 + buf[13 :
                 8] + 1;
                 if (true && key5[0] == buf[cstart0 : 1]) {
                     consume(Tuple[Int[nonnull], Int[nonnull]],
                     (key5[0], buf[vstart4 : 1]));
                 } else {

                 }
            }
            cstart0 = cstart0 + 1;
            i1 = i1 + 1;
        }
    } |}]

let%expect_test "ordered-idx" =
  run_test
    "depjoin(AList(r1 as kl, AScalar(kl.f)) as k, AOrderedIdx(dedup(select([f as \
     kf], r1)) as k1, ascalar(k1.kf+1), k.f, k.f+1))" ;
  [%expect
    {|
    (FuncT
     (((ListT
        ((IntT ((range (Interval 1 3)) (nullable false)))
         ((count (Interval 5 5)))))
       (OrderedIdxT
        ((IntT ((range (Interval 1 3)) (nullable false)))
         (IntT ((range (Interval 2 4)) (nullable false)))
         ((count (Interval 3 3))))))
      Child_sum))
    // Locals:
    // vstart17 : Int[nonnull] (persists=true)
    // key22 : Tuple[Int[nonnull]] (persists=false)
    // i14 : Int[nonnull] (persists=true)
    // key23 : Tuple[Int[nonnull]] (persists=false)
    // high20 : Int[nonnull] (persists=true)
    // mid21 : Int[nonnull] (persists=true)
    // kstart16 : Int[nonnull] (persists=true)
    // key25 : Tuple[Int[nonnull]] (persists=false)
    // count15 : Int[nonnull] (persists=true)
    // cstart13 : Int[nonnull] (persists=true)
    // key24 : Tuple[Int[nonnull]] (persists=true)
    // key18 : Tuple[Int[nonnull]] (persists=true)
    // low19 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart13 = 0;
        i14 = 0;
        count15 = 5;
        loop (i14 < count15) {
            low19 = 0;
            high20 = 6 / 2;
            loop (low19 < high20) {
                mid21 = low19 + high20 / 2;
                kstart16 = 5 + mid21 * 2;
                key22 = (buf[kstart16 : 1]);
                if (key22[0] < buf[cstart13 : 1]) {
                    low19 = mid21 + 1;
                } else {
                     high20 = mid21;
                }
            }
            if (low19 < 6 / 2) {
                kstart16 = 5 + low19 * 2;
                key23 = (buf[kstart16 : 1]);
                key24 = key23;
                loop (key24[0] < buf[cstart13 : 1] + 1 && low19 < 6 / 2) {
                    vstart17 = buf[5 + low19 * 2 + 1 : 1] + 6 + 5;
                    key18 = key24;
                    print(Tuple[Int[nonnull], Int[nonnull]],
                    (key18[0], buf[vstart17 : 1]));
                    low19 = low19 + 1;
                    kstart16 = 5 + low19 * 2;
                    key25 = (buf[kstart16 : 1]);
                    key24 = key25;
                }
            } else {

            }
            cstart13 = cstart13 + 1;
            i14 = i14 + 1;
        }
    }
    // Locals:
    // i1 : Int[nonnull] (persists=true)
    // key10 : Tuple[Int[nonnull]] (persists=false)
    // vstart4 : Int[nonnull] (persists=true)
    // mid8 : Int[nonnull] (persists=true)
    // key9 : Tuple[Int[nonnull]] (persists=false)
    // key11 : Tuple[Int[nonnull]] (persists=true)
    // count2 : Int[nonnull] (persists=true)
    // high7 : Int[nonnull] (persists=true)
    // kstart3 : Int[nonnull] (persists=true)
    // key5 : Tuple[Int[nonnull]] (persists=true)
    // low6 : Int[nonnull] (persists=true)
    // key12 : Tuple[Int[nonnull]] (persists=false)
    // cstart0 : Int[nonnull] (persists=true)
    fun consumer () : Void {
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
                if (key9[0] < buf[cstart0 : 1]) {
                    low6 = mid8 + 1;
                } else {
                     high7 = mid8;
                }
            }
            if (low6 < 6 / 2) {
                kstart3 = 5 + low6 * 2;
                key10 = (buf[kstart3 : 1]);
                key11 = key10;
                loop (key11[0] < buf[cstart0 : 1] + 1 && low6 < 6 / 2) {
                    vstart4 = buf[5 + low6 * 2 + 1 : 1] + 6 + 5;
                    key5 = key11;
                    consume(Tuple[Int[nonnull], Int[nonnull]],
                    (key5[0], buf[vstart4 : 1]));
                    low6 = low6 + 1;
                    kstart3 = 5 + low6 * 2;
                    key12 = (buf[kstart3 : 1]);
                    key11 = key12;
                }
            } else {

            }
            cstart0 = cstart0 + 1;
            i1 = i1 + 1;
        }
    } |}]

let%expect_test "ordered-idx-date" =
  run_test ~print_code:false
    {|AOrderedIdx(dedup(select([f], r_date)) as k, ascalar(k.f as ff), date("2018-01-01"), date("2018-01-01"))|} ;
  [%expect
    {|
    (OrderedIdxT
     ((DateT ((range (Interval 17136 17775)) (nullable false)))
      (DateT ((range (Interval 17136 17775)) (nullable false)))
      ((count (Interval 5 5))))) |}]

let%expect_test "example-1" =
  Demomatch.(run_test ~params:Demomatch.example_params (example1 "log")) ;
  [%expect
    {|
    (FuncT
     (((FuncT
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
         Child_sum)))
      (Width 2)))
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
                    print(Tuple[Int[nonnull], Int[nonnull]],
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
                    consume(Tuple[Int[nonnull], Int[nonnull]],
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
  Demomatch.(run_test ~params:example_params (example2 "log")) ;
  [%expect
    {|
    (FuncT
     (((HashIdxT
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
         ((key_count (Interval 2 2)) (value_count (Interval 1 2))))))
      (Width 2)))
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
        if (<tuplehash> * 8 < 0 || buf[12 + buf[4 : 8] : 8] - 1 < <tuplehash> * 8) {

        } else {
             kstart10 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] + 12 +
             buf[4 : 8] + 8 + buf[12 + buf[4 : 8] : 8];
             cstart13 = kstart10;
             cstart14 = cstart13 + 1;
             key12 = (buf[cstart13 : 1], buf[cstart14 : 1]);
             vstart11 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] + 12 +
             buf[4 : 8] + 8 + buf[12 + buf[4 : 8] : 8] + 2;
             if (true && key12[0] == id_p && key12[1] == id_c) {
                 cstart15 = vstart11 + 1 + 1;
                 i16 = 0;
                 count17 = buf[vstart11 : 1];
                 loop (i16 < count17) {
                     cstart18 = cstart15;
                     cstart19 = cstart18 + 1;
                     print(Tuple[Int[nonnull], Int[nonnull]],
                     (buf[cstart18 : 1], buf[cstart19 : 1]));
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
        if (<tuplehash> * 8 < 0 || buf[12 + buf[4 : 8] : 8] - 1 < <tuplehash> * 8) {

        } else {
             kstart0 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] + 12 +
             buf[4 : 8] + 8 + buf[12 + buf[4 : 8] : 8];
             cstart3 = kstart0;
             cstart4 = cstart3 + 1;
             key2 = (buf[cstart3 : 1], buf[cstart4 : 1]);
             vstart1 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] + 12 +
             buf[4 : 8] + 8 + buf[12 + buf[4 : 8] : 8] + 2;
             if (true && key2[0] == id_p && key2[1] == id_c) {
                 cstart5 = vstart1 + 1 + 1;
                 i6 = 0;
                 count7 = buf[vstart1 : 1];
                 loop (i6 < count7) {
                     cstart8 = cstart5;
                     cstart9 = cstart8 + 1;
                     consume(Tuple[Int[nonnull], Int[nonnull]],
                     (buf[cstart8 : 1], buf[cstart9 : 1]));
                     cstart5 = cstart5 + 2;
                     i6 = i6 + 1;
                 }
             } else {

             }
        }
    } |}]

let%expect_test "example-3" =
  Demomatch.(run_test ~params:example_params (example3 "log")) ;
  [%expect
    {|
    (FuncT
     (((FuncT
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
           (((FuncT
              (((OrderedIdxT
                 ((IntT ((range (Interval 1 5)) (nullable false)))
                  (ListT
                   ((TupleT
                     (((IntT ((range (Interval 1 3)) (nullable false)))
                       (IntT ((range (Interval 1 5)) (nullable false))))
                      ((count (Interval 1 1)))))
                    ((count (Interval 1 1)))))
                  ((count (Interval 5 5))))))
               Child_sum)))
            (Width 2))))
         Child_sum)))
      (Width 2)))
    // Locals:
    // cstart29 : Int[nonnull] (persists=true)
    // cstart44 : Int[nonnull] (persists=true)
    // i27 : Int[nonnull] (persists=true)
    // key25 : Tuple[Int[nonnull]] (persists=false)
    // count42 : Int[nonnull] (persists=true)
    // cstart43 : Int[nonnull] (persists=true)
    // vstart24 : Int[nonnull] (persists=false)
    // kstart23 : Int[nonnull] (persists=false)
    // count28 : Int[nonnull] (persists=true)
    // key38 : Tuple[Int[nonnull]] (persists=false)
    // low34 : Int[nonnull] (persists=true)
    // key37 : Tuple[Int[nonnull]] (persists=false)
    // cstart30 : Int[nonnull] (persists=true)
    // high35 : Int[nonnull] (persists=true)
    // kstart31 : Int[nonnull] (persists=true)
    // key39 : Tuple[Int[nonnull]] (persists=true)
    // i41 : Int[nonnull] (persists=true)
    // key45 : Tuple[Int[nonnull]] (persists=false)
    // mid36 : Int[nonnull] (persists=true)
    // key33 : Tuple[Int[nonnull]] (persists=true)
    // vstart32 : Int[nonnull] (persists=true)
    // cstart40 : Int[nonnull] (persists=true)
    // cstart26 : Int[nonnull] (persists=true)
    fun printer () : Void {
        if (id_p * 8 < 0 || buf[8 : 8] - 1 < id_p * 8) {

        } else {
             kstart23 = buf[16 + id_p * 8 : 8] + 16 + buf[8 : 8];
             key25 = (buf[kstart23 : 1]);
             vstart24 = buf[16 + id_p * 8 : 8] + 16 + buf[8 : 8] + 1;
             if (true && key25[0] == id_p) {
                 cstart26 = vstart24 + 1 + 1;
                 i27 = 0;
                 count28 = buf[vstart24 : 1];
                 loop (i27 < count28) {
                     cstart29 = cstart26;
                     cstart30 = cstart29 + 1;
                     low34 = 0;
                     high35 = 10 / 2;
                     loop (low34 < high35) {
                         mid36 = low34 + high35 / 2;
                         kstart31 = 4 + buf[4 : 4] + mid36 * 2;
                         key37 = (buf[kstart31 : 1]);
                         if (key37[0] < buf[cstart29 : 1]) {
                             low34 = mid36 + 1;
                         } else {
                              high35 = mid36;
                         }
                     }
                     if (low34 < 10 / 2) {
                         kstart31 = 4 + buf[4 : 4] + low34 * 2;
                         key38 = (buf[kstart31 : 1]);
                         key39 = key38;
                         loop (key39[0] < buf[cstart30 : 1] && low34 < 10 / 2) {
                             vstart32 = buf[4 + buf[4 : 4] + low34 * 2 + 1 : 1] +
                             10 + 4 + buf[4 : 4];
                             key33 = key39;
                             cstart40 = vstart32;
                             i41 = 0;
                             count42 = 1;
                             loop (i41 < count42) {
                                 cstart43 = cstart40;
                                 cstart44 = cstart43 + 1;
                                 if (buf[cstart43 : 1] == id_c) {
                                     print(Tuple[Int[nonnull], Int[nonnull]],
                                     (buf[cstart29 : 1], buf[cstart44 : 1]));
                                 } else {

                                 }
                                 cstart40 = cstart40 + 2;
                                 i41 = i41 + 1;
                             }
                             low34 = low34 + 1;
                             kstart31 = 4 + buf[4 : 4] + low34 * 2;
                             key45 = (buf[kstart31 : 1]);
                             key39 = key45;
                         }
                     } else {

                     }
                     cstart26 = cstart26 + 2;
                     i27 = i27 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // i18 : Int[nonnull] (persists=true)
    // key10 : Tuple[Int[nonnull]] (persists=true)
    // key22 : Tuple[Int[nonnull]] (persists=false)
    // cstart21 : Int[nonnull] (persists=true)
    // high12 : Int[nonnull] (persists=true)
    // cstart17 : Int[nonnull] (persists=true)
    // mid13 : Int[nonnull] (persists=true)
    // count5 : Int[nonnull] (persists=true)
    // key14 : Tuple[Int[nonnull]] (persists=false)
    // key16 : Tuple[Int[nonnull]] (persists=true)
    // key2 : Tuple[Int[nonnull]] (persists=false)
    // count19 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart7 : Int[nonnull] (persists=true)
    // vstart9 : Int[nonnull] (persists=true)
    // kstart8 : Int[nonnull] (persists=true)
    // key15 : Tuple[Int[nonnull]] (persists=false)
    // kstart0 : Int[nonnull] (persists=false)
    // vstart1 : Int[nonnull] (persists=false)
    // i4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // low11 : Int[nonnull] (persists=true)
    // cstart20 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        if (id_p * 8 < 0 || buf[8 : 8] - 1 < id_p * 8) {

        } else {
             kstart0 = buf[16 + id_p * 8 : 8] + 16 + buf[8 : 8];
             key2 = (buf[kstart0 : 1]);
             vstart1 = buf[16 + id_p * 8 : 8] + 16 + buf[8 : 8] + 1;
             if (true && key2[0] == id_p) {
                 cstart3 = vstart1 + 1 + 1;
                 i4 = 0;
                 count5 = buf[vstart1 : 1];
                 loop (i4 < count5) {
                     cstart6 = cstart3;
                     cstart7 = cstart6 + 1;
                     low11 = 0;
                     high12 = 10 / 2;
                     loop (low11 < high12) {
                         mid13 = low11 + high12 / 2;
                         kstart8 = 4 + buf[4 : 4] + mid13 * 2;
                         key14 = (buf[kstart8 : 1]);
                         if (key14[0] < buf[cstart6 : 1]) {
                             low11 = mid13 + 1;
                         } else {
                              high12 = mid13;
                         }
                     }
                     if (low11 < 10 / 2) {
                         kstart8 = 4 + buf[4 : 4] + low11 * 2;
                         key15 = (buf[kstart8 : 1]);
                         key16 = key15;
                         loop (key16[0] < buf[cstart7 : 1] && low11 < 10 / 2) {
                             vstart9 = buf[4 + buf[4 : 4] + low11 * 2 + 1 : 1] +
                             10 + 4 + buf[4 : 4];
                             key10 = key16;
                             cstart17 = vstart9;
                             i18 = 0;
                             count19 = 1;
                             loop (i18 < count19) {
                                 cstart20 = cstart17;
                                 cstart21 = cstart20 + 1;
                                 if (buf[cstart20 : 1] == id_c) {
                                     consume(Tuple[Int[nonnull], Int[nonnull]],
                                     (buf[cstart6 : 1], buf[cstart21 : 1]));
                                 } else {

                                 }
                                 cstart17 = cstart17 + 2;
                                 i18 = i18 + 1;
                             }
                             low11 = low11 + 1;
                             kstart8 = 4 + buf[4 : 4] + low11 * 2;
                             key22 = (buf[kstart8 : 1]);
                             key16 = key22;
                         }
                     } else {

                     }
                     cstart3 = cstart3 + 2;
                     i4 = i4 + 1;
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
    [ERROR] Tried to get schema of unnamed predicate min(counter).
    [ERROR] Tried to get schema of unnamed predicate min(counter).
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
  Demomatch.(run_test ~params:example_db_params (example3 "log_str")) ;
  [%expect
    {|
    (FuncT
     (((FuncT
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
           (((FuncT
              (((OrderedIdxT
                 ((IntT ((range (Interval 1 5)) (nullable false)))
                  (ListT
                   ((TupleT
                     (((StringT ((nchars (Interval 3 8)) (nullable false)))
                       (IntT ((range (Interval 1 5)) (nullable false))))
                      ((count (Interval 1 1)))))
                    ((count (Interval 1 1)))))
                  ((count (Interval 5 5))))))
               Child_sum)))
            (Width 2))))
         Child_sum)))
      (Width 2)))
    // Locals:
    // cstart29 : Int[nonnull] (persists=true)
    // cstart44 : Int[nonnull] (persists=true)
    // i27 : Int[nonnull] (persists=true)
    // key25 : Tuple[String[nonnull]] (persists=false)
    // count42 : Int[nonnull] (persists=true)
    // cstart43 : Int[nonnull] (persists=true)
    // vstart24 : Int[nonnull] (persists=false)
    // kstart23 : Int[nonnull] (persists=false)
    // count28 : Int[nonnull] (persists=true)
    // key38 : Tuple[Int[nonnull]] (persists=false)
    // low34 : Int[nonnull] (persists=true)
    // key37 : Tuple[Int[nonnull]] (persists=false)
    // cstart30 : Int[nonnull] (persists=true)
    // high35 : Int[nonnull] (persists=true)
    // kstart31 : Int[nonnull] (persists=true)
    // key39 : Tuple[Int[nonnull]] (persists=true)
    // i41 : Int[nonnull] (persists=true)
    // key45 : Tuple[Int[nonnull]] (persists=false)
    // mid36 : Int[nonnull] (persists=true)
    // key33 : Tuple[Int[nonnull]] (persists=true)
    // vstart32 : Int[nonnull] (persists=true)
    // cstart40 : Int[nonnull] (persists=true)
    // cstart26 : Int[nonnull] (persists=true)
    fun printer () : Void {
        if (hash(16, id_p) * 8 < 0 || buf[16 + buf[8 : 8] : 8] - 1 <
            hash(16, id_p) * 8) {

        } else {
             kstart23 = buf[16 + buf[8 : 8] + 8 + hash(16, id_p) * 8 : 8] + 16 +
             buf[8 : 8] + 8 + buf[16 + buf[8 : 8] : 8];
             key25 = (load_str(kstart23 + 1, buf[kstart23 : 1]));
             vstart24 = buf[16 + buf[8 : 8] + 8 + hash(16, id_p) * 8 : 8] + 16 +
             buf[8 : 8] + 8 + buf[16 + buf[8 : 8] : 8] + 1 + buf[buf[16 + buf[8 :
             8] + 8 + hash(16, id_p) * 8 : 8] + 16 + buf[8 : 8] + 8 + buf[16 +
             buf[8 : 8] : 8] : 1];
             if (true && key25[0] == id_p) {
                 cstart26 = vstart24 + 1 + 1;
                 i27 = 0;
                 count28 = buf[vstart24 : 1];
                 loop (i27 < count28) {
                     cstart29 = cstart26;
                     cstart30 = cstart29 + 1;
                     low34 = 0;
                     high35 = 10 / 2;
                     loop (low34 < high35) {
                         mid36 = low34 + high35 / 2;
                         kstart31 = 4 + buf[4 : 4] + 1 + mid36 * 2;
                         key37 = (buf[kstart31 : 1]);
                         if (key37[0] < buf[cstart29 : 1]) {
                             low34 = mid36 + 1;
                         } else {
                              high35 = mid36;
                         }
                     }
                     if (low34 < 10 / 2) {
                         kstart31 = 4 + buf[4 : 4] + 1 + low34 * 2;
                         key38 = (buf[kstart31 : 1]);
                         key39 = key38;
                         loop (key39[0] < buf[cstart30 : 1] && low34 < 10 / 2) {
                             vstart32 = buf[4 + buf[4 : 4] + 1 + low34 * 2 + 1 :
                             1] + 10 + 4 + buf[4 : 4] + 1;
                             key33 = key39;
                             cstart40 = vstart32 + 1;
                             i41 = 0;
                             count42 = 1;
                             loop (i41 < count42) {
                                 cstart43 = cstart40 + 1;
                                 cstart44 = cstart43 + 1 + buf[cstart43 : 1];
                                 if (load_str(cstart43 + 1, buf[cstart43 : 1]) ==
                                     id_c) {
                                     print(Tuple[Int[nonnull], Int[nonnull]],
                                     (buf[cstart29 : 1], buf[cstart44 : 1]));
                                 } else {

                                 }
                                 cstart40 = cstart40 + buf[cstart40 : 1];
                                 i41 = i41 + 1;
                             }
                             low34 = low34 + 1;
                             kstart31 = 4 + buf[4 : 4] + 1 + low34 * 2;
                             key45 = (buf[kstart31 : 1]);
                             key39 = key45;
                         }
                     } else {

                     }
                     cstart26 = cstart26 + 2;
                     i27 = i27 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // i18 : Int[nonnull] (persists=true)
    // key10 : Tuple[Int[nonnull]] (persists=true)
    // key22 : Tuple[Int[nonnull]] (persists=false)
    // cstart21 : Int[nonnull] (persists=true)
    // high12 : Int[nonnull] (persists=true)
    // cstart17 : Int[nonnull] (persists=true)
    // mid13 : Int[nonnull] (persists=true)
    // count5 : Int[nonnull] (persists=true)
    // key14 : Tuple[Int[nonnull]] (persists=false)
    // key16 : Tuple[Int[nonnull]] (persists=true)
    // key2 : Tuple[String[nonnull]] (persists=false)
    // count19 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart7 : Int[nonnull] (persists=true)
    // vstart9 : Int[nonnull] (persists=true)
    // kstart8 : Int[nonnull] (persists=true)
    // key15 : Tuple[Int[nonnull]] (persists=false)
    // kstart0 : Int[nonnull] (persists=false)
    // vstart1 : Int[nonnull] (persists=false)
    // i4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // low11 : Int[nonnull] (persists=true)
    // cstart20 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        if (hash(16, id_p) * 8 < 0 || buf[16 + buf[8 : 8] : 8] - 1 <
            hash(16, id_p) * 8) {

        } else {
             kstart0 = buf[16 + buf[8 : 8] + 8 + hash(16, id_p) * 8 : 8] + 16 +
             buf[8 : 8] + 8 + buf[16 + buf[8 : 8] : 8];
             key2 = (load_str(kstart0 + 1, buf[kstart0 : 1]));
             vstart1 = buf[16 + buf[8 : 8] + 8 + hash(16, id_p) * 8 : 8] + 16 +
             buf[8 : 8] + 8 + buf[16 + buf[8 : 8] : 8] + 1 + buf[buf[16 + buf[8 :
             8] + 8 + hash(16, id_p) * 8 : 8] + 16 + buf[8 : 8] + 8 + buf[16 +
             buf[8 : 8] : 8] : 1];
             if (true && key2[0] == id_p) {
                 cstart3 = vstart1 + 1 + 1;
                 i4 = 0;
                 count5 = buf[vstart1 : 1];
                 loop (i4 < count5) {
                     cstart6 = cstart3;
                     cstart7 = cstart6 + 1;
                     low11 = 0;
                     high12 = 10 / 2;
                     loop (low11 < high12) {
                         mid13 = low11 + high12 / 2;
                         kstart8 = 4 + buf[4 : 4] + 1 + mid13 * 2;
                         key14 = (buf[kstart8 : 1]);
                         if (key14[0] < buf[cstart6 : 1]) {
                             low11 = mid13 + 1;
                         } else {
                              high12 = mid13;
                         }
                     }
                     if (low11 < 10 / 2) {
                         kstart8 = 4 + buf[4 : 4] + 1 + low11 * 2;
                         key15 = (buf[kstart8 : 1]);
                         key16 = key15;
                         loop (key16[0] < buf[cstart7 : 1] && low11 < 10 / 2) {
                             vstart9 = buf[4 + buf[4 : 4] + 1 + low11 * 2 + 1 :
                             1] + 10 + 4 + buf[4 : 4] + 1;
                             key10 = key16;
                             cstart17 = vstart9 + 1;
                             i18 = 0;
                             count19 = 1;
                             loop (i18 < count19) {
                                 cstart20 = cstart17 + 1;
                                 cstart21 = cstart20 + 1 + buf[cstart20 : 1];
                                 if (load_str(cstart20 + 1, buf[cstart20 : 1]) ==
                                     id_c) {
                                     consume(Tuple[Int[nonnull], Int[nonnull]],
                                     (buf[cstart6 : 1], buf[cstart21 : 1]));
                                 } else {

                                 }
                                 cstart17 = cstart17 + buf[cstart17 : 1];
                                 i18 = i18 + 1;
                             }
                             low11 = low11 + 1;
                             kstart8 = 4 + buf[4 : 4] + 1 + low11 * 2;
                             key22 = (buf[kstart8 : 1]);
                             key16 = key22;
                         }
                     } else {

                     }
                     cstart3 = cstart3 + 2;
                     i4 = i4 + 1;
                 }
             } else {

             }
        }
    } |}]
