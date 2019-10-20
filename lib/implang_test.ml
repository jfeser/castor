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
    M.annotate_type layout;
    let type_ = Meta.(find_exn layout type_) in
    print_endline (Sexp.to_string_hum ([%sexp_of: Type.t] type_));
    let ir = I.irgen ~params:param_names ~data_fn:"/tmp/buf" layout in
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
     (((IntT ((range (Interval 1 1)) (distinct <opaque>) (nullable false)))
       (IntT ((range (Interval 2 2)) (distinct <opaque>) (nullable false))))
      ((kind Cross))))
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

let%expect_test "sum-complex" =
  run_test sum_complex;
  [%expect
    {|
    (FuncT
     (((ListT
        ((TupleT
          (((IntT ((range (Interval 1 3)) (distinct <opaque>) (nullable false)))
            (IntT ((range (Interval -1 2)) (distinct <opaque>) (nullable false))))
           ((kind Cross))))
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
            sum20 = sum20 + int2fl(tup16[0]) / int2fl(2);
            found_tup17 = true;
            cstart21 = cstart21 + 2;
            i22 = i22 + 1;
        }
        if (found_tup17) {
            print(Tuple[Int[nonnull], Fixed[nonnull]],
            (sum18 + 5, int2fl(count19) + sum20));
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
            sum7 = sum7 + int2fl(tup3[0]) / int2fl(2);
            found_tup4 = true;
            cstart8 = cstart8 + 2;
            i9 = i9 + 1;
        }
        if (found_tup4) {
            consume(Tuple[Int[nonnull], Fixed[nonnull]],
            (sum5 + 5, int2fl(count6) + sum7));
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
          (((IntT ((range (Interval 1 3)) (distinct <opaque>) (nullable false)))
            (IntT ((range (Interval -1 2)) (distinct <opaque>) (nullable false))))
           ((kind Cross))))
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
  run_test "AList(r1 as k, ATuple([AScalar(k.f), AScalar(k.g - k.f)], cross))";
  [%expect
    {|
    (ListT
     ((TupleT
       (((IntT ((range (Interval 1 3)) (distinct <opaque>) (nullable false)))
         (IntT ((range (Interval -1 2)) (distinct <opaque>) (nullable false))))
        ((kind Cross))))
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
    "depjoin(AList(r1 as kl, AScalar(kl.f)) as k, AHashIdx(dedup(select([f], \
     r1)) as kh, ascalar(kh.f + 1), k.f))";
  [%expect
    {|
    (FuncT
     (((ListT
        ((IntT ((range (Interval 1 3)) (distinct <opaque>) (nullable false)))
         ((count (Interval 5 5)))))
       (HashIdxT
        ((IntT ((range (Interval 1 3)) (distinct <opaque>) (nullable false)))
         (IntT ((range (Interval 2 4)) (distinct <opaque>) (nullable false)))
         ((key_count (Interval 3 3))))))
      Child_sum))
    // Locals:
    // count8 : Int[nonnull] (persists=true)
    // i7 : Int[nonnull] (persists=true)
    // vstart10 : Int[nonnull] (persists=false)
    // key11 : Tuple[Int[nonnull]] (persists=false)
    // cstart6 : Int[nonnull] (persists=true)
    // kstart9 : Int[nonnull] (persists=false)
    fun printer () : Void {
        cstart6 = 1;
        i7 = 0;
        count8 = 5;
        loop (i7 < count8) {
            if (buf[cstart6 : 1] * 1 < 0 || buf[7 : 1] - 1 < buf[cstart6 : 1] * 1) {

            } else {
                 kstart9 = buf[8 + buf[cstart6 : 1] * 1 : 1] + 8 + buf[7 : 1];
                 key11 = (buf[kstart9 : 1]);
                 vstart10 =
                 buf[8 + buf[cstart6 : 1] * 1 : 1] + 8 + buf[7 : 1] + 1;
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
        cstart0 = 1;
        i1 = 0;
        count2 = 5;
        loop (i1 < count2) {
            if (buf[cstart0 : 1] * 1 < 0 || buf[7 : 1] - 1 < buf[cstart0 : 1] * 1) {

            } else {
                 kstart3 = buf[8 + buf[cstart0 : 1] * 1 : 1] + 8 + buf[7 : 1];
                 key5 = (buf[kstart3 : 1]);
                 vstart4 =
                 buf[8 + buf[cstart0 : 1] * 1 : 1] + 8 + buf[7 : 1] + 1;
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
    "depjoin(AList(r1 as kl, AScalar(kl.f)) as k, AOrderedIdx(dedup(select([f \
     as kf], r1)) as k1, ascalar(k1.kf+1), k.f, k.f+1))";
  [%expect
    {|
    (FuncT
     (((ListT
        ((IntT ((range (Interval 1 3)) (distinct <opaque>) (nullable false)))
         ((count (Interval 5 5)))))
       (OrderedIdxT
        ((IntT ((range (Interval 1 3)) (distinct <opaque>) (nullable false)))
         (IntT ((range (Interval 2 4)) (distinct <opaque>) (nullable false)))
         ((key_count (Interval 3 3))))))
      Child_sum))
    // Locals:
    // count16 : Int[nonnull] (persists=true)
    // mid22 : Int[nonnull] (persists=true)
    // key23 : Tuple[Int[nonnull]] (persists=false)
    // key19 : Tuple[Int[nonnull]] (persists=true)
    // kstart17 : Int[nonnull] (persists=true)
    // key27 : Tuple[Int[nonnull]] (persists=false)
    // key25 : Tuple[Int[nonnull]] (persists=false)
    // key26 : Tuple[Int[nonnull]] (persists=true)
    // idx24 : Int[nonnull] (persists=true)
    // vstart18 : Int[nonnull] (persists=true)
    // i15 : Int[nonnull] (persists=true)
    // cstart14 : Int[nonnull] (persists=true)
    // low20 : Int[nonnull] (persists=true)
    // high21 : Int[nonnull] (persists=true)
    fun printer () : Void {
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
                        print(Tuple[Int[nonnull], Int[nonnull]],
                        (key19[0], buf[vstart18 : 1]));
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
    // idx10 : Int[nonnull] (persists=true)
    // i1 : Int[nonnull] (persists=true)
    // key13 : Tuple[Int[nonnull]] (persists=false)
    // vstart4 : Int[nonnull] (persists=true)
    // mid8 : Int[nonnull] (persists=true)
    // key9 : Tuple[Int[nonnull]] (persists=false)
    // key11 : Tuple[Int[nonnull]] (persists=false)
    // count2 : Int[nonnull] (persists=true)
    // high7 : Int[nonnull] (persists=true)
    // kstart3 : Int[nonnull] (persists=true)
    // key5 : Tuple[Int[nonnull]] (persists=true)
    // low6 : Int[nonnull] (persists=true)
    // key12 : Tuple[Int[nonnull]] (persists=true)
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
                        consume(Tuple[Int[nonnull], Int[nonnull]],
                        (key5[0], buf[vstart4 : 1]));
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
     ((DateT
       ((range (Interval 17136 17775)) (distinct <opaque>) (nullable false)))
      (DateT
       ((range (Interval 17136 17775)) (distinct <opaque>) (nullable false)))
      ((key_count (Interval 5 5))))) |}]

let%expect_test "example-1" =
  Demomatch.(run_test ~params:Demomatch.example_params (example1 "log"));
  [%expect
    {|
    (FuncT
     (((FuncT
        (((ListT
           ((TupleT
             (((IntT
                ((range (Interval 1 1)) (distinct <opaque>) (nullable false)))
               (IntT
                ((range (Interval 1 4)) (distinct <opaque>) (nullable false)))
               (ListT
                ((TupleT
                  (((IntT
                     ((range (Interval 2 3)) (distinct <opaque>)
                      (nullable false)))
                    (IntT
                     ((range (Interval 2 5)) (distinct <opaque>)
                      (nullable false))))
                   ((kind Cross))))
                 ((count (Interval 1 2))))))
              ((kind Cross))))
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
  Demomatch.(run_test ~params:example_params (example2 "log"));
  [%expect
    {|
    (FuncT
     (((HashIdxT
        ((TupleT
          (((IntT ((range (Interval 1 1)) (distinct <opaque>) (nullable false)))
            (IntT ((range (Interval 2 3)) (distinct <opaque>) (nullable false))))
           ((kind Cross))))
         (ListT
          ((TupleT
            (((IntT
               ((range (Interval 1 4)) (distinct <opaque>) (nullable false)))
              (IntT
               ((range (Interval 2 5)) (distinct <opaque>) (nullable false))))
             ((kind Cross))))
           ((count (Interval 1 2)))))
         ((key_count (Interval 2 2))))))
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
        if (<tuplehash> * 1 < 0 || buf[4 + buf[2 : 2] : 1] - 1 < <tuplehash> * 1) {

        } else {
             kstart10 =
             buf[4 + buf[2 : 2] + 1 + <tuplehash> * 1 : 1] +
             4 + buf[2 : 2] + 1 + buf[4 + buf[2 : 2] : 1];
             cstart13 = kstart10;
             cstart14 = cstart13 + 1;
             key12 = (buf[cstart13 : 1], buf[cstart14 : 1]);
             vstart11 =
             buf[4 + buf[2 : 2] + 1 + <tuplehash> * 1 : 1] +
             4 + buf[2 : 2] + 1 + buf[4 + buf[2 : 2] : 1] + 2;
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
        if (<tuplehash> * 1 < 0 || buf[4 + buf[2 : 2] : 1] - 1 < <tuplehash> * 1) {

        } else {
             kstart0 =
             buf[4 + buf[2 : 2] + 1 + <tuplehash> * 1 : 1] +
             4 + buf[2 : 2] + 1 + buf[4 + buf[2 : 2] : 1];
             cstart3 = kstart0;
             cstart4 = cstart3 + 1;
             key2 = (buf[cstart3 : 1], buf[cstart4 : 1]);
             vstart1 =
             buf[4 + buf[2 : 2] + 1 + <tuplehash> * 1 : 1] +
             4 + buf[2 : 2] + 1 + buf[4 + buf[2 : 2] : 1] + 2;
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
  Demomatch.(run_test ~params:example_params (example3 "log"));
  [%expect
    {|
    (FuncT
     (((FuncT
        (((HashIdxT
           ((IntT ((range (Interval 1 3)) (distinct <opaque>) (nullable false)))
            (ListT
             ((TupleT
               (((IntT
                  ((range (Interval 1 5)) (distinct <opaque>) (nullable false)))
                 (IntT
                  ((range (Interval 3 6)) (distinct <opaque>) (nullable false))))
                ((kind Cross))))
              ((count (Interval 1 2)))))
            ((key_count (Interval 3 3)))))
          (FuncT
           (((FuncT
              (((OrderedIdxT
                 ((IntT
                   ((range (Interval 1 5)) (distinct <opaque>) (nullable false)))
                  (ListT
                   ((TupleT
                     (((IntT
                        ((range (Interval 1 3)) (distinct <opaque>)
                         (nullable false)))
                       (IntT
                        ((range (Interval 1 5)) (distinct <opaque>)
                         (nullable false))))
                      ((kind Cross))))
                    ((count (Interval 1 1)))))
                  ((key_count (Interval 5 5))))))
               Child_sum)))
            (Width 2))))
         Child_sum)))
      (Width 2)))
    // Locals:
    // vstart33 : Int[nonnull] (persists=true)
    // key34 : Tuple[Int[nonnull]] (persists=true)
    // high36 : Int[nonnull] (persists=true)
    // count44 : Int[nonnull] (persists=true)
    // key47 : Tuple[Int[nonnull]] (persists=false)
    // cstart31 : Int[nonnull] (persists=true)
    // vstart25 : Int[nonnull] (persists=false)
    // kstart24 : Int[nonnull] (persists=false)
    // cstart46 : Int[nonnull] (persists=true)
    // count29 : Int[nonnull] (persists=true)
    // key41 : Tuple[Int[nonnull]] (persists=true)
    // key38 : Tuple[Int[nonnull]] (persists=false)
    // idx39 : Int[nonnull] (persists=true)
    // i28 : Int[nonnull] (persists=true)
    // cstart30 : Int[nonnull] (persists=true)
    // mid37 : Int[nonnull] (persists=true)
    // kstart32 : Int[nonnull] (persists=true)
    // cstart42 : Int[nonnull] (persists=true)
    // low35 : Int[nonnull] (persists=true)
    // key26 : Tuple[Int[nonnull]] (persists=false)
    // i43 : Int[nonnull] (persists=true)
    // cstart45 : Int[nonnull] (persists=true)
    // cstart27 : Int[nonnull] (persists=true)
    // key40 : Tuple[Int[nonnull]] (persists=false)
    fun printer () : Void {
        if (id_p * 1 < 0 || buf[2 : 1] - 1 < id_p * 1) {

        } else {
             kstart24 = buf[3 + id_p * 1 : 1] + 3 + buf[2 : 1];
             key26 = (buf[kstart24 : 1]);
             vstart25 = buf[3 + id_p * 1 : 1] + 3 + buf[2 : 1] + 1;
             if (true && key26[0] == id_p) {
                 cstart27 = vstart25 + 1 + 1;
                 i28 = 0;
                 count29 = buf[vstart25 : 1];
                 loop (i28 < count29) {
                     cstart30 = cstart27;
                     cstart31 = cstart30 + 1;
                     low35 = 0;
                     high36 = 10 / 2;
                     loop (low35 < high36) {
                         mid37 = low35 + high36 / 2;
                         kstart32 = 1 + buf[1 : 1] + mid37 * 2;
                         key38 = (buf[kstart32 : 1]);
                         if (not(key38[0] < buf[cstart30 : 1])) {
                             high36 = mid37;
                         } else {
                              low35 = mid37 + 1;
                         }
                     }
                     idx39 = low35;
                     if (idx39 < 10 / 2) {
                         kstart32 = 1 + buf[1 : 1] + idx39 * 2;
                         key40 = (buf[kstart32 : 1]);
                         key41 = key40;
                         loop (key41[0] < buf[cstart31 : 1] && idx39 < 10 / 2) {
                             if (key41[0] < buf[cstart31 : 1] &&
                                 not(key41[0] < buf[cstart30 : 1])) {
                                 vstart33 =
                                 buf[1 + buf[1 : 1] + idx39 * 2 + 1 : 1] + 10 +
                                 1 + buf[1 : 1];
                                 key34 = key41;
                                 cstart42 = vstart33;
                                 i43 = 0;
                                 count44 = 1;
                                 loop (i43 < count44) {
                                     cstart45 = cstart42;
                                     cstart46 = cstart45 + 1;
                                     if (buf[cstart45 : 1] == id_c) {
                                         print(Tuple[Int[nonnull], Int[nonnull]],
                                         (buf[cstart30 : 1], buf[cstart46 : 1]));
                                     } else {

                                     }
                                     cstart42 = cstart42 + 2;
                                     i43 = i43 + 1;
                                 }
                             } else {

                             }
                             idx39 = idx39 + 1;
                             kstart32 = 1 + buf[1 : 1] + idx39 * 2;
                             key47 = (buf[kstart32 : 1]);
                             key41 = key47;
                         }
                     } else {

                     }
                     cstart27 = cstart27 + 2;
                     i28 = i28 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // key10 : Tuple[Int[nonnull]] (persists=true)
    // cstart21 : Int[nonnull] (persists=true)
    // high12 : Int[nonnull] (persists=true)
    // key17 : Tuple[Int[nonnull]] (persists=true)
    // cstart22 : Int[nonnull] (persists=true)
    // mid13 : Int[nonnull] (persists=true)
    // count5 : Int[nonnull] (persists=true)
    // key14 : Tuple[Int[nonnull]] (persists=false)
    // key16 : Tuple[Int[nonnull]] (persists=false)
    // key2 : Tuple[Int[nonnull]] (persists=false)
    // i19 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart7 : Int[nonnull] (persists=true)
    // cstart18 : Int[nonnull] (persists=true)
    // vstart9 : Int[nonnull] (persists=true)
    // key23 : Tuple[Int[nonnull]] (persists=false)
    // kstart8 : Int[nonnull] (persists=true)
    // kstart0 : Int[nonnull] (persists=false)
    // count20 : Int[nonnull] (persists=true)
    // vstart1 : Int[nonnull] (persists=false)
    // i4 : Int[nonnull] (persists=true)
    // idx15 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // low11 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        if (id_p * 1 < 0 || buf[2 : 1] - 1 < id_p * 1) {

        } else {
             kstart0 = buf[3 + id_p * 1 : 1] + 3 + buf[2 : 1];
             key2 = (buf[kstart0 : 1]);
             vstart1 = buf[3 + id_p * 1 : 1] + 3 + buf[2 : 1] + 1;
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
                         kstart8 = 1 + buf[1 : 1] + mid13 * 2;
                         key14 = (buf[kstart8 : 1]);
                         if (not(key14[0] < buf[cstart6 : 1])) {
                             high12 = mid13;
                         } else {
                              low11 = mid13 + 1;
                         }
                     }
                     idx15 = low11;
                     if (idx15 < 10 / 2) {
                         kstart8 = 1 + buf[1 : 1] + idx15 * 2;
                         key16 = (buf[kstart8 : 1]);
                         key17 = key16;
                         loop (key17[0] < buf[cstart7 : 1] && idx15 < 10 / 2) {
                             if (key17[0] < buf[cstart7 : 1] &&
                                 not(key17[0] < buf[cstart6 : 1])) {
                                 vstart9 =
                                 buf[1 + buf[1 : 1] + idx15 * 2 + 1 : 1] + 10 +
                                 1 + buf[1 : 1];
                                 key10 = key17;
                                 cstart18 = vstart9;
                                 i19 = 0;
                                 count20 = 1;
                                 loop (i19 < count20) {
                                     cstart21 = cstart18;
                                     cstart22 = cstart21 + 1;
                                     if (buf[cstart21 : 1] == id_c) {
                                         consume(Tuple[Int[nonnull],
                                         Int[nonnull]],
                                         (buf[cstart6 : 1], buf[cstart22 : 1]));
                                     } else {

                                     }
                                     cstart18 = cstart18 + 2;
                                     i19 = i19 + 1;
                                 }
                             } else {

                             }
                             idx15 = idx15 + 1;
                             kstart8 = 1 + buf[1 : 1] + idx15 * 2;
                             key23 = (buf[kstart8 : 1]);
                             key17 = key23;
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
|};
  [%expect
    {|
    (FuncT
     (((FuncT
        (((ListT
           ((IntT ((range (Interval 1 3)) (distinct <opaque>) (nullable false)))
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
  Demomatch.(run_test ~params:example_db_params (example3 "log_str"));
  [%expect
    {|
    (FuncT
     (((FuncT
        (((HashIdxT
           ((StringT
             ((nchars (Interval 3 8)) (distinct <opaque>) (nullable false)))
            (ListT
             ((TupleT
               (((IntT
                  ((range (Interval 1 5)) (distinct <opaque>) (nullable false)))
                 (IntT
                  ((range (Interval 3 6)) (distinct <opaque>) (nullable false))))
                ((kind Cross))))
              ((count (Interval 1 2)))))
            ((key_count (Interval 3 3)))))
          (FuncT
           (((FuncT
              (((OrderedIdxT
                 ((IntT
                   ((range (Interval 1 5)) (distinct <opaque>) (nullable false)))
                  (ListT
                   ((TupleT
                     (((StringT
                        ((nchars (Interval 3 8)) (distinct <opaque>)
                         (nullable false)))
                       (IntT
                        ((range (Interval 1 5)) (distinct <opaque>)
                         (nullable false))))
                      ((kind Cross))))
                    ((count (Interval 1 1)))))
                  ((key_count (Interval 5 5))))))
               Child_sum)))
            (Width 2))))
         Child_sum)))
      (Width 2)))
    // Locals:
    // vstart33 : Int[nonnull] (persists=true)
    // key34 : Tuple[Int[nonnull]] (persists=true)
    // high36 : Int[nonnull] (persists=true)
    // count44 : Int[nonnull] (persists=true)
    // key47 : Tuple[Int[nonnull]] (persists=false)
    // cstart31 : Int[nonnull] (persists=true)
    // vstart25 : Int[nonnull] (persists=false)
    // kstart24 : Int[nonnull] (persists=false)
    // cstart46 : Int[nonnull] (persists=true)
    // count29 : Int[nonnull] (persists=true)
    // key41 : Tuple[Int[nonnull]] (persists=true)
    // key38 : Tuple[Int[nonnull]] (persists=false)
    // idx39 : Int[nonnull] (persists=true)
    // i28 : Int[nonnull] (persists=true)
    // cstart30 : Int[nonnull] (persists=true)
    // mid37 : Int[nonnull] (persists=true)
    // kstart32 : Int[nonnull] (persists=true)
    // cstart42 : Int[nonnull] (persists=true)
    // low35 : Int[nonnull] (persists=true)
    // key26 : Tuple[String[nonnull]] (persists=false)
    // i43 : Int[nonnull] (persists=true)
    // cstart45 : Int[nonnull] (persists=true)
    // cstart27 : Int[nonnull] (persists=true)
    // key40 : Tuple[Int[nonnull]] (persists=false)
    fun printer () : Void {
        if (hash(6, id_p) * 1 < 0 ||
            buf[6 + buf[4 : 2] : 1] - 1 < hash(6, id_p) * 1) {

        } else {
             kstart24 =
             buf[6 + buf[4 : 2] + 1 + hash(6, id_p) * 1 : 1] +
             6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1];
             key26 = (load_str(kstart24 + 1, buf[kstart24 : 1]));
             vstart25 =
             buf[6 + buf[4 : 2] + 1 + hash(6, id_p) * 1 : 1] +
             6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1] +
             1 +
             buf[buf[6 + buf[4 : 2] + 1 + hash(6, id_p) * 1 : 1] +
                 6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1] : 1];
             if (true && key26[0] == id_p) {
                 cstart27 = vstart25 + 1 + 1;
                 i28 = 0;
                 count29 = buf[vstart25 : 1];
                 loop (i28 < count29) {
                     cstart30 = cstart27;
                     cstart31 = cstart30 + 1;
                     low35 = 0;
                     high36 = 10 / 2;
                     loop (low35 < high36) {
                         mid37 = low35 + high36 / 2;
                         kstart32 = 2 + buf[2 : 2] + 1 + mid37 * 2;
                         key38 = (buf[kstart32 : 1]);
                         if (not(key38[0] < buf[cstart30 : 1])) {
                             high36 = mid37;
                         } else {
                              low35 = mid37 + 1;
                         }
                     }
                     idx39 = low35;
                     if (idx39 < 10 / 2) {
                         kstart32 = 2 + buf[2 : 2] + 1 + idx39 * 2;
                         key40 = (buf[kstart32 : 1]);
                         key41 = key40;
                         loop (key41[0] < buf[cstart31 : 1] && idx39 < 10 / 2) {
                             if (key41[0] < buf[cstart31 : 1] &&
                                 not(key41[0] < buf[cstart30 : 1])) {
                                 vstart33 =
                                 buf[2 + buf[2 : 2] + 1 + idx39 * 2 + 1 : 1] + 10 +
                                 2 + buf[2 : 2] + 1;
                                 key34 = key41;
                                 cstart42 = vstart33 + 1;
                                 i43 = 0;
                                 count44 = 1;
                                 loop (i43 < count44) {
                                     cstart45 = cstart42 + 1;
                                     cstart46 = cstart45 + 1 + buf[cstart45 : 1];
                                     if (load_str(cstart45 + 1, buf[cstart45 :
                                         1]) == id_c) {
                                         print(Tuple[Int[nonnull], Int[nonnull]],
                                         (buf[cstart30 : 1], buf[cstart46 : 1]));
                                     } else {

                                     }
                                     cstart42 = cstart42 + buf[cstart42 : 1];
                                     i43 = i43 + 1;
                                 }
                             } else {

                             }
                             idx39 = idx39 + 1;
                             kstart32 = 2 + buf[2 : 2] + 1 + idx39 * 2;
                             key47 = (buf[kstart32 : 1]);
                             key41 = key47;
                         }
                     } else {

                     }
                     cstart27 = cstart27 + 2;
                     i28 = i28 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // key10 : Tuple[Int[nonnull]] (persists=true)
    // cstart21 : Int[nonnull] (persists=true)
    // high12 : Int[nonnull] (persists=true)
    // key17 : Tuple[Int[nonnull]] (persists=true)
    // cstart22 : Int[nonnull] (persists=true)
    // mid13 : Int[nonnull] (persists=true)
    // count5 : Int[nonnull] (persists=true)
    // key14 : Tuple[Int[nonnull]] (persists=false)
    // key16 : Tuple[Int[nonnull]] (persists=false)
    // key2 : Tuple[String[nonnull]] (persists=false)
    // i19 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart7 : Int[nonnull] (persists=true)
    // cstart18 : Int[nonnull] (persists=true)
    // vstart9 : Int[nonnull] (persists=true)
    // key23 : Tuple[Int[nonnull]] (persists=false)
    // kstart8 : Int[nonnull] (persists=true)
    // kstart0 : Int[nonnull] (persists=false)
    // count20 : Int[nonnull] (persists=true)
    // vstart1 : Int[nonnull] (persists=false)
    // i4 : Int[nonnull] (persists=true)
    // idx15 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // low11 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        if (hash(6, id_p) * 1 < 0 ||
            buf[6 + buf[4 : 2] : 1] - 1 < hash(6, id_p) * 1) {

        } else {
             kstart0 =
             buf[6 + buf[4 : 2] + 1 + hash(6, id_p) * 1 : 1] +
             6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1];
             key2 = (load_str(kstart0 + 1, buf[kstart0 : 1]));
             vstart1 =
             buf[6 + buf[4 : 2] + 1 + hash(6, id_p) * 1 : 1] +
             6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1] +
             1 +
             buf[buf[6 + buf[4 : 2] + 1 + hash(6, id_p) * 1 : 1] +
                 6 + buf[4 : 2] + 1 + buf[6 + buf[4 : 2] : 1] : 1];
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
                         kstart8 = 2 + buf[2 : 2] + 1 + mid13 * 2;
                         key14 = (buf[kstart8 : 1]);
                         if (not(key14[0] < buf[cstart6 : 1])) {
                             high12 = mid13;
                         } else {
                              low11 = mid13 + 1;
                         }
                     }
                     idx15 = low11;
                     if (idx15 < 10 / 2) {
                         kstart8 = 2 + buf[2 : 2] + 1 + idx15 * 2;
                         key16 = (buf[kstart8 : 1]);
                         key17 = key16;
                         loop (key17[0] < buf[cstart7 : 1] && idx15 < 10 / 2) {
                             if (key17[0] < buf[cstart7 : 1] &&
                                 not(key17[0] < buf[cstart6 : 1])) {
                                 vstart9 =
                                 buf[2 + buf[2 : 2] + 1 + idx15 * 2 + 1 : 1] + 10 +
                                 2 + buf[2 : 2] + 1;
                                 key10 = key17;
                                 cstart18 = vstart9 + 1;
                                 i19 = 0;
                                 count20 = 1;
                                 loop (i19 < count20) {
                                     cstart21 = cstart18 + 1;
                                     cstart22 = cstart21 + 1 + buf[cstart21 : 1];
                                     if (load_str(cstart21 + 1, buf[cstart21 :
                                         1]) == id_c) {
                                         consume(Tuple[Int[nonnull],
                                         Int[nonnull]],
                                         (buf[cstart6 : 1], buf[cstart22 : 1]));
                                     } else {

                                     }
                                     cstart18 = cstart18 + buf[cstart18 : 1];
                                     i19 = i19 + 1;
                                 }
                             } else {

                             }
                             idx15 = idx15 + 1;
                             kstart8 = 2 + buf[2 : 2] + 1 + idx15 * 2;
                             key23 = (buf[kstart8 : 1]);
                             key17 = key23;
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
