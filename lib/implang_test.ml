open Core
open Base
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

module E = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (E)

let _ =
  Test_util.create rels "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

let _ =
  Test_util.create rels "log" ["id"; "succ"; "counter"]
    [[1; 4; 1]; [2; 3; 2]; [3; 4; 3]; [4; 6; 1]; [5; 6; 3]]

let _ =
  Test_util.create_val rels "log_str"
    [ ("counter", Type.PrimType.IntT {nullable= false})
    ; ("succ", Type.PrimType.IntT {nullable= false})
    ; ("id", Type.PrimType.StringT {nullable= false}) ]
    [ [Int 1; Int 4; String "foo"]
    ; [Int 2; Int 3; String "fizzbuzz"]
    ; [Int 3; Int 4; String "bar"]
    ; [Int 4; Int 6; String "foo"]
    ; [Int 5; Int 6; String "bar"] ]

let run_test ?(params = []) layout_str =
  let module S =
    Serialize.Make (struct
        let layout_map_channel = None
      end)
      (E)
  in
  let module I =
    Irgen.Make (struct
        let code_only = true

        let debug = false
      end)
      (E)
      (S)
      ()
  in
  let sparams = Set.of_list (module Name.Compare_no_type) params in
  let layout =
    of_string_exn layout_str |> M.resolve ~params:sparams |> M.annotate_schema
    |> M.annotate_key_layouts
  in
  annotate_foreach layout ;
  M.annotate_subquery_types layout ;
  I.irgen ~params ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter

let run_test_db ?(params = []) layout_str =
  let module E = Eval.Make (struct
    let conn = new Postgresql.connection ~dbname:"demomatch" ~port:"5433" ()
  end) in
  let module M = Abslayout_db.Make (E) in
  let module S =
    Serialize.Make (struct
        let layout_map_channel = None
      end)
      (E)
  in
  let module I =
    Irgen.Make (struct
        let code_only = true

        let debug = false
      end)
      (E)
      (S)
      ()
  in
  let sparams = Set.of_list (module Name.Compare_no_type) params in
  let layout =
    of_string_exn layout_str |> M.resolve ~params:sparams |> M.annotate_schema
    |> M.annotate_key_layouts
  in
  annotate_foreach layout ;
  M.annotate_subquery_types layout ;
  I.irgen ~params ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter

let example_params =
  [ Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_p"
  ; Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_c" ]

let example_db_params =
  [ Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_p"
  ; Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_c" ]

let%expect_test "tuple-simple-cross" =
  run_test "ATuple([AScalar(1), AScalar(2)], cross)" ;
  [%expect
    {|
    // Locals:
    // cstart4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart3 = 0;
        cstart4 = cstart3 + 1;
        print(Tuple[Int[nonnull], Int[nonnull]],
        (buf[cstart3 : 1], buf[cstart4 : 1]));
    }
    // Locals:
    // cstart2 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 0;
        cstart2 = cstart1 + 1;
        c0 = c0 + 1;
        return c0;
    } |}]

let%expect_test "tuple-simple-zip" =
  run_test "ATuple([AScalar(1), AScalar(2)], zip)";
  [%expect {|
    // Locals:
    // start : Int[nonnull] (persists=true)
    fun zt_10 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // start : Int[nonnull] (persists=true)
    fun zt_9 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
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
    // cstart8 : Int[nonnull] (persists=true)
    // i13 : Int[nonnull] (persists=true)
    // tup12 : Tuple[Int[nonnull]] (persists=true)
    // tup11 : Tuple[Int[nonnull]] (persists=true)
    // count14 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart8 = 0;
        cstart8 = 0;
        init zt_9(cstart8);
        cstart8 = cstart8 + 1;
        init zt_10(cstart8);
        cstart8 = cstart8 + 1;
        i13 = 0;
        count14 = 1;
        loop (i13 < count14) {
            tup11 = next(zt_9);
            tup12 = next(zt_10);
            print(Tuple[Int[nonnull], Int[nonnull]], (tup11[0], tup12[0]));
            i13 = i13 + 1;
        }
    }
    // Locals:
    // tup4 : Tuple[Int[nonnull]] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // tup5 : Tuple[Int[nonnull]] (persists=true)
    // count7 : Int[nonnull] (persists=true)
    // i6 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 0;
        cstart1 = 0;
        init zt_2(cstart1);
        cstart1 = cstart1 + 1;
        init zt_3(cstart1);
        cstart1 = cstart1 + 1;
        i6 = 0;
        count7 = 1;
        loop (i6 < count7) {
            tup4 = next(zt_2);
            tup5 = next(zt_3);
            c0 = c0 + 1;
            i6 = i6 + 1;
        }
        return c0;
    } |}]

let%expect_test "sum-complex" =
  run_test
    "Select([sum(r1.f) + 5, count() + sum(r1.f / 2)], AList(r1, \
     ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross)))" ;
  [%expect
    {|
    // Locals:
    // tup17 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // cstart25 : Int[nonnull] (persists=true)
    // cstart22 : Int[nonnull] (persists=true)
    // i23 : Int[nonnull] (persists=true)
    // count20 : Int[nonnull] (persists=false)
    // found_tup18 : Bool[nonnull] (persists=false)
    // sum19 : Int[nonnull] (persists=false)
    // count24 : Int[nonnull] (persists=true)
    // sum21 : Int[nonnull] (persists=false)
    // cstart26 : Int[nonnull] (persists=true)
    fun printer () : Void {
        found_tup18 = false;
        sum19 = 0;
        count20 = 0;
        sum21 = 0;
        cstart22 = 0;
        i23 = 0;
        count24 = 5;
        loop (i23 < count24) {
            cstart25 = cstart22;
            cstart26 = cstart25 + 1;
            tup17 = (buf[cstart25 : 1], buf[cstart26 : 1]);
            sum19 = sum19 + tup17[0];
            count20 = count20 + 1;
            sum21 = sum21 + tup17[0] / 2;
            found_tup18 = true;
            cstart22 = cstart22 + 2;
            i23 = i23 + 1;
        }
        if (found_tup18) {
            print(Tuple[Int[nonnull], Int[nonnull]],
            (sum19 + 5, count20 + sum21));
        } else {

        }
    }
    // Locals:
    // sum8 : Int[nonnull] (persists=false)
    // cstart9 : Int[nonnull] (persists=true)
    // cstart12 : Int[nonnull] (persists=true)
    // count11 : Int[nonnull] (persists=true)
    // tup4 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // cstart13 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // sum6 : Int[nonnull] (persists=false)
    // found_tup5 : Bool[nonnull] (persists=false)
    // i10 : Int[nonnull] (persists=true)
    // count7 : Int[nonnull] (persists=false)
    fun counter () : Int[nonnull] {
        c0 = 0;
        found_tup5 = false;
        sum6 = 0;
        count7 = 0;
        sum8 = 0;
        cstart9 = 0;
        i10 = 0;
        count11 = 5;
        loop (i10 < count11) {
            cstart12 = cstart9;
            cstart13 = cstart12 + 1;
            tup4 = (buf[cstart12 : 1], buf[cstart13 : 1]);
            sum6 = sum6 + tup4[0];
            count7 = count7 + 1;
            sum8 = sum8 + tup4[0] / 2;
            found_tup5 = true;
            cstart9 = cstart9 + 2;
            i10 = i10 + 1;
        }
        if (found_tup5) {
            c0 = c0 + 1;
        } else {

        }
        return c0;
    } |}]

let%expect_test "sum" =
  run_test
    "Select([sum(r1.f), count()], AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g - \
     r1.f)], cross)))" ;
  [%expect
    {|
    // Locals:
    // cstart18 : Int[nonnull] (persists=true)
    // cstart21 : Int[nonnull] (persists=true)
    // cstart22 : Int[nonnull] (persists=true)
    // found_tup15 : Bool[nonnull] (persists=false)
    // count20 : Int[nonnull] (persists=true)
    // sum16 : Int[nonnull] (persists=false)
    // count17 : Int[nonnull] (persists=false)
    // i19 : Int[nonnull] (persists=true)
    // tup14 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    fun printer () : Void {
        found_tup15 = false;
        sum16 = 0;
        count17 = 0;
        cstart18 = 0;
        i19 = 0;
        count20 = 5;
        loop (i19 < count20) {
            cstart21 = cstart18;
            cstart22 = cstart21 + 1;
            tup14 = (buf[cstart21 : 1], buf[cstart22 : 1]);
            sum16 = sum16 + tup14[0];
            count17 = count17 + 1;
            found_tup15 = true;
            cstart18 = cstart18 + 2;
            i19 = i19 + 1;
        }
        if (found_tup15) {
            print(Tuple[Int[nonnull], Int[nonnull]], (sum16, count17));
        } else {

        }
    }
    // Locals:
    // cstart7 : Int[nonnull] (persists=true)
    // count9 : Int[nonnull] (persists=true)
    // tup3 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // count6 : Int[nonnull] (persists=false)
    // sum5 : Int[nonnull] (persists=false)
    // i8 : Int[nonnull] (persists=true)
    // cstart11 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // found_tup4 : Bool[nonnull] (persists=false)
    fun counter () : Int[nonnull] {
        c0 = 0;
        found_tup4 = false;
        sum5 = 0;
        count6 = 0;
        cstart7 = 0;
        i8 = 0;
        count9 = 5;
        loop (i8 < count9) {
            cstart10 = cstart7;
            cstart11 = cstart10 + 1;
            tup3 = (buf[cstart10 : 1], buf[cstart11 : 1]);
            sum5 = sum5 + tup3[0];
            count6 = count6 + 1;
            found_tup4 = true;
            cstart7 = cstart7 + 2;
            i8 = i8 + 1;
        }
        if (found_tup4) {
            c0 = c0 + 1;
        } else {

        }
        return c0;
    } |}]

let%expect_test "cross-tuple" =
  run_test "AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross))" ;
  [%expect
    {|
    // Locals:
    // count8 : Int[nonnull] (persists=true)
    // i7 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart6 = 0;
        i7 = 0;
        count8 = 5;
        loop (i7 < count8) {
            cstart9 = cstart6;
            cstart10 = cstart9 + 1;
            print(Tuple[Int[nonnull], Int[nonnull]],
            (buf[cstart9 : 1], buf[cstart10 : 1]));
            cstart6 = cstart6 + 2;
            i7 = i7 + 1;
        }
    }
    // Locals:
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // count3 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    // i2 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 0;
        i2 = 0;
        count3 = 5;
        loop (i2 < count3) {
            cstart4 = cstart1;
            cstart5 = cstart4 + 1;
            c0 = c0 + 1;
            cstart1 = cstart1 + 2;
            i2 = i2 + 1;
        }
        return c0;
    } |}]

let%expect_test "hash-idx" =
  run_test
    "ATuple([AList(r1, AScalar(r1.f)) as f, AHashIdx(dedup(select([r1.f], r1)) as \
     k, ascalar(k.f+1), f.f)], cross)" ;
  [%expect
    {|
    // Locals:
    // kstart14 : Int[nonnull] (persists=false)
    // cstart11 : Int[nonnull] (persists=true)
    // i12 : Int[nonnull] (persists=true)
    // key16 : Tuple[Int[nonnull]] (persists=false)
    // cstart9 : Int[nonnull] (persists=true)
    // vstart15 : Int[nonnull] (persists=false)
    // cstart10 : Int[nonnull] (persists=true)
    // count13 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart9 = 4;
        cstart10 = cstart9 + 5;
        cstart11 = cstart9;
        i12 = 0;
        count13 = 5;
        loop (i12 < count13) {
            if (hash(cstart10 + 4 + 8, buf[cstart11 : 1]) * 8 < 0 ||
                buf[cstart10 + 4 + 8 + buf[cstart10 + 4 : 8] : 8] - 1 <
                hash(cstart10 + 4 + 8, buf[cstart11 : 1]) * 8 || buf[cstart10 +
                4 + 8 + buf[cstart10 + 4 : 8] + 8 + hash(cstart10 + 4 +
                8, buf[cstart11 : 1]) * 8 : 8] = 0) {

            } else {
                 kstart14 = buf[cstart10 + 4 + 8 + buf[cstart10 + 4 : 8] + 8 +
                 hash(cstart10 + 4 + 8, buf[cstart11 : 1]) * 8 : 8];
                 key16 = (buf[kstart14 : 1]);
                 vstart15 = buf[cstart10 + 4 + 8 + buf[cstart10 + 4 : 8] + 8 +
                 hash(cstart10 + 4 + 8, buf[cstart11 : 1]) * 8 : 8] + 1;
                 if (true && key16[0] = buf[cstart11 : 1]) {
                     print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull]],
                     (buf[cstart11 : 1], key16[0], buf[vstart15 : 1]));
                 } else {

                 }
            }
            cstart11 = cstart11 + 1;
            i12 = i12 + 1;
        }
    }
    // Locals:
    // cstart2 : Int[nonnull] (persists=true)
    // count5 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // i4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // kstart6 : Int[nonnull] (persists=false)
    // key8 : Tuple[Int[nonnull]] (persists=false)
    // vstart7 : Int[nonnull] (persists=false)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 4;
        cstart2 = cstart1 + 5;
        cstart3 = cstart1;
        i4 = 0;
        count5 = 5;
        loop (i4 < count5) {
            if (hash(cstart2 + 4 + 8, buf[cstart3 : 1]) * 8 < 0 || buf[cstart2 +
                4 + 8 + buf[cstart2 + 4 : 8] : 8] - 1 < hash(cstart2 + 4 +
                8, buf[cstart3 : 1]) * 8 || buf[cstart2 + 4 + 8 + buf[cstart2 +
                4 : 8] + 8 + hash(cstart2 + 4 + 8, buf[cstart3 : 1]) * 8 : 8] = 0) {

            } else {
                 kstart6 = buf[cstart2 + 4 + 8 + buf[cstart2 + 4 : 8] + 8 +
                 hash(cstart2 + 4 + 8, buf[cstart3 : 1]) * 8 : 8];
                 key8 = (buf[kstart6 : 1]);
                 vstart7 = buf[cstart2 + 4 + 8 + buf[cstart2 + 4 : 8] + 8 +
                 hash(cstart2 + 4 + 8, buf[cstart3 : 1]) * 8 : 8] + 1;
                 if (true && key8[0] = buf[cstart3 : 1]) {
                     c0 = c0 + 1;
                 } else {

                 }
            }
            cstart3 = cstart3 + 1;
            i4 = i4 + 1;
        }
        return c0;
    } |}]

let%expect_test "ordered-idx" =
  run_test
    "ATuple([AList(r1, AScalar(r1.f)) as f, AOrderedIdx(dedup(select([r1.f], r1)) \
     as k, ascalar(k.f+1), f.f, f.f+1)], cross)" ;
  [%expect
    {|
    // Locals:
    // cstart18 : Int[nonnull] (persists=true)
    // key23 : Tuple[Int[nonnull]] (persists=true)
    // cstart16 : Int[nonnull] (persists=true)
    // vstart22 : Int[nonnull] (persists=true)
    // key28 : Tuple[Int[nonnull]] (persists=false)
    // low24 : Int[nonnull] (persists=true)
    // key27 : Tuple[Int[nonnull]] (persists=false)
    // high25 : Int[nonnull] (persists=true)
    // cstart17 : Int[nonnull] (persists=true)
    // count20 : Int[nonnull] (persists=true)
    // key29 : Tuple[Int[nonnull]] (persists=true)
    // kstart21 : Int[nonnull] (persists=true)
    // mid26 : Int[nonnull] (persists=true)
    // key30 : Tuple[Int[nonnull]] (persists=false)
    // i19 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart16 = 4;
        cstart17 = cstart16 + 5;
        cstart18 = cstart16;
        i19 = 0;
        count20 = 5;
        loop (i19 < count20) {
            low24 = 0;
            high25 = buf[cstart17 + 4 : 8] / 9;
            loop (low24 < high25) {
                mid26 = low24 + high25 / 2;
                kstart21 = cstart17 + 4 + 8 + mid26 * 9;
                key27 = (buf[kstart21 : 1]);
                if (key27[0] < buf[cstart18 : 1]) {
                    low24 = mid26 + 1;
                } else {
                     high25 = mid26;
                }
            }
            if (low24 < buf[cstart17 + 4 : 8] / 9) {
                kstart21 = cstart17 + 4 + 8 + low24 * 9;
                key28 = (buf[kstart21 : 1]);
                key29 = key28;
                loop (key29[0] < buf[cstart18 : 1] + 1 && low24 < buf[cstart17 +
                      4 : 8] / 9) {
                    vstart22 = buf[cstart17 + 4 + 8 + low24 * 9 + 1 : 8];
                    key23 = key29;
                    print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull]],
                    (buf[cstart18 : 1], key23[0], buf[vstart22 : 1]));
                    low24 = low24 + 1;
                    kstart21 = cstart17 + 4 + 8 + low24 * 9;
                    key30 = (buf[kstart21 : 1]);
                    key29 = key30;
                }
            } else {

            }
            cstart18 = cstart18 + 1;
            i19 = i19 + 1;
        }
    }
    // Locals:
    // key13 : Tuple[Int[nonnull]] (persists=false)
    // cstart2 : Int[nonnull] (persists=true)
    // mid11 : Int[nonnull] (persists=true)
    // key15 : Tuple[Int[nonnull]] (persists=false)
    // low9 : Int[nonnull] (persists=true)
    // count5 : Int[nonnull] (persists=true)
    // key14 : Tuple[Int[nonnull]] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // i4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // kstart6 : Int[nonnull] (persists=true)
    // high10 : Int[nonnull] (persists=true)
    // key8 : Tuple[Int[nonnull]] (persists=true)
    // key12 : Tuple[Int[nonnull]] (persists=false)
    // vstart7 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 4;
        cstart2 = cstart1 + 5;
        cstart3 = cstart1;
        i4 = 0;
        count5 = 5;
        loop (i4 < count5) {
            low9 = 0;
            high10 = buf[cstart2 + 4 : 8] / 9;
            loop (low9 < high10) {
                mid11 = low9 + high10 / 2;
                kstart6 = cstart2 + 4 + 8 + mid11 * 9;
                key12 = (buf[kstart6 : 1]);
                if (key12[0] < buf[cstart3 : 1]) {
                    low9 = mid11 + 1;
                } else {
                     high10 = mid11;
                }
            }
            if (low9 < buf[cstart2 + 4 : 8] / 9) {
                kstart6 = cstart2 + 4 + 8 + low9 * 9;
                key13 = (buf[kstart6 : 1]);
                key14 = key13;
                loop (key14[0] < buf[cstart3 : 1] + 1 && low9 < buf[cstart2 + 4 :
                      8] / 9) {
                    vstart7 = buf[cstart2 + 4 + 8 + low9 * 9 + 1 : 8];
                    key8 = key14;
                    c0 = c0 + 1;
                    low9 = low9 + 1;
                    kstart6 = cstart2 + 4 + 8 + low9 * 9;
                    key15 = (buf[kstart6 : 1]);
                    key14 = key15;
                }
            } else {

            }
            cstart3 = cstart3 + 1;
            i4 = i4 + 1;
        }
        return c0;
    } |}]

let%expect_test "example-1" =
  run_test ~params:example_params
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
    // Locals:
    // cstart18 : Int[nonnull] (persists=true)
    // cstart21 : Int[nonnull] (persists=true)
    // cstart16 : Int[nonnull] (persists=true)
    // cstart12 : Int[nonnull] (persists=true)
    // cstart22 : Int[nonnull] (persists=true)
    // cstart17 : Int[nonnull] (persists=true)
    // count14 : Int[nonnull] (persists=true)
    // cstart15 : Int[nonnull] (persists=true)
    // count20 : Int[nonnull] (persists=true)
    // i13 : Int[nonnull] (persists=true)
    // i19 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart12 = 1;
        i13 = 0;
        count14 = 3;
        loop (i13 < count14) {
            cstart15 = cstart12 + 1;
            cstart16 = cstart15 + 1;
            cstart17 = cstart16 + 1;
            cstart18 = cstart17 + 1 + 1;
            i19 = 0;
            count20 = buf[cstart17 : 1];
            loop (i19 < count20) {
                cstart21 = cstart18;
                cstart22 = cstart21 + 1;
                if (buf[cstart21 : 1] = id_c && buf[cstart15 : 1] = id_p) {
                    print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                    Int[nonnull]],
                    (buf[cstart15 : 1], buf[cstart16 : 1], buf[cstart21 : 1],
                     buf[cstart22 : 1]));
                } else {

                }
                cstart18 = cstart18 + 2;
                i19 = i19 + 1;
            }
            cstart12 = cstart12 + buf[cstart12 : 1];
            i13 = i13 + 1;
        }
    }
    // Locals:
    // cstart7 : Int[nonnull] (persists=true)
    // count9 : Int[nonnull] (persists=true)
    // i2 : Int[nonnull] (persists=true)
    // i8 : Int[nonnull] (persists=true)
    // cstart11 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // count3 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 1;
        i2 = 0;
        count3 = 3;
        loop (i2 < count3) {
            cstart4 = cstart1 + 1;
            cstart5 = cstart4 + 1;
            cstart6 = cstart5 + 1;
            cstart7 = cstart6 + 1 + 1;
            i8 = 0;
            count9 = buf[cstart6 : 1];
            loop (i8 < count9) {
                cstart10 = cstart7;
                cstart11 = cstart10 + 1;
                if (buf[cstart10 : 1] = id_c && buf[cstart4 : 1] = id_p) {
                    c0 = c0 + 1;
                } else {

                }
                cstart7 = cstart7 + 2;
                i8 = i8 + 1;
            }
            cstart1 = cstart1 + buf[cstart1 : 1];
            i2 = i2 + 1;
        }
        return c0;
    } |}]

(* let%expect_test "example-1-db" =
 *   run_test_db ~params:example_db_params
 *     {|
 * filter(lc.id = id_c && lp.id = id_p,
 * alist(filter(succ > counter + 1, log_bench) as lp,
 * atuple([ascalar(lp.id), ascalar(lp.counter),
 * alist(filter(lp.counter < log_bench.counter &&
 * log_bench.counter < lp.succ, log_bench) as lc,
 * atuple([ascalar(lc.id), ascalar(lc.counter)], cross))], cross)))
 * |};
 *   [%expect {|
 *     fun scalar_6 (start) {
 *         yield (load_str(start + 1, buf[start : 1]));
 *     }fun scalar_8 (lp_id,
 *          start) {
 *          yield (buf[start : 3]);
 *     }fun scalar_14 (lp_id,
 *          lp_counter,
 *          start) {
 *          yield (load_str(start + 1, buf[start : 1]));
 *     }fun scalar_16 (lp_id,
 *          lp_counter,
 *          lc_id,
 *          start) {
 *          yield (buf[start : 3]);
 *     }fun tuple_11 (lp_id,
 *          lp_counter,
 *          start) {
 *          cstart12 = start + 1;
 *          cstart13 = cstart12 + 1 + buf[cstart12 : 1];
 *          init scalar_14(lp_id, lp_counter, cstart12);
 *          tup15 = next(scalar_14);
 *          init scalar_16(lp_id, lp_counter, tup15[0], cstart13);
 *          tup17 = next(scalar_16);
 *          yield (tup15[0], tup17[0]);
 *     }fun list_10 (lp_id,
 *          lp_counter,
 *          start) {
 *          cstart = start + 1 + 1;
 *          pcount = buf[start : 1];
 *          loop (0 < pcount) {
 *              init tuple_11(lp_id, lp_counter, cstart);
 *              tup18 = next(tuple_11);
 *              yield tup18;
 *              cstart = cstart + buf[cstart : 1];
 *              pcount = pcount - 1;
 *          }
 *     }fun tuple_2 (start) {
 *          cstart3 = start + 2;
 *          cstart4 = cstart3 + 1 + buf[cstart3 : 1];
 *          cstart5 = cstart4 + 3;
 *          init scalar_6(cstart3);
 *          tup7 = next(scalar_6);
 *          init scalar_8(tup7[0], cstart4);
 *          tup9 = next(scalar_8);
 *          init list_10(tup7[0], tup9[0], cstart5);
 *          loop (not done(list_10)) {
 *              tup19 = next(list_10);
 *              if (not done(list_10)) {
 *                  yield (tup7[0], tup9[0], tup19[0], tup19[1]);
 *              } else {
 * 
 *              }
 *          }
 *     }fun list_1 () {
 *          cstart = 3;
 *          pcount = 404;
 *          loop (0 < pcount) {
 *              init tuple_2(cstart);
 *              loop (not done(tuple_2)) {
 *                  tup20 = next(tuple_2);
 *                  if (not done(tuple_2)) {
 *                      yield tup20;
 *                  } else {
 * 
 *                  }
 *              }
 *              cstart = cstart + buf[cstart : 2];
 *              pcount = pcount - 1;
 *          }
 *     }fun filter_0 () {
 *          init list_1();
 *          count22 = 404;
 *          loop (0 < count22) {
 *              tup21 = next(list_1);
 *              if (tup21[2] = id_c && tup21[0] = id_p) {
 *                  yield tup21;
 *              } else {
 * 
 *              }
 *              count22 = count22 - 1;
 *          }
 *     }fun printer () {
 *          init filter_0();
 *          loop (not done(filter_0)) {
 *              tup24 = next(filter_0);
 *              if (not done(filter_0)) {
 *                  print(Tuple[String[nonnull], Int[nonnull], String[nonnull],
 *                  Int[nonnull]], tup24);
 *              } else {
 * 
 *              }
 *          }
 *     }fun counter () {
 *          c = 0;
 *          init filter_0();
 *          loop (not done(filter_0)) {
 *              tup23 = next(filter_0);
 *              if (not done(filter_0)) {
 *                  c = c + 1;
 *              } else {
 * 
 *              }
 *          }
 *          return c;
 *     } |}] *)

let%expect_test "example-2" =
  run_test ~params:example_params
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
    // Locals:
    // key13 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // i17 : Int[nonnull] (persists=true)
    // cstart16 : Int[nonnull] (persists=true)
    // kstart11 : Int[nonnull] (persists=false)
    // cstart15 : Int[nonnull] (persists=true)
    // count18 : Int[nonnull] (persists=true)
    // vstart12 : Int[nonnull] (persists=false)
    // cstart14 : Int[nonnull] (persists=true)
    // cstart20 : Int[nonnull] (persists=true)
    // cstart19 : Int[nonnull] (persists=true)
    fun printer () : Void {
        if (<tuplehash> * 8 < 0 || buf[12 + buf[4 : 8] : 8] - 1 < <tuplehash> *
            8 || buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] = 0) {

        } else {
             kstart11 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8];
             cstart14 = kstart11;
             cstart15 = cstart14 + 1;
             key13 = (buf[cstart14 : 1], buf[cstart15 : 1]);
             vstart12 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] + 2;
             if (true && key13[0] = id_p && key13[1] = id_c) {
                 cstart16 = vstart12 + 1 + 1;
                 i17 = 0;
                 count18 = buf[vstart12 : 1];
                 loop (i17 < count18) {
                     cstart19 = cstart16;
                     cstart20 = cstart19 + 1;
                     print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                     Int[nonnull]],
                     (key13[0], key13[1], buf[cstart19 : 1], buf[cstart20 : 1]));
                     cstart16 = cstart16 + 2;
                     i17 = i17 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // count8 : Int[nonnull] (persists=true)
    // i7 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // vstart2 : Int[nonnull] (persists=false)
    // key3 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // c0 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // kstart1 : Int[nonnull] (persists=false)
    // cstart10 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        if (<tuplehash> * 8 < 0 || buf[12 + buf[4 : 8] : 8] - 1 < <tuplehash> *
            8 || buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] = 0) {

        } else {
             kstart1 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8];
             cstart4 = kstart1;
             cstart5 = cstart4 + 1;
             key3 = (buf[cstart4 : 1], buf[cstart5 : 1]);
             vstart2 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] + 2;
             if (true && key3[0] = id_p && key3[1] = id_c) {
                 cstart6 = vstart2 + 1 + 1;
                 i7 = 0;
                 count8 = buf[vstart2 : 1];
                 loop (i7 < count8) {
                     cstart9 = cstart6;
                     cstart10 = cstart9 + 1;
                     c0 = c0 + 1;
                     cstart6 = cstart6 + 2;
                     i7 = i7 + 1;
                 }
             } else {

             }
        }
        return c0;
    } |}]

(* let%expect_test "example-2-db" =
 *   run_test_db ~params:example_db_params
 *     {|
 * ahashidx(dedup(
 *       join(true, select([id as lp_k], log_bench), select([id as lc_k], log_bench))),
 *   alist(select([counter_lp, counter_lc], 
 *     join(counter_lp < counter_lc && 
 *          counter_lc < succ_lp, 
 *       filter(id_lp = lp_k,
 *         select([id as id_lp, counter as counter_lp, succ as succ_lp], log_bench)), 
 *       filter(id_lc = lc_k,
 *         select([id as id_lc, counter as counter_lc], log_bench)))),
 *     atuple([ascalar(counter_lp), ascalar(counter_lc)], cross)),
 *   (id_p, id_c))
 * |} ;
 *   [%expect
 *     {|
 *     fun scalar_4 (start) {
 *         yield (load_str(start + 1, buf[start : 1]));
 *     }fun scalar_6 (start,
 *          lp_k) {
 *          yield (load_str(start + 1, buf[start : 1]));
 *     }fun tuple_1 (start) {
 *          cstart2 = start + 1;
 *          cstart3 = cstart2 + 1 + buf[cstart2 : 1];
 *          init scalar_4(cstart2);
 *          tup5 = next(scalar_4);
 *          init scalar_6(cstart3, tup5[0]);
 *          tup7 = next(scalar_6);
 *          yield (tup5[0], tup7[0]);
 *     }fun scalar_12 (start,
 *          lp_k,
 *          lc_k) {
 *          yield (buf[start : 3]);
 *     }fun scalar_14 (start,
 *          lp_k,
 *          lc_k,
 *          counter_lp) {
 *          yield (buf[start : 3]);
 *     }fun tuple_9 (start,
 *          lp_k,
 *          lc_k) {
 *          cstart10 = start;
 *          cstart11 = cstart10 + 3;
 *          init scalar_12(cstart10, lp_k, lc_k);
 *          tup13 = next(scalar_12);
 *          init scalar_14(cstart11, lp_k, lc_k, tup13[0]);
 *          tup15 = next(scalar_14);
 *          yield (tup13[0], tup15[0]);
 *     }fun list_8 (start,
 *          lp_k,
 *          lc_k) {
 *          cstart = start;
 *          pcount = 1;
 *          loop (0 < pcount) {
 *              init tuple_9(cstart, lp_k, lc_k);
 *              tup16 = next(tuple_9);
 *              yield tup16;
 *              cstart = cstart + 6;
 *              pcount = pcount - 1;
 *          }
 *     }fun hash_idx_0 () {
 *          if (hash(11, (id_p, id_c)) * 8 < 0 || 11 + buf[3 : 8] - 1 <
 *              hash(11, (id_p, id_c)) * 8 || buf[11 + buf[3 : 8] + 8 +
 *              hash(11, (id_p, id_c)) * 8 : 8] = 0) {
 * 
 *          } else {
 *               kstart = buf[11 + buf[3 : 8] + 8 + hash(11, (id_p, id_c)) * 8 : 8];
 *               init tuple_1(kstart);
 *               key = next(tuple_1);
 *               vstart = buf[11 + buf[3 : 8] + 8 + hash(11, (id_p, id_c)) * 8 :
 *               8] + buf[buf[11 + buf[3 : 8] + 8 + hash(11, (id_p, id_c)) * 8 :
 *               8] : 1];
 *               if (true && key[0] = id_p && key[1] = id_c) {
 *                   init list_8(vstart, key[0], key[1]);
 *                   tup17 = next(list_8);
 *                   yield (key[0], key[1], tup17[0], tup17[1]);
 *               } else {
 * 
 *               }
 *          }
 *     }fun printer () {
 *          init hash_idx_0();
 *          loop (not done(hash_idx_0)) {
 *              tup19 = next(hash_idx_0);
 *              if (not done(hash_idx_0)) {
 *                  print(Tuple[String[nonnull], String[nonnull], Int[nonnull],
 *                  Int[nonnull]], tup19);
 *              } else {
 * 
 *              }
 *          }
 *     }fun counter () {
 *          c = 0;
 *          init hash_idx_0();
 *          loop (not done(hash_idx_0)) {
 *              tup18 = next(hash_idx_0);
 *              if (not done(hash_idx_0)) {
 *                  c = c + 1;
 *              } else {
 * 
 *              }
 *          }
 *          return c;
 *     } |}] *)

let%expect_test "example-3" =
  run_test ~params:example_params
    {|
select([lp.counter, lc.counter],
  atuple([ahashidx(select([id as k], log), 
    alist(select([counter, succ], 
        filter(k = id && counter < succ, log)), 
      atuple([ascalar(counter), ascalar(succ)], cross)), 
    id_p) as lp,
  filter(lc.id = id_c,
    aorderedidx(select([log.counter as k], log), 
      alist(filter(log.counter = k, log),
        atuple([ascalar(log.id), ascalar(log.counter)], cross)), 
      lp.counter, lp.succ) as lc)], cross))
|} ;
  [%expect
    {|
    // Locals:
    // key44 : Tuple[Int[nonnull]] (persists=true)
    // vstart29 : Int[nonnull] (persists=false)
    // cstart35 : Int[nonnull] (persists=true)
    // count47 : Int[nonnull] (persists=true)
    // high40 : Int[nonnull] (persists=true)
    // key50 : Tuple[Int[nonnull]] (persists=false)
    // cstart34 : Int[nonnull] (persists=true)
    // vstart37 : Int[nonnull] (persists=true)
    // key42 : Tuple[Int[nonnull]] (persists=false)
    // cstart48 : Int[nonnull] (persists=true)
    // cstart31 : Int[nonnull] (persists=true)
    // kstart28 : Int[nonnull] (persists=false)
    // low39 : Int[nonnull] (persists=true)
    // key30 : Tuple[Int[nonnull]] (persists=false)
    // i32 : Int[nonnull] (persists=true)
    // key38 : Tuple[Int[nonnull]] (persists=true)
    // mid41 : Int[nonnull] (persists=true)
    // key43 : Tuple[Int[nonnull]] (persists=false)
    // count33 : Int[nonnull] (persists=true)
    // cstart49 : Int[nonnull] (persists=true)
    // i46 : Int[nonnull] (persists=true)
    // cstart45 : Int[nonnull] (persists=true)
    // cstart27 : Int[nonnull] (persists=true)
    // kstart36 : Int[nonnull] (persists=true)
    // cstart26 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart26 = 4;
        cstart27 = cstart26 + buf[cstart26 : 4];
        if (hash(cstart26 + 4 + 8, id_p) * 8 < 0 || buf[cstart26 + 4 + 8 +
            buf[cstart26 + 4 : 8] : 8] - 1 < hash(cstart26 + 4 + 8, id_p) * 8 ||
            buf[cstart26 + 4 + 8 + buf[cstart26 + 4 : 8] + 8 + hash(cstart26 +
            4 + 8, id_p) * 8 : 8] = 0) {

        } else {
             kstart28 = buf[cstart26 + 4 + 8 + buf[cstart26 + 4 : 8] + 8 +
             hash(cstart26 + 4 + 8, id_p) * 8 : 8];
             key30 = (buf[kstart28 : 1]);
             vstart29 = buf[cstart26 + 4 + 8 + buf[cstart26 + 4 : 8] + 8 +
             hash(cstart26 + 4 + 8, id_p) * 8 : 8] + 1;
             if (true && key30[0] = id_p) {
                 cstart31 = vstart29;
                 i32 = 0;
                 count33 = 1;
                 loop (i32 < count33) {
                     cstart34 = cstart31;
                     cstart35 = cstart34 + 1;
                     low39 = 0;
                     high40 = buf[cstart27 + 4 : 8] / 9;
                     loop (low39 < high40) {
                         mid41 = low39 + high40 / 2;
                         kstart36 = cstart27 + 4 + 8 + mid41 * 9;
                         key42 = (buf[kstart36 : 1]);
                         if (key42[0] < buf[cstart34 : 1]) {
                             low39 = mid41 + 1;
                         } else {
                              high40 = mid41;
                         }
                     }
                     if (low39 < buf[cstart27 + 4 : 8] / 9) {
                         kstart36 = cstart27 + 4 + 8 + low39 * 9;
                         key43 = (buf[kstart36 : 1]);
                         key44 = key43;
                         loop (key44[0] < buf[cstart35 : 1] && low39 <
                               buf[cstart27 + 4 : 8] / 9) {
                             vstart37 = buf[cstart27 + 4 + 8 + low39 * 9 + 1 :
                             8];
                             key38 = key44;
                             cstart45 = vstart37 + 1 + 1;
                             i46 = 0;
                             count47 = buf[vstart37 : 1];
                             loop (i46 < count47) {
                                 cstart48 = cstart45;
                                 cstart49 = cstart48 + 1;
                                 if (buf[cstart48 : 1] = id_c) {
                                     print(Tuple[Int[nonnull], Int[nonnull]],
                                     (buf[cstart34 : 1], buf[cstart49 : 1]));
                                 } else {

                                 }
                                 cstart45 = cstart45 + 2;
                                 i46 = i46 + 1;
                             }
                             low39 = low39 + 1;
                             kstart36 = cstart27 + 4 + 8 + low39 * 9;
                             key50 = (buf[kstart36 : 1]);
                             key44 = key50;
                         }
                     } else {

                     }
                     cstart31 = cstart31 + 2;
                     i32 = i32 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // cstart24 : Int[nonnull] (persists=true)
    // key13 : Tuple[Int[nonnull]] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    // vstart4 : Int[nonnull] (persists=false)
    // i7 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // key19 : Tuple[Int[nonnull]] (persists=true)
    // key17 : Tuple[Int[nonnull]] (persists=false)
    // mid16 : Int[nonnull] (persists=true)
    // key25 : Tuple[Int[nonnull]] (persists=false)
    // count22 : Int[nonnull] (persists=true)
    // i21 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // cstart23 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // low14 : Int[nonnull] (persists=true)
    // count8 : Int[nonnull] (persists=true)
    // kstart11 : Int[nonnull] (persists=true)
    // kstart3 : Int[nonnull] (persists=false)
    // key5 : Tuple[Int[nonnull]] (persists=false)
    // vstart12 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // high15 : Int[nonnull] (persists=true)
    // key18 : Tuple[Int[nonnull]] (persists=false)
    // cstart20 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 4;
        cstart2 = cstart1 + buf[cstart1 : 4];
        if (hash(cstart1 + 4 + 8, id_p) * 8 < 0 || buf[cstart1 + 4 + 8 +
            buf[cstart1 + 4 : 8] : 8] - 1 < hash(cstart1 + 4 + 8, id_p) * 8 ||
            buf[cstart1 + 4 + 8 + buf[cstart1 + 4 : 8] + 8 + hash(cstart1 + 4 +
            8, id_p) * 8 : 8] = 0) {

        } else {
             kstart3 = buf[cstart1 + 4 + 8 + buf[cstart1 + 4 : 8] + 8 +
             hash(cstart1 + 4 + 8, id_p) * 8 : 8];
             key5 = (buf[kstart3 : 1]);
             vstart4 = buf[cstart1 + 4 + 8 + buf[cstart1 + 4 : 8] + 8 +
             hash(cstart1 + 4 + 8, id_p) * 8 : 8] + 1;
             if (true && key5[0] = id_p) {
                 cstart6 = vstart4;
                 i7 = 0;
                 count8 = 1;
                 loop (i7 < count8) {
                     cstart9 = cstart6;
                     cstart10 = cstart9 + 1;
                     low14 = 0;
                     high15 = buf[cstart2 + 4 : 8] / 9;
                     loop (low14 < high15) {
                         mid16 = low14 + high15 / 2;
                         kstart11 = cstart2 + 4 + 8 + mid16 * 9;
                         key17 = (buf[kstart11 : 1]);
                         if (key17[0] < buf[cstart9 : 1]) {
                             low14 = mid16 + 1;
                         } else {
                              high15 = mid16;
                         }
                     }
                     if (low14 < buf[cstart2 + 4 : 8] / 9) {
                         kstart11 = cstart2 + 4 + 8 + low14 * 9;
                         key18 = (buf[kstart11 : 1]);
                         key19 = key18;
                         loop (key19[0] < buf[cstart10 : 1] && low14 <
                               buf[cstart2 + 4 : 8] / 9) {
                             vstart12 = buf[cstart2 + 4 + 8 + low14 * 9 + 1 : 8];
                             key13 = key19;
                             cstart20 = vstart12 + 1 + 1;
                             i21 = 0;
                             count22 = buf[vstart12 : 1];
                             loop (i21 < count22) {
                                 cstart23 = cstart20;
                                 cstart24 = cstart23 + 1;
                                 if (buf[cstart23 : 1] = id_c) {
                                     c0 = c0 + 1;
                                 } else {

                                 }
                                 cstart20 = cstart20 + 2;
                                 i21 = i21 + 1;
                             }
                             low14 = low14 + 1;
                             kstart11 = cstart2 + 4 + 8 + low14 * 9;
                             key25 = (buf[kstart11 : 1]);
                             key19 = key25;
                         }
                     } else {

                     }
                     cstart6 = cstart6 + 2;
                     i7 = i7 + 1;
                 }
             } else {

             }
        }
        return c0;
    } |}]

let%expect_test "subquery-first" =
  run_test ~params:example_params
    {|
    select([log.id], filter((select([min(l.counter)],
 alist(log as l, ascalar(l.counter))))=log.id, alist(log, ascalar(log.id))))
|} ;
  [%expect
    {|
    // Locals:
    // tup17 : Tuple[Int[nonnull]] (persists=false)
    // min19 : Int[nonnull] (persists=false)
    // cstart12 : Int[nonnull] (persists=true)
    // count14 : Int[nonnull] (persists=true)
    // count22 : Int[nonnull] (persists=true)
    // i21 : Int[nonnull] (persists=true)
    // i13 : Int[nonnull] (persists=true)
    // found_tup18 : Bool[nonnull] (persists=false)
    // first15 : Int[nonnull] (persists=true)
    // cstart20 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart12 = 0;
        i13 = 0;
        count14 = 5;
        loop (i13 < count14) {
            found_tup18 = false;
            min19 = 4611686018427387903;
            cstart20 = 0;
            i21 = 0;
            count22 = 5;
            loop (i21 < count22) {
                tup17 = (buf[cstart20 : 1]);
                min19 = tup17[0] < min19 ? tup17[0] : min19;
                found_tup18 = true;
                cstart20 = cstart20 + 1;
                i21 = i21 + 1;
            }
            if (found_tup18) {
                first15 = min19;
            } else {

            }
            if (first15 = buf[cstart12 : 1]) {
                print(Tuple[Int[nonnull]], (buf[cstart12 : 1]));
            } else {

            }
            cstart12 = cstart12 + 1;
            i13 = i13 + 1;
        }
    }
    // Locals:
    // cstart9 : Int[nonnull] (persists=true)
    // min8 : Int[nonnull] (persists=false)
    // i2 : Int[nonnull] (persists=true)
    // count11 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // tup6 : Tuple[Int[nonnull]] (persists=false)
    // first4 : Int[nonnull] (persists=true)
    // count3 : Int[nonnull] (persists=true)
    // i10 : Int[nonnull] (persists=true)
    // found_tup7 : Bool[nonnull] (persists=false)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 0;
        i2 = 0;
        count3 = 5;
        loop (i2 < count3) {
            found_tup7 = false;
            min8 = 4611686018427387903;
            cstart9 = 0;
            i10 = 0;
            count11 = 5;
            loop (i10 < count11) {
                tup6 = (buf[cstart9 : 1]);
                min8 = tup6[0] < min8 ? tup6[0] : min8;
                found_tup7 = true;
                cstart9 = cstart9 + 1;
                i10 = i10 + 1;
            }
            if (found_tup7) {
                first4 = min8;
            } else {

            }
            if (first4 = buf[cstart1 : 1]) {
                c0 = c0 + 1;
            } else {

            }
            cstart1 = cstart1 + 1;
            i2 = i2 + 1;
        }
        return c0;
    } |}]

let%expect_test "example-3-str" =
  run_test ~params:example_db_params
    {|
select([lp.counter, lc.counter],
  atuple([ahashidx(dedup(select([id as k], log_str)), 
    alist(select([counter, succ], 
        filter(k = id && counter < succ, log_str)), 
      atuple([ascalar(counter), ascalar(succ)], cross)), 
    id_p) as lp,
  filter(lc.id = id_c,
    aorderedidx(select([log_str.counter as k], log_str), 
      alist(filter(log_str.counter = k, log_str),
        atuple([ascalar(log_str.id), ascalar(log_str.counter)], cross)), 
      lp.counter, lp.succ) as lc)], cross))
|} ;
  [%expect
    {|
    // Locals:
    // key44 : Tuple[Int[nonnull]] (persists=true)
    // vstart29 : Int[nonnull] (persists=false)
    // cstart35 : Int[nonnull] (persists=true)
    // count47 : Int[nonnull] (persists=true)
    // high40 : Int[nonnull] (persists=true)
    // key50 : Tuple[Int[nonnull]] (persists=false)
    // cstart34 : Int[nonnull] (persists=true)
    // vstart37 : Int[nonnull] (persists=true)
    // key42 : Tuple[Int[nonnull]] (persists=false)
    // cstart48 : Int[nonnull] (persists=true)
    // cstart31 : Int[nonnull] (persists=true)
    // kstart28 : Int[nonnull] (persists=false)
    // low39 : Int[nonnull] (persists=true)
    // key30 : Tuple[String[nonnull]] (persists=false)
    // i32 : Int[nonnull] (persists=true)
    // key38 : Tuple[Int[nonnull]] (persists=true)
    // mid41 : Int[nonnull] (persists=true)
    // key43 : Tuple[Int[nonnull]] (persists=false)
    // count33 : Int[nonnull] (persists=true)
    // cstart49 : Int[nonnull] (persists=true)
    // i46 : Int[nonnull] (persists=true)
    // cstart45 : Int[nonnull] (persists=true)
    // cstart27 : Int[nonnull] (persists=true)
    // kstart36 : Int[nonnull] (persists=true)
    // cstart26 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart26 = 4;
        cstart27 = cstart26 + buf[cstart26 : 4];
        if (hash(cstart26 + 4 + 8, id_p) * 8 < 0 || buf[cstart26 + 4 + 8 +
            buf[cstart26 + 4 : 8] : 8] - 1 < hash(cstart26 + 4 + 8, id_p) * 8 ||
            buf[cstart26 + 4 + 8 + buf[cstart26 + 4 : 8] + 8 + hash(cstart26 +
            4 + 8, id_p) * 8 : 8] = 0) {

        } else {
             kstart28 = buf[cstart26 + 4 + 8 + buf[cstart26 + 4 : 8] + 8 +
             hash(cstart26 + 4 + 8, id_p) * 8 : 8];
             key30 = (load_str(kstart28 + 1, buf[kstart28 : 1]));
             vstart29 = buf[cstart26 + 4 + 8 + buf[cstart26 + 4 : 8] + 8 +
             hash(cstart26 + 4 + 8, id_p) * 8 : 8] + 1 + buf[buf[cstart26 + 4 +
             8 + buf[cstart26 + 4 : 8] + 8 + hash(cstart26 + 4 + 8, id_p) * 8 :
             8] : 1];
             if (true && key30[0] = id_p) {
                 cstart31 = vstart29 + 1 + 1;
                 i32 = 0;
                 count33 = buf[vstart29 : 1];
                 loop (i32 < count33) {
                     cstart34 = cstart31;
                     cstart35 = cstart34 + 1;
                     low39 = 0;
                     high40 = buf[cstart27 + 4 : 8] / 9;
                     loop (low39 < high40) {
                         mid41 = low39 + high40 / 2;
                         kstart36 = cstart27 + 4 + 8 + mid41 * 9;
                         key42 = (buf[kstart36 : 1]);
                         if (key42[0] < buf[cstart34 : 1]) {
                             low39 = mid41 + 1;
                         } else {
                              high40 = mid41;
                         }
                     }
                     if (low39 < buf[cstart27 + 4 : 8] / 9) {
                         kstart36 = cstart27 + 4 + 8 + low39 * 9;
                         key43 = (buf[kstart36 : 1]);
                         key44 = key43;
                         loop (key44[0] < buf[cstart35 : 1] && low39 <
                               buf[cstart27 + 4 : 8] / 9) {
                             vstart37 = buf[cstart27 + 4 + 8 + low39 * 9 + 1 :
                             8];
                             key38 = key44;
                             cstart45 = vstart37 + 1;
                             i46 = 0;
                             count47 = 1;
                             loop (i46 < count47) {
                                 cstart48 = cstart45 + 1;
                                 cstart49 = cstart48 + 1 + buf[cstart48 : 1];
                                 if (load_str(cstart48 + 1, buf[cstart48 : 1]) =
                                     id_c) {
                                     print(Tuple[Int[nonnull], Int[nonnull]],
                                     (buf[cstart34 : 1], buf[cstart49 : 1]));
                                 } else {

                                 }
                                 cstart45 = cstart45 + buf[cstart45 : 1];
                                 i46 = i46 + 1;
                             }
                             low39 = low39 + 1;
                             kstart36 = cstart27 + 4 + 8 + low39 * 9;
                             key50 = (buf[kstart36 : 1]);
                             key44 = key50;
                         }
                     } else {

                     }
                     cstart31 = cstart31 + 2;
                     i32 = i32 + 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // cstart24 : Int[nonnull] (persists=true)
    // key13 : Tuple[Int[nonnull]] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    // vstart4 : Int[nonnull] (persists=false)
    // i7 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // key19 : Tuple[Int[nonnull]] (persists=true)
    // key17 : Tuple[Int[nonnull]] (persists=false)
    // mid16 : Int[nonnull] (persists=true)
    // key25 : Tuple[Int[nonnull]] (persists=false)
    // count22 : Int[nonnull] (persists=true)
    // i21 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // cstart23 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // low14 : Int[nonnull] (persists=true)
    // count8 : Int[nonnull] (persists=true)
    // kstart11 : Int[nonnull] (persists=true)
    // kstart3 : Int[nonnull] (persists=false)
    // key5 : Tuple[String[nonnull]] (persists=false)
    // vstart12 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // high15 : Int[nonnull] (persists=true)
    // key18 : Tuple[Int[nonnull]] (persists=false)
    // cstart20 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 4;
        cstart2 = cstart1 + buf[cstart1 : 4];
        if (hash(cstart1 + 4 + 8, id_p) * 8 < 0 || buf[cstart1 + 4 + 8 +
            buf[cstart1 + 4 : 8] : 8] - 1 < hash(cstart1 + 4 + 8, id_p) * 8 ||
            buf[cstart1 + 4 + 8 + buf[cstart1 + 4 : 8] + 8 + hash(cstart1 + 4 +
            8, id_p) * 8 : 8] = 0) {

        } else {
             kstart3 = buf[cstart1 + 4 + 8 + buf[cstart1 + 4 : 8] + 8 +
             hash(cstart1 + 4 + 8, id_p) * 8 : 8];
             key5 = (load_str(kstart3 + 1, buf[kstart3 : 1]));
             vstart4 = buf[cstart1 + 4 + 8 + buf[cstart1 + 4 : 8] + 8 +
             hash(cstart1 + 4 + 8, id_p) * 8 : 8] + 1 + buf[buf[cstart1 + 4 + 8 +
             buf[cstart1 + 4 : 8] + 8 + hash(cstart1 + 4 + 8, id_p) * 8 : 8] :
             1];
             if (true && key5[0] = id_p) {
                 cstart6 = vstart4 + 1 + 1;
                 i7 = 0;
                 count8 = buf[vstart4 : 1];
                 loop (i7 < count8) {
                     cstart9 = cstart6;
                     cstart10 = cstart9 + 1;
                     low14 = 0;
                     high15 = buf[cstart2 + 4 : 8] / 9;
                     loop (low14 < high15) {
                         mid16 = low14 + high15 / 2;
                         kstart11 = cstart2 + 4 + 8 + mid16 * 9;
                         key17 = (buf[kstart11 : 1]);
                         if (key17[0] < buf[cstart9 : 1]) {
                             low14 = mid16 + 1;
                         } else {
                              high15 = mid16;
                         }
                     }
                     if (low14 < buf[cstart2 + 4 : 8] / 9) {
                         kstart11 = cstart2 + 4 + 8 + low14 * 9;
                         key18 = (buf[kstart11 : 1]);
                         key19 = key18;
                         loop (key19[0] < buf[cstart10 : 1] && low14 <
                               buf[cstart2 + 4 : 8] / 9) {
                             vstart12 = buf[cstart2 + 4 + 8 + low14 * 9 + 1 : 8];
                             key13 = key19;
                             cstart20 = vstart12 + 1;
                             i21 = 0;
                             count22 = 1;
                             loop (i21 < count22) {
                                 cstart23 = cstart20 + 1;
                                 cstart24 = cstart23 + 1 + buf[cstart23 : 1];
                                 if (load_str(cstart23 + 1, buf[cstart23 : 1]) =
                                     id_c) {
                                     c0 = c0 + 1;
                                 } else {

                                 }
                                 cstart20 = cstart20 + buf[cstart20 : 1];
                                 i21 = i21 + 1;
                             }
                             low14 = low14 + 1;
                             kstart11 = cstart2 + 4 + 8 + low14 * 9;
                             key25 = (buf[kstart11 : 1]);
                             key19 = key25;
                         }
                     } else {

                     }
                     cstart6 = cstart6 + 2;
                     i7 = i7 + 1;
                 }
             } else {

             }
        }
        return c0;
    } |}]
