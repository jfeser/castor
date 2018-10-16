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

let%expect_test "tuple-simple" =
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
        (buf[cstart4 : 1], buf[cstart3 : 1]));
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

let%expect_test "sum-complex" =
  run_test
    "Select([sum(r1.f) + 5, count() + sum(r1.f / 2)], AList(r1, \
     ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross)))" ;
  [%expect
    {|
    // Locals:
    // found_tup17 : Bool[nonnull] (persists=false)
    // cstart24 : Int[nonnull] (persists=true)
    // cstart21 : Int[nonnull] (persists=true)
    // tup16 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // sum20 : Int[nonnull] (persists=false)
    // sum18 : Int[nonnull] (persists=false)
    // pcount22 : Int[nonnull] (persists=true)
    // cstart23 : Int[nonnull] (persists=true)
    // count19 : Int[nonnull] (persists=false)
    fun printer () : Void {
        found_tup17 = false;
        sum18 = 0;
        count19 = 0;
        sum20 = 0;
        cstart21 = 0;
        pcount22 = 5;
        loop (0 < pcount22) {
            cstart23 = cstart21;
            cstart24 = cstart23 + 1;
            tup16 = (buf[cstart24 : 1], buf[cstart23 : 1]);
            sum18 = sum18 + tup16[0];
            count19 = count19 + 1;
            sum20 = sum20 + tup16[0] / 2;
            found_tup17 = true;
            cstart21 = cstart21 + 2;
            pcount22 = pcount22 - 1;
        }
        if (found_tup17) {
            print(Tuple[Int[nonnull], Int[nonnull]],
            (sum18 + 5, count19 + sum20));
        } else {

        }
    }
    // Locals:
    // sum8 : Int[nonnull] (persists=false)
    // pcount10 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // cstart12 : Int[nonnull] (persists=true)
    // cstart11 : Int[nonnull] (persists=true)
    // tup4 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // c0 : Int[nonnull] (persists=true)
    // sum6 : Int[nonnull] (persists=false)
    // found_tup5 : Bool[nonnull] (persists=false)
    // count7 : Int[nonnull] (persists=false)
    fun counter () : Int[nonnull] {
        c0 = 0;
        found_tup5 = false;
        sum6 = 0;
        count7 = 0;
        sum8 = 0;
        cstart9 = 0;
        pcount10 = 5;
        loop (0 < pcount10) {
            cstart11 = cstart9;
            cstart12 = cstart11 + 1;
            tup4 = (buf[cstart12 : 1], buf[cstart11 : 1]);
            sum6 = sum6 + tup4[0];
            count7 = count7 + 1;
            sum8 = sum8 + tup4[0] / 2;
            found_tup5 = true;
            cstart9 = cstart9 + 2;
            pcount10 = pcount10 - 1;
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
    // sum15 : Int[nonnull] (persists=false)
    // count16 : Int[nonnull] (persists=false)
    // found_tup14 : Bool[nonnull] (persists=false)
    // pcount18 : Int[nonnull] (persists=true)
    // cstart17 : Int[nonnull] (persists=true)
    // tup13 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // cstart20 : Int[nonnull] (persists=true)
    // cstart19 : Int[nonnull] (persists=true)
    fun printer () : Void {
        found_tup14 = false;
        sum15 = 0;
        count16 = 0;
        cstart17 = 0;
        pcount18 = 5;
        loop (0 < pcount18) {
            cstart19 = cstart17;
            cstart20 = cstart19 + 1;
            tup13 = (buf[cstart20 : 1], buf[cstart19 : 1]);
            sum15 = sum15 + tup13[0];
            count16 = count16 + 1;
            found_tup14 = true;
            cstart17 = cstart17 + 2;
            pcount18 = pcount18 - 1;
        }
        if (found_tup14) {
            print(Tuple[Int[nonnull], Int[nonnull]], (sum15, count16));
        } else {

        }
    }
    // Locals:
    // cstart7 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // tup3 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // count6 : Int[nonnull] (persists=false)
    // sum5 : Int[nonnull] (persists=false)
    // c0 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // found_tup4 : Bool[nonnull] (persists=false)
    // pcount8 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        found_tup4 = false;
        sum5 = 0;
        count6 = 0;
        cstart7 = 0;
        pcount8 = 5;
        loop (0 < pcount8) {
            cstart9 = cstart7;
            cstart10 = cstart9 + 1;
            tup3 = (buf[cstart10 : 1], buf[cstart9 : 1]);
            sum5 = sum5 + tup3[0];
            count6 = count6 + 1;
            found_tup4 = true;
            cstart7 = cstart7 + 2;
            pcount8 = pcount8 - 1;
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
    // cstart8 : Int[nonnull] (persists=true)
    // cstart7 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    // pcount6 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart5 = 0;
        pcount6 = 5;
        loop (0 < pcount6) {
            cstart7 = cstart5;
            cstart8 = cstart7 + 1;
            print(Tuple[Int[nonnull], Int[nonnull]],
            (buf[cstart8 : 1], buf[cstart7 : 1]));
            cstart5 = cstart5 + 2;
            pcount6 = pcount6 - 1;
        }
    }
    // Locals:
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // pcount2 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 0;
        pcount2 = 5;
        loop (0 < pcount2) {
            cstart3 = cstart1;
            cstart4 = cstart3 + 1;
            c0 = c0 + 1;
            cstart1 = cstart1 + 2;
            pcount2 = pcount2 - 1;
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
    // cstart8 : Int[nonnull] (persists=true)
    // key14 : Tuple[Int[nonnull]] (persists=false)
    // cstart9 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // pcount11 : Int[nonnull] (persists=true)
    // vstart13 : Int[nonnull] (persists=false)
    // kstart12 : Int[nonnull] (persists=false)
    fun printer () : Void {
        cstart8 = 4;
        cstart9 = cstart8 + 5;
        cstart10 = cstart8;
        pcount11 = 5;
        loop (0 < pcount11) {
            if (hash(cstart9 + 4 + 8, buf[cstart10 : 1]) * 8 < 0 || buf[cstart9 +
                4 + 8 + buf[cstart9 + 4 : 8] : 8] - 1 < hash(cstart9 + 4 +
                8, buf[cstart10 : 1]) * 8 || buf[cstart9 + 4 + 8 + buf[cstart9 +
                4 : 8] + 8 + hash(cstart9 + 4 + 8, buf[cstart10 : 1]) * 8 : 8] =
                0) {

            } else {
                 kstart12 = buf[cstart9 + 4 + 8 + buf[cstart9 + 4 : 8] + 8 +
                 hash(cstart9 + 4 + 8, buf[cstart10 : 1]) * 8 : 8];
                 key14 = (buf[kstart12 : 1]);
                 vstart13 = buf[cstart9 + 4 + 8 + buf[cstart9 + 4 : 8] + 8 +
                 hash(cstart9 + 4 + 8, buf[cstart10 : 1]) * 8 : 8] + 1;
                 if (true && key14[0] = buf[cstart10 : 1]) {
                     print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull]],
                     (key14[0], buf[vstart13 : 1], buf[cstart10 : 1]));
                 } else {

                 }
            }
            cstart10 = cstart10 + 1;
            pcount11 = pcount11 - 1;
        }
    }
    // Locals:
    // pcount4 : Int[nonnull] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // kstart5 : Int[nonnull] (persists=false)
    // cstart3 : Int[nonnull] (persists=true)
    // vstart6 : Int[nonnull] (persists=false)
    // key7 : Tuple[Int[nonnull]] (persists=false)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 4;
        cstart2 = cstart1 + 5;
        cstart3 = cstart1;
        pcount4 = 5;
        loop (0 < pcount4) {
            if (hash(cstart2 + 4 + 8, buf[cstart3 : 1]) * 8 < 0 || buf[cstart2 +
                4 + 8 + buf[cstart2 + 4 : 8] : 8] - 1 < hash(cstart2 + 4 +
                8, buf[cstart3 : 1]) * 8 || buf[cstart2 + 4 + 8 + buf[cstart2 +
                4 : 8] + 8 + hash(cstart2 + 4 + 8, buf[cstart3 : 1]) * 8 : 8] = 0) {

            } else {
                 kstart5 = buf[cstart2 + 4 + 8 + buf[cstart2 + 4 : 8] + 8 +
                 hash(cstart2 + 4 + 8, buf[cstart3 : 1]) * 8 : 8];
                 key7 = (buf[kstart5 : 1]);
                 vstart6 = buf[cstart2 + 4 + 8 + buf[cstart2 + 4 : 8] + 8 +
                 hash(cstart2 + 4 + 8, buf[cstart3 : 1]) * 8 : 8] + 1;
                 if (true && key7[0] = buf[cstart3 : 1]) {
                     c0 = c0 + 1;
                 } else {

                 }
            }
            cstart3 = cstart3 + 1;
            pcount4 = pcount4 - 1;
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
    // high23 : Int[nonnull] (persists=true)
    // cstart16 : Int[nonnull] (persists=true)
    // mid24 : Int[nonnull] (persists=true)
    // low22 : Int[nonnull] (persists=true)
    // key28 : Tuple[Int[nonnull]] (persists=false)
    // pcount18 : Int[nonnull] (persists=true)
    // kstart19 : Int[nonnull] (persists=true)
    // key27 : Tuple[Int[nonnull]] (persists=true)
    // key25 : Tuple[Int[nonnull]] (persists=false)
    // cstart17 : Int[nonnull] (persists=true)
    // key26 : Tuple[Int[nonnull]] (persists=false)
    // cstart15 : Int[nonnull] (persists=true)
    // key21 : Tuple[Int[nonnull]] (persists=true)
    // vstart20 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart15 = 4;
        cstart16 = cstart15 + 5;
        cstart17 = cstart15;
        pcount18 = 5;
        loop (0 < pcount18) {
            low22 = 0;
            high23 = buf[cstart16 + 4 : 8] / 9;
            loop (low22 < high23) {
                mid24 = low22 + high23 / 2;
                kstart19 = cstart16 + 4 + 8 + mid24 * 9;
                key25 = (buf[kstart19 : 1]);
                if (key25[0] < buf[cstart17 : 1]) {
                    low22 = mid24 + 1;
                } else {
                     high23 = mid24;
                }
            }
            if (low22 < buf[cstart16 + 4 : 8] / 9) {
                kstart19 = cstart16 + 4 + 8 + low22 * 9;
                key26 = (buf[kstart19 : 1]);
                key27 = key26;
                loop (key27[0] < buf[cstart17 : 1] + 1 && low22 < buf[cstart16 +
                      4 : 8] / 9) {
                    vstart20 = buf[cstart16 + 4 + 8 + low22 * 9 + 1 : 8];
                    key21 = key27;
                    print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull]],
                    (key21[0], buf[vstart20 : 1], buf[cstart17 : 1]));
                    low22 = low22 + 1;
                    kstart19 = cstart16 + 4 + 8 + low22 * 9;
                    key28 = (buf[kstart19 : 1]);
                    key27 = key28;
                }
            } else {

            }
            cstart17 = cstart17 + 1;
            pcount18 = pcount18 - 1;
        }
    }
    // Locals:
    // pcount4 : Int[nonnull] (persists=true)
    // key13 : Tuple[Int[nonnull]] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    // high9 : Int[nonnull] (persists=true)
    // mid10 : Int[nonnull] (persists=true)
    // key11 : Tuple[Int[nonnull]] (persists=false)
    // low8 : Int[nonnull] (persists=true)
    // key14 : Tuple[Int[nonnull]] (persists=false)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // kstart5 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // vstart6 : Int[nonnull] (persists=true)
    // key12 : Tuple[Int[nonnull]] (persists=false)
    // key7 : Tuple[Int[nonnull]] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 4;
        cstart2 = cstart1 + 5;
        cstart3 = cstart1;
        pcount4 = 5;
        loop (0 < pcount4) {
            low8 = 0;
            high9 = buf[cstart2 + 4 : 8] / 9;
            loop (low8 < high9) {
                mid10 = low8 + high9 / 2;
                kstart5 = cstart2 + 4 + 8 + mid10 * 9;
                key11 = (buf[kstart5 : 1]);
                if (key11[0] < buf[cstart3 : 1]) {
                    low8 = mid10 + 1;
                } else {
                     high9 = mid10;
                }
            }
            if (low8 < buf[cstart2 + 4 : 8] / 9) {
                kstart5 = cstart2 + 4 + 8 + low8 * 9;
                key12 = (buf[kstart5 : 1]);
                key13 = key12;
                loop (key13[0] < buf[cstart3 : 1] + 1 && low8 < buf[cstart2 + 4 :
                      8] / 9) {
                    vstart6 = buf[cstart2 + 4 + 8 + low8 * 9 + 1 : 8];
                    key7 = key13;
                    c0 = c0 + 1;
                    low8 = low8 + 1;
                    kstart5 = cstart2 + 4 + 8 + low8 * 9;
                    key14 = (buf[kstart5 : 1]);
                    key13 = key14;
                }
            } else {

            }
            cstart3 = cstart3 + 1;
            pcount4 = pcount4 - 1;
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
    // pcount16 : Int[nonnull] (persists=true)
    // cstart12 : Int[nonnull] (persists=true)
    // pcount11 : Int[nonnull] (persists=true)
    // cstart17 : Int[nonnull] (persists=true)
    // cstart15 : Int[nonnull] (persists=true)
    // cstart13 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // cstart14 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart10 = 1;
        pcount11 = 3;
        loop (0 < pcount11) {
            cstart12 = cstart10 + 1;
            cstart13 = cstart12 + 1;
            cstart14 = cstart13 + 1;
            cstart15 = cstart14 + 1 + 1;
            pcount16 = buf[cstart14 : 1];
            loop (0 < pcount16) {
                cstart17 = cstart15;
                cstart18 = cstart17 + 1;
                if (buf[cstart13 : 1] = id_c && buf[cstart18 : 1] = id_p) {
                    print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                    Int[nonnull]],
                    (buf[cstart18 : 1], buf[cstart17 : 1], buf[cstart13 : 1],
                     buf[cstart12 : 1]));
                } else {

                }
                cstart15 = cstart15 + 2;
                pcount16 = pcount16 - 1;
            }
            cstart10 = cstart10 + buf[cstart10 : 1];
            pcount11 = pcount11 - 1;
        }
    }
    // Locals:
    // cstart9 : Int[nonnull] (persists=true)
    // pcount7 : Int[nonnull] (persists=true)
    // pcount2 : Int[nonnull] (persists=true)
    // cstart8 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 1;
        pcount2 = 3;
        loop (0 < pcount2) {
            cstart3 = cstart1 + 1;
            cstart4 = cstart3 + 1;
            cstart5 = cstart4 + 1;
            cstart6 = cstart5 + 1 + 1;
            pcount7 = buf[cstart5 : 1];
            loop (0 < pcount7) {
                cstart8 = cstart6;
                cstart9 = cstart8 + 1;
                if (buf[cstart4 : 1] = id_c && buf[cstart9 : 1] = id_p) {
                    c0 = c0 + 1;
                } else {

                }
                cstart6 = cstart6 + 2;
                pcount7 = pcount7 - 1;
            }
            cstart1 = cstart1 + buf[cstart1 : 1];
            pcount2 = pcount2 - 1;
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
    // kstart10 : Int[nonnull] (persists=false)
    // cstart18 : Int[nonnull] (persists=true)
    // pcount16 : Int[nonnull] (persists=true)
    // vstart11 : Int[nonnull] (persists=false)
    // cstart17 : Int[nonnull] (persists=true)
    // cstart15 : Int[nonnull] (persists=true)
    // cstart13 : Int[nonnull] (persists=true)
    // key12 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // cstart14 : Int[nonnull] (persists=true)
    fun printer () : Void {
        if (<tuplehash> * 8 < 0 || buf[12 + buf[4 : 8] : 8] - 1 < <tuplehash> *
            8 || buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] = 0) {

        } else {
             kstart10 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8];
             cstart13 = kstart10;
             cstart14 = cstart13 + 1;
             key12 = (buf[cstart14 : 1], buf[cstart13 : 1]);
             vstart11 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] + 2;
             if (true && key12[0] = id_p && key12[1] = id_c) {
                 cstart15 = vstart11 + 1 + 1;
                 pcount16 = buf[vstart11 : 1];
                 loop (0 < pcount16) {
                     cstart17 = cstart15;
                     cstart18 = cstart17 + 1;
                     print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                     Int[nonnull]],
                     (key12[0], key12[1], buf[cstart18 : 1], buf[cstart17 : 1]));
                     cstart15 = cstart15 + 2;
                     pcount16 = pcount16 - 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // cstart9 : Int[nonnull] (persists=true)
    // vstart2 : Int[nonnull] (persists=false)
    // pcount7 : Int[nonnull] (persists=true)
    // key3 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // cstart8 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // kstart1 : Int[nonnull] (persists=false)
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
             key3 = (buf[cstart5 : 1], buf[cstart4 : 1]);
             vstart2 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] + 2;
             if (true && key3[0] = id_p && key3[1] = id_c) {
                 cstart6 = vstart2 + 1 + 1;
                 pcount7 = buf[vstart2 : 1];
                 loop (0 < pcount7) {
                     cstart8 = cstart6;
                     cstart9 = cstart8 + 1;
                     c0 = c0 + 1;
                     cstart6 = cstart6 + 2;
                     pcount7 = pcount7 - 1;
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
    // cstart29 : Int[nonnull] (persists=true)
    // cstart24 : Int[nonnull] (persists=true)
    // pcount30 : Int[nonnull] (persists=true)
    // cstart25 : Int[nonnull] (persists=true)
    // cstart44 : Int[nonnull] (persists=true)
    // key28 : Tuple[Int[nonnull]] (persists=false)
    // key46 : Tuple[Int[nonnull]] (persists=false)
    // key35 : Tuple[Int[nonnull]] (persists=true)
    // cstart31 : Int[nonnull] (persists=true)
    // pcount43 : Int[nonnull] (persists=true)
    // low36 : Int[nonnull] (persists=true)
    // vstart34 : Int[nonnull] (persists=true)
    // key41 : Tuple[Int[nonnull]] (persists=true)
    // high37 : Int[nonnull] (persists=true)
    // vstart27 : Int[nonnull] (persists=false)
    // kstart33 : Int[nonnull] (persists=true)
    // key39 : Tuple[Int[nonnull]] (persists=false)
    // cstart42 : Int[nonnull] (persists=true)
    // cstart45 : Int[nonnull] (persists=true)
    // mid38 : Int[nonnull] (persists=true)
    // kstart26 : Int[nonnull] (persists=false)
    // key40 : Tuple[Int[nonnull]] (persists=false)
    // cstart32 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart24 = 4;
        cstart25 = cstart24 + buf[cstart24 : 4];
        if (hash(cstart24 + 4 + 8, id_p) * 8 < 0 || buf[cstart24 + 4 + 8 +
            buf[cstart24 + 4 : 8] : 8] - 1 < hash(cstart24 + 4 + 8, id_p) * 8 ||
            buf[cstart24 + 4 + 8 + buf[cstart24 + 4 : 8] + 8 + hash(cstart24 +
            4 + 8, id_p) * 8 : 8] = 0) {

        } else {
             kstart26 = buf[cstart24 + 4 + 8 + buf[cstart24 + 4 : 8] + 8 +
             hash(cstart24 + 4 + 8, id_p) * 8 : 8];
             key28 = (buf[kstart26 : 1]);
             vstart27 = buf[cstart24 + 4 + 8 + buf[cstart24 + 4 : 8] + 8 +
             hash(cstart24 + 4 + 8, id_p) * 8 : 8] + 1;
             if (true && key28[0] = id_p) {
                 cstart29 = vstart27;
                 pcount30 = 1;
                 loop (0 < pcount30) {
                     cstart31 = cstart29;
                     cstart32 = cstart31 + 1;
                     low36 = 0;
                     high37 = buf[cstart25 + 4 : 8] / 9;
                     loop (low36 < high37) {
                         mid38 = low36 + high37 / 2;
                         kstart33 = cstart25 + 4 + 8 + mid38 * 9;
                         key39 = (buf[kstart33 : 1]);
                         if (key39[0] < buf[cstart32 : 1]) {
                             low36 = mid38 + 1;
                         } else {
                              high37 = mid38;
                         }
                     }
                     if (low36 < buf[cstart25 + 4 : 8] / 9) {
                         kstart33 = cstart25 + 4 + 8 + low36 * 9;
                         key40 = (buf[kstart33 : 1]);
                         key41 = key40;
                         loop (key41[0] < buf[cstart31 : 1] && low36 <
                               buf[cstart25 + 4 : 8] / 9) {
                             vstart34 = buf[cstart25 + 4 + 8 + low36 * 9 + 1 :
                             8];
                             key35 = key41;
                             cstart42 = vstart34 + 1 + 1;
                             pcount43 = buf[vstart34 : 1];
                             loop (0 < pcount43) {
                                 cstart44 = cstart42;
                                 cstart45 = cstart44 + 1;
                                 if (buf[cstart45 : 1] = id_c) {
                                     print(Tuple[Int[nonnull], Int[nonnull]],
                                     (buf[cstart45 : 1], buf[cstart31 : 1]));
                                 } else {

                                 }
                                 cstart42 = cstart42 + 2;
                                 pcount43 = pcount43 - 1;
                             }
                             low36 = low36 + 1;
                             kstart33 = cstart25 + 4 + 8 + low36 * 9;
                             key46 = (buf[kstart33 : 1]);
                             key41 = key46;
                         }
                     } else {

                     }
                     cstart29 = cstart29 + 2;
                     pcount30 = pcount30 - 1;
                 }
             } else {

             }
        }
    }
    // Locals:
    // kstart10 : Int[nonnull] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    // vstart4 : Int[nonnull] (persists=false)
    // cstart9 : Int[nonnull] (persists=true)
    // cstart21 : Int[nonnull] (persists=true)
    // key17 : Tuple[Int[nonnull]] (persists=false)
    // cstart22 : Int[nonnull] (persists=true)
    // pcount7 : Int[nonnull] (persists=true)
    // cstart8 : Int[nonnull] (persists=true)
    // low13 : Int[nonnull] (persists=true)
    // key16 : Tuple[Int[nonnull]] (persists=false)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart19 : Int[nonnull] (persists=true)
    // high14 : Int[nonnull] (persists=true)
    // mid15 : Int[nonnull] (persists=true)
    // key23 : Tuple[Int[nonnull]] (persists=false)
    // vstart11 : Int[nonnull] (persists=true)
    // pcount20 : Int[nonnull] (persists=true)
    // kstart3 : Int[nonnull] (persists=false)
    // key5 : Tuple[Int[nonnull]] (persists=false)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // key18 : Tuple[Int[nonnull]] (persists=true)
    // key12 : Tuple[Int[nonnull]] (persists=true)
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
                 pcount7 = 1;
                 loop (0 < pcount7) {
                     cstart8 = cstart6;
                     cstart9 = cstart8 + 1;
                     low13 = 0;
                     high14 = buf[cstart2 + 4 : 8] / 9;
                     loop (low13 < high14) {
                         mid15 = low13 + high14 / 2;
                         kstart10 = cstart2 + 4 + 8 + mid15 * 9;
                         key16 = (buf[kstart10 : 1]);
                         if (key16[0] < buf[cstart9 : 1]) {
                             low13 = mid15 + 1;
                         } else {
                              high14 = mid15;
                         }
                     }
                     if (low13 < buf[cstart2 + 4 : 8] / 9) {
                         kstart10 = cstart2 + 4 + 8 + low13 * 9;
                         key17 = (buf[kstart10 : 1]);
                         key18 = key17;
                         loop (key18[0] < buf[cstart8 : 1] && low13 <
                               buf[cstart2 + 4 : 8] / 9) {
                             vstart11 = buf[cstart2 + 4 + 8 + low13 * 9 + 1 : 8];
                             key12 = key18;
                             cstart19 = vstart11 + 1 + 1;
                             pcount20 = buf[vstart11 : 1];
                             loop (0 < pcount20) {
                                 cstart21 = cstart19;
                                 cstart22 = cstart21 + 1;
                                 if (buf[cstart22 : 1] = id_c) {
                                     c0 = c0 + 1;
                                 } else {

                                 }
                                 cstart19 = cstart19 + 2;
                                 pcount20 = pcount20 - 1;
                             }
                             low13 = low13 + 1;
                             kstart10 = cstart2 + 4 + 8 + low13 * 9;
                             key23 = (buf[kstart10 : 1]);
                             key18 = key23;
                         }
                     } else {

                     }
                     cstart6 = cstart6 + 2;
                     pcount7 = pcount7 - 1;
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
    // min16 : Int[nonnull] (persists=false)
    // found_tup15 : Bool[nonnull] (persists=false)
    // first12 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // pcount11 : Int[nonnull] (persists=true)
    // pcount18 : Int[nonnull] (persists=true)
    // cstart17 : Int[nonnull] (persists=true)
    // tup14 : Tuple[Int[nonnull]] (persists=false)
    fun printer () : Void {
        cstart10 = 0;
        pcount11 = 5;
        loop (0 < pcount11) {
            found_tup15 = false;
            min16 = 4611686018427387903;
            cstart17 = 0;
            pcount18 = 5;
            loop (0 < pcount18) {
                tup14 = (buf[cstart17 : 1]);
                min16 = tup14[0] < min16 ? tup14[0] : min16;
                found_tup15 = true;
                cstart17 = cstart17 + 1;
                pcount18 = pcount18 - 1;
            }
            if (found_tup15) {
                first12 = min16;
            } else {

            }
            if (first12 = buf[cstart10 : 1]) {
                print(Tuple[Int[nonnull]], (buf[cstart10 : 1]));
            } else {

            }
            cstart10 = cstart10 + 1;
            pcount11 = pcount11 - 1;
        }
    }
    // Locals:
    // first3 : Int[nonnull] (persists=true)
    // pcount2 : Int[nonnull] (persists=true)
    // min7 : Int[nonnull] (persists=false)
    // pcount9 : Int[nonnull] (persists=true)
    // cstart8 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // tup5 : Tuple[Int[nonnull]] (persists=false)
    // found_tup6 : Bool[nonnull] (persists=false)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 0;
        pcount2 = 5;
        loop (0 < pcount2) {
            found_tup6 = false;
            min7 = 4611686018427387903;
            cstart8 = 0;
            pcount9 = 5;
            loop (0 < pcount9) {
                tup5 = (buf[cstart8 : 1]);
                min7 = tup5[0] < min7 ? tup5[0] : min7;
                found_tup6 = true;
                cstart8 = cstart8 + 1;
                pcount9 = pcount9 - 1;
            }
            if (found_tup6) {
                first3 = min7;
            } else {

            }
            if (first3 = buf[cstart1 : 1]) {
                c0 = c0 + 1;
            } else {

            }
            cstart1 = cstart1 + 1;
            pcount2 = pcount2 - 1;
        }
        return c0;
    } |}]
