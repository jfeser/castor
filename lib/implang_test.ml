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

let%expect_test "sum-complex" =
  run_test
    "Select([sum(r1.f) + 5, count() + sum(r1.f / 2)], AList(r1, \
     ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross)))" ;
  [%expect
    {|
    // Locals:
    // start : Int[nonnull] (persists=true)
    fun scalar_6 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // start : Int[nonnull] (persists=true)
    // r1_f : Int[nonnull] (persists=true)
    fun scalar_8 (r1_f,
        start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // tup9 : Tuple[Int[nonnull]] (persists=true)
    // tup7 : Tuple[Int[nonnull]] (persists=true)
    // start : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    fun tuple_3 (start) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart4 = start;
        cstart5 = cstart4 + 1;
        init scalar_6(cstart4);
        tup7 = next(scalar_6);
        init scalar_8(tup7[0], cstart5);
        tup9 = next(scalar_8);
        yield (tup7[0], tup9[0]);
    }
    // Locals:
    // tup11 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // pcount10 : Int[nonnull] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    fun list_1 () : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart2 = 0;
        pcount10 = 5;
        loop (0 < pcount10) {
            init tuple_3(cstart2);
            tup11 = next(tuple_3);
            yield tup11;
            cstart2 = cstart2 + 2;
            pcount10 = pcount10 - 1;
        }
    }
    // Locals:
    // tup17 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // found_tup16 : Bool[nonnull] (persists=false)
    // sum20 : Int[nonnull] (persists=false)
    // sum18 : Int[nonnull] (persists=false)
    // count21 : Int[nonnull] (persists=true)
    // tup15 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // count19 : Int[nonnull] (persists=false)
    fun select_0 () : Tuple[Int[nonnull],
        Int[nonnull]] {
        found_tup16 = false;
        init list_1();
        sum18 = 0;
        count19 = 0;
        sum20 = 0;
        count21 = 5;
        loop (0 < count21) {
            tup17 = next(list_1);
            sum18 = sum18 + tup17[0];
            count19 = count19 + 1;
            sum20 = sum20 + tup17[0] / 2;
            tup15 = tup17;
            found_tup16 = true;
            count21 = count21 - 1;
        }
        if (found_tup16) {
            yield (sum18 + 5, count19 + sum20);
        } else {

        }
    }
    // Locals:
    // tup24 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    fun printer () : Void {
        init select_0();
        loop (not done(select_0)) {
            tup24 = next(select_0);
            if (not done(select_0)) {
                print(Tuple[Int[nonnull], Int[nonnull]], tup24);
            } else {

            }
        }
    }
    // Locals:
    // tup23 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // c22 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c22 = 0;
        init select_0();
        loop (not done(select_0)) {
            tup23 = next(select_0);
            if (not done(select_0)) {
                c22 = c22 + 1;
            } else {

            }
        }
        return c22;
    } |}]

let%expect_test "sum" =
  run_test
    "Select([sum(r1.f), count()], AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g - \
     r1.f)], cross)))" ;
  [%expect
    {|
    // Locals:
    // start : Int[nonnull] (persists=true)
    fun scalar_6 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // start : Int[nonnull] (persists=true)
    // r1_f : Int[nonnull] (persists=true)
    fun scalar_8 (r1_f,
        start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // tup9 : Tuple[Int[nonnull]] (persists=true)
    // tup7 : Tuple[Int[nonnull]] (persists=true)
    // start : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    fun tuple_3 (start) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart4 = start;
        cstart5 = cstart4 + 1;
        init scalar_6(cstart4);
        tup7 = next(scalar_6);
        init scalar_8(tup7[0], cstart5);
        tup9 = next(scalar_8);
        yield (tup7[0], tup9[0]);
    }
    // Locals:
    // tup11 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // pcount10 : Int[nonnull] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    fun list_1 () : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart2 = 0;
        pcount10 = 5;
        loop (0 < pcount10) {
            init tuple_3(cstart2);
            tup11 = next(tuple_3);
            yield tup11;
            cstart2 = cstart2 + 2;
            pcount10 = pcount10 - 1;
        }
    }
    // Locals:
    // count18 : Int[nonnull] (persists=false)
    // found_tup15 : Bool[nonnull] (persists=false)
    // tup16 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // sum17 : Int[nonnull] (persists=false)
    // count19 : Int[nonnull] (persists=true)
    // tup14 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    fun select_0 () : Tuple[Int[nonnull],
        Int[nonnull]] {
        found_tup15 = false;
        init list_1();
        sum17 = 0;
        count18 = 0;
        count19 = 5;
        loop (0 < count19) {
            tup16 = next(list_1);
            sum17 = sum17 + tup16[0];
            count18 = count18 + 1;
            tup14 = tup16;
            found_tup15 = true;
            count19 = count19 - 1;
        }
        if (found_tup15) {
            yield (sum17, count18);
        } else {

        }
    }
    // Locals:
    // tup22 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    fun printer () : Void {
        init select_0();
        loop (not done(select_0)) {
            tup22 = next(select_0);
            if (not done(select_0)) {
                print(Tuple[Int[nonnull], Int[nonnull]], tup22);
            } else {

            }
        }
    }
    // Locals:
    // tup21 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // c20 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c20 = 0;
        init select_0();
        loop (not done(select_0)) {
            tup21 = next(select_0);
            if (not done(select_0)) {
                c20 = c20 + 1;
            } else {

            }
        }
        return c20;
    } |}]

let%expect_test "cross-tuple" =
  run_test "AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross))" ;
  [%expect
    {|
    // Locals:
    // start : Int[nonnull] (persists=true)
    fun scalar_5 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // start : Int[nonnull] (persists=true)
    // r1_f : Int[nonnull] (persists=true)
    fun scalar_7 (r1_f,
        start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // tup6 : Tuple[Int[nonnull]] (persists=true)
    // start : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // tup8 : Tuple[Int[nonnull]] (persists=true)
    fun tuple_2 (start) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart3 = start;
        cstart4 = cstart3 + 1;
        init scalar_5(cstart3);
        tup6 = next(scalar_5);
        init scalar_7(tup6[0], cstart4);
        tup8 = next(scalar_7);
        yield (tup6[0], tup8[0]);
    }
    // Locals:
    // pcount9 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // tup10 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    fun list_0 () : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart1 = 0;
        pcount9 = 5;
        loop (0 < pcount9) {
            init tuple_2(cstart1);
            tup10 = next(tuple_2);
            yield tup10;
            cstart1 = cstart1 + 2;
            pcount9 = pcount9 - 1;
        }
    }
    // Locals:
    // tup13 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    fun printer () : Void {
        init list_0();
        loop (not done(list_0)) {
            tup13 = next(list_0);
            if (not done(list_0)) {
                print(Tuple[Int[nonnull], Int[nonnull]], tup13);
            } else {

            }
        }
    }
    // Locals:
    // tup12 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // c11 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c11 = 0;
        init list_0();
        loop (not done(list_0)) {
            tup12 = next(list_0);
            if (not done(list_0)) {
                c11 = c11 + 1;
            } else {

            }
        }
        return c11;
    } |}]

let%expect_test "hash-idx" =
  run_test
    "ATuple([AList(r1, AScalar(r1.f)) as f, AHashIdx(dedup(select([r1.f], r1)) as \
     k, ascalar(k.f+1), f.f)], cross)" ;
  [%expect
    {|
    // Locals:
    // start : Int[nonnull] (persists=true)
    fun scalar_5 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // tup7 : Tuple[Int[nonnull]] (persists=false)
    // start : Int[nonnull] (persists=true)
    // pcount6 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    fun list_3 (start) : Tuple[Int[nonnull]] {
        cstart4 = start;
        pcount6 = 5;
        loop (0 < pcount6) {
            init scalar_5(cstart4);
            tup7 = next(scalar_5);
            yield tup7;
            cstart4 = cstart4 + 1;
            pcount6 = pcount6 - 1;
        }
    }
    // Locals:
    // f_f : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    fun scalar_12 (f_f,
        start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // f_f : Int[nonnull] (persists=true)
    // k_f : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    fun scalar_15 (k_f,
        f_f,
        start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // f_f : Int[nonnull] (persists=true)
    // vstart14 : Int[nonnull] (persists=false)
    // key13 : Tuple[Int[nonnull]] (persists=false)
    // start : Int[nonnull] (persists=true)
    // tup16 : Tuple[Int[nonnull]] (persists=false)
    // kstart11 : Int[nonnull] (persists=false)
    fun hash_idx_10 (f_f,
        start) : Tuple[Int[nonnull],
        Int[nonnull]] {
        if (hash(start + 4 + 8, f_f) * 8 < 0 || buf[start + 4 + 8 + buf[start +
            4 : 8] : 8] - 1 < hash(start + 4 + 8, f_f) * 8 || buf[start + 4 + 8 +
            buf[start + 4 : 8] + 8 + hash(start + 4 + 8, f_f) * 8 : 8] = 0) {

        } else {
             kstart11 = buf[start + 4 + 8 + buf[start + 4 : 8] + 8 + hash(start +
             4 + 8, f_f) * 8 : 8];
             init scalar_12(f_f, kstart11);
             key13 = next(scalar_12);
             vstart14 = buf[start + 4 + 8 + buf[start + 4 : 8] + 8 + hash(start +
             4 + 8, f_f) * 8 : 8] + 1;
             if (true && key13[0] = f_f) {
                 init scalar_15(key13[0], f_f, vstart14);
                 tup16 = next(scalar_15);
                 yield (key13[0], tup16[0]);
             } else {

             }
        }
    }
    // Locals:
    // tup17 : Tuple[Int[nonnull], Int[nonnull]] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // count9 : Int[nonnull] (persists=true)
    // tup8 : Tuple[Int[nonnull]] (persists=true)
    fun tuple_0 () : Tuple[Int[nonnull],
        Int[nonnull],
        Int[nonnull]] {
        cstart1 = 4;
        cstart2 = cstart1 + 5;
        init list_3(cstart1);
        count9 = 5;
        loop (0 < count9) {
            tup8 = next(list_3);
            init hash_idx_10(tup8[0], cstart2);
            loop (not done(hash_idx_10)) {
                tup17 = next(hash_idx_10);
                if (not done(hash_idx_10)) {
                    yield (tup8[0], tup17[0], tup17[1]);
                } else {

                }
            }
            count9 = count9 - 1;
        }
    }
    // Locals:
    // tup20 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull]] (persists=false)
    fun printer () : Void {
        init tuple_0();
        loop (not done(tuple_0)) {
            tup20 = next(tuple_0);
            if (not done(tuple_0)) {
                print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull]], tup20);
            } else {

            }
        }
    }
    // Locals:
    // c18 : Int[nonnull] (persists=true)
    // tup19 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull]] (persists=false)
    fun counter () : Int[nonnull] {
        c18 = 0;
        init tuple_0();
        loop (not done(tuple_0)) {
            tup19 = next(tuple_0);
            if (not done(tuple_0)) {
                c18 = c18 + 1;
            } else {

            }
        }
        return c18;
    } |}]

let example_params =
  [ Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_p"
  ; Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_c" ]

let example_db_params =
  [ Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_p"
  ; Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_c" ]

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
    // start : Int[nonnull] (persists=true)
    fun scalar_7 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // lp_id : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    fun scalar_9 (lp_id,
        start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // lp_id : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // lp_counter : Int[nonnull] (persists=true)
    fun scalar_16 (lp_id,
        lp_counter,
        start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // lc_id : Int[nonnull] (persists=true)
    // lp_id : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // lp_counter : Int[nonnull] (persists=true)
    fun scalar_18 (lp_id,
        lp_counter,
        lc_id,
        start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // tup17 : Tuple[Int[nonnull]] (persists=true)
    // cstart15 : Int[nonnull] (persists=true)
    // tup19 : Tuple[Int[nonnull]] (persists=true)
    // start : Int[nonnull] (persists=true)
    // lp_counter : Int[nonnull] (persists=true)
    // cstart14 : Int[nonnull] (persists=true)
    // lp_id : Int[nonnull] (persists=true)
    fun tuple_13 (lp_id,
        lp_counter,
        start) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart14 = start;
        cstart15 = cstart14 + 1;
        init scalar_16(lp_id, lp_counter, cstart14);
        tup17 = next(scalar_16);
        init scalar_18(lp_id, lp_counter, tup17[0], cstart15);
        tup19 = next(scalar_18);
        yield (tup17[0], tup19[0]);
    }
    // Locals:
    // pcount20 : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // lp_counter : Int[nonnull] (persists=true)
    // cstart12 : Int[nonnull] (persists=true)
    // tup21 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // lp_id : Int[nonnull] (persists=true)
    fun list_11 (lp_id,
        lp_counter,
        start) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart12 = start + 1 + 1;
        pcount20 = buf[start : 1];
        loop (0 < pcount20) {
            init tuple_13(lp_id, lp_counter, cstart12);
            tup21 = next(tuple_13);
            yield tup21;
            cstart12 = cstart12 + 2;
            pcount20 = pcount20 - 1;
        }
    }
    // Locals:
    // tup10 : Tuple[Int[nonnull]] (persists=true)
    // start : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // tup22 : Tuple[Int[nonnull], Int[nonnull]] (persists=true)
    // tup8 : Tuple[Int[nonnull]] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    fun tuple_3 (start) : Tuple[Int[nonnull],
        Int[nonnull],
        Int[nonnull],
        Int[nonnull]] {
        cstart4 = start + 1;
        cstart5 = cstart4 + 1;
        cstart6 = cstart5 + 1;
        init scalar_7(cstart4);
        tup8 = next(scalar_7);
        init scalar_9(tup8[0], cstart5);
        tup10 = next(scalar_9);
        init list_11(tup8[0], tup10[0], cstart6);
        loop (not done(list_11)) {
            tup22 = next(list_11);
            if (not done(list_11)) {
                yield (tup8[0], tup10[0], tup22[0], tup22[1]);
            } else {

            }
        }
    }
    // Locals:
    // pcount23 : Int[nonnull] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    // tup24 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull], Int[nonnull]] (persists=false)
    fun list_1 () : Tuple[Int[nonnull],
        Int[nonnull],
        Int[nonnull],
        Int[nonnull]] {
        cstart2 = 1;
        pcount23 = 3;
        loop (0 < pcount23) {
            init tuple_3(cstart2);
            loop (not done(tuple_3)) {
                tup24 = next(tuple_3);
                if (not done(tuple_3)) {
                    yield tup24;
                } else {

                }
            }
            cstart2 = cstart2 + buf[cstart2 : 1];
            pcount23 = pcount23 - 1;
        }
    }
    // Locals:
    // tup25 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull], Int[nonnull]] (persists=false)
    // count26 : Int[nonnull] (persists=true)
    fun filter_0 () : Tuple[Int[nonnull],
        Int[nonnull],
        Int[nonnull],
        Int[nonnull]] {
        init list_1();
        count26 = 3;
        loop (0 < count26) {
            tup25 = next(list_1);
            if (tup25[2] = id_c && tup25[0] = id_p) {
                yield tup25;
            } else {

            }
            count26 = count26 - 1;
        }
    }
    // Locals:
    // tup29 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull], Int[nonnull]] (persists=false)
    fun printer () : Void {
        init filter_0();
        loop (not done(filter_0)) {
            tup29 = next(filter_0);
            if (not done(filter_0)) {
                print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                Int[nonnull]], tup29);
            } else {

            }
        }
    }
    // Locals:
    // tup28 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull], Int[nonnull]] (persists=false)
    // c27 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c27 = 0;
        init filter_0();
        loop (not done(filter_0)) {
            tup28 = next(filter_0);
            if (not done(filter_0)) {
                c27 = c27 + 1;
            } else {

            }
        }
        return c27;
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
    // start : Int[nonnull] (persists=true)
    fun scalar_5 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // lp_k : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    fun scalar_7 (start,
        lp_k) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // tup6 : Tuple[Int[nonnull]] (persists=true)
    // start : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // tup8 : Tuple[Int[nonnull]] (persists=true)
    fun tuple_2 (start) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart3 = start;
        cstart4 = cstart3 + 1;
        init scalar_5(cstart3);
        tup6 = next(scalar_5);
        init scalar_7(cstart4, tup6[0]);
        tup8 = next(scalar_7);
        yield (tup6[0], tup8[0]);
    }
    // Locals:
    // lp_k : Int[nonnull] (persists=true)
    // lc_k : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    fun scalar_16 (start,
        lp_k,
        lc_k) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // lp_k : Int[nonnull] (persists=true)
    // lc_k : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // lp_counter : Int[nonnull] (persists=true)
    fun scalar_18 (lp_counter,
        start,
        lp_k,
        lc_k) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // tup17 : Tuple[Int[nonnull]] (persists=true)
    // cstart15 : Int[nonnull] (persists=true)
    // lc_k : Int[nonnull] (persists=true)
    // tup19 : Tuple[Int[nonnull]] (persists=true)
    // start : Int[nonnull] (persists=true)
    // lp_k : Int[nonnull] (persists=true)
    // cstart14 : Int[nonnull] (persists=true)
    fun tuple_13 (start,
        lp_k,
        lc_k) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart14 = start;
        cstart15 = cstart14 + 1;
        init scalar_16(cstart14, lp_k, lc_k);
        tup17 = next(scalar_16);
        init scalar_18(tup17[0], cstart15, lp_k, lc_k);
        tup19 = next(scalar_18);
        yield (tup17[0], tup19[0]);
    }
    // Locals:
    // pcount20 : Int[nonnull] (persists=true)
    // lc_k : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // lp_k : Int[nonnull] (persists=true)
    // cstart12 : Int[nonnull] (persists=true)
    // tup21 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    fun list_11 (start,
        lp_k,
        lc_k) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart12 = start + 1 + 1;
        pcount20 = buf[start : 1];
        loop (0 < pcount20) {
            init tuple_13(cstart12, lp_k, lc_k);
            tup21 = next(tuple_13);
            yield tup21;
            cstart12 = cstart12 + 2;
            pcount20 = pcount20 - 1;
        }
    }
    // Locals:
    // tup22 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // kstart1 : Int[nonnull] (persists=false)
    // key9 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // vstart10 : Int[nonnull] (persists=false)
    fun hash_idx_0 () : Tuple[Int[nonnull],
        Int[nonnull],
        Int[nonnull],
        Int[nonnull]] {
        if (<tuplehash> * 8 < 0 || buf[12 + buf[4 : 8] : 8] - 1 < <tuplehash> *
            8 || buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] = 0) {

        } else {
             kstart1 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8];
             init tuple_2(kstart1);
             key9 = next(tuple_2);
             vstart10 = buf[12 + buf[4 : 8] + 8 + <tuplehash> * 8 : 8] + 2;
             if (true && key9[0] = id_p && key9[1] = id_c) {
                 init list_11(vstart10, key9[0], key9[1]);
                 loop (not done(list_11)) {
                     tup22 = next(list_11);
                     if (not done(list_11)) {
                         yield (key9[0], key9[1], tup22[0], tup22[1]);
                     } else {

                     }
                 }
             } else {

             }
        }
    }
    // Locals:
    // tup25 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull], Int[nonnull]] (persists=false)
    fun printer () : Void {
        init hash_idx_0();
        loop (not done(hash_idx_0)) {
            tup25 = next(hash_idx_0);
            if (not done(hash_idx_0)) {
                print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                Int[nonnull]], tup25);
            } else {

            }
        }
    }
    // Locals:
    // tup24 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull], Int[nonnull]] (persists=false)
    // c23 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c23 = 0;
        init hash_idx_0();
        loop (not done(hash_idx_0)) {
            tup24 = next(hash_idx_0);
            if (not done(hash_idx_0)) {
                c23 = c23 + 1;
            } else {

            }
        }
        return c23;
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
    // start : Int[nonnull] (persists=true)
    fun scalar_6 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // start : Int[nonnull] (persists=true)
    // k : Int[nonnull] (persists=true)
    fun scalar_14 (start,
        k) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // k : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // log_counter : Int[nonnull] (persists=true)
    fun scalar_16 (log_counter,
        start,
        k) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // tup17 : Tuple[Int[nonnull]] (persists=true)
    // cstart13 : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // cstart12 : Int[nonnull] (persists=true)
    // tup15 : Tuple[Int[nonnull]] (persists=true)
    // k : Int[nonnull] (persists=true)
    fun tuple_11 (start,
        k) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart12 = start;
        cstart13 = cstart12 + 1;
        init scalar_14(cstart12, k);
        tup15 = next(scalar_14);
        init scalar_16(tup15[0], cstart13, k);
        tup17 = next(scalar_16);
        yield (tup15[0], tup17[0]);
    }
    // Locals:
    // tup19 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // start : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // k : Int[nonnull] (persists=true)
    // pcount18 : Int[nonnull] (persists=true)
    fun list_9 (start,
        k) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart10 = start;
        pcount18 = 1;
        loop (0 < pcount18) {
            init tuple_11(cstart10, k);
            tup19 = next(tuple_11);
            yield tup19;
            cstart10 = cstart10 + 2;
            pcount18 = pcount18 - 1;
        }
    }
    // Locals:
    // tup20 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // kstart5 : Int[nonnull] (persists=false)
    // vstart8 : Int[nonnull] (persists=false)
    // start : Int[nonnull] (persists=true)
    // key7 : Tuple[Int[nonnull]] (persists=false)
    fun hash_idx_4 (start) : Tuple[Int[nonnull],
        Int[nonnull],
        Int[nonnull]] {
        if (hash(start + 4 + 8, id_p) * 8 < 0 || buf[start + 4 + 8 + buf[start +
            4 : 8] : 8] - 1 < hash(start + 4 + 8, id_p) * 8 || buf[start + 4 +
            8 + buf[start + 4 : 8] + 8 + hash(start + 4 + 8, id_p) * 8 : 8] = 0) {

        } else {
             kstart5 = buf[start + 4 + 8 + buf[start + 4 : 8] + 8 + hash(start +
             4 + 8, id_p) * 8 : 8];
             init scalar_6(kstart5);
             key7 = next(scalar_6);
             vstart8 = buf[start + 4 + 8 + buf[start + 4 : 8] + 8 + hash(start +
             4 + 8, id_p) * 8 : 8] + 1;
             if (true && key7[0] = id_p) {
                 init list_9(vstart8, key7[0]);
                 tup20 = next(list_9);
                 yield (key7[0], tup20[0], tup20[1]);
             } else {

             }
        }
    }
    // Locals:
    // lp_succ : Int[nonnull] (persists=true)
    // lp_k : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // lp_counter : Int[nonnull] (persists=true)
    fun scalar_25 (lp_succ,
        lp_k,
        lp_counter,
        start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // lp_succ : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // lp_counter : Int[nonnull] (persists=true)
    // lp_k : Int[nonnull] (persists=true)
    // k : Int[nonnull] (persists=true)
    fun scalar_33 (lp_succ,
        lp_k,
        lp_counter,
        start,
        k) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // lp_succ : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // lp_counter : Int[nonnull] (persists=true)
    // lp_k : Int[nonnull] (persists=true)
    // log_id : Int[nonnull] (persists=true)
    // k : Int[nonnull] (persists=true)
    fun scalar_35 (lp_succ,
        lp_k,
        lp_counter,
        log_id,
        start,
        k) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // lp_counter : Int[nonnull] (persists=true)
    // lp_k : Int[nonnull] (persists=true)
    // lp_succ : Int[nonnull] (persists=true)
    // cstart31 : Int[nonnull] (persists=true)
    // tup36 : Tuple[Int[nonnull]] (persists=true)
    // tup34 : Tuple[Int[nonnull]] (persists=true)
    // start : Int[nonnull] (persists=true)
    // k : Int[nonnull] (persists=true)
    // cstart32 : Int[nonnull] (persists=true)
    fun tuple_30 (lp_succ,
        lp_k,
        lp_counter,
        start,
        k) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart31 = start;
        cstart32 = cstart31 + 1;
        init scalar_33(lp_succ, lp_k, lp_counter, cstart31, k);
        tup34 = next(scalar_33);
        init scalar_35(lp_succ, lp_k, lp_counter, tup34[0], cstart32, k);
        tup36 = next(scalar_35);
        yield (tup34[0], tup36[0]);
    }
    // Locals:
    // pcount37 : Int[nonnull] (persists=true)
    // lp_succ : Int[nonnull] (persists=true)
    // cstart29 : Int[nonnull] (persists=true)
    // tup38 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // start : Int[nonnull] (persists=true)
    // lp_counter : Int[nonnull] (persists=true)
    // lp_k : Int[nonnull] (persists=true)
    // k : Int[nonnull] (persists=true)
    fun list_28 (lp_succ,
        lp_k,
        lp_counter,
        start,
        k) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart29 = start + 1 + 1;
        pcount37 = buf[start : 1];
        loop (0 < pcount37) {
            init tuple_30(lp_succ, lp_k, lp_counter, cstart29, k);
            tup38 = next(tuple_30);
            yield tup38;
            cstart29 = cstart29 + 2;
            pcount37 = pcount37 - 1;
        }
    }
    // Locals:
    // vstart27 : Int[nonnull] (persists=true)
    // key44 : Tuple[Int[nonnull]] (persists=true)
    // lp_counter : Int[nonnull] (persists=true)
    // high40 : Int[nonnull] (persists=true)
    // lp_k : Int[nonnull] (persists=true)
    // key46 : Tuple[Int[nonnull]] (persists=false)
    // tup45 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // key42 : Tuple[Int[nonnull]] (persists=false)
    // lp_succ : Int[nonnull] (persists=true)
    // key26 : Tuple[Int[nonnull]] (persists=true)
    // kstart24 : Int[nonnull] (persists=true)
    // low39 : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // mid41 : Int[nonnull] (persists=true)
    // key43 : Tuple[Int[nonnull]] (persists=false)
    fun ordered_idx_23 (lp_succ,
        lp_k,
        lp_counter,
        start) : Tuple[Int[nonnull],
        Int[nonnull],
        Int[nonnull]] {
        low39 = 0;
        high40 = buf[start + 4 : 8] / 9;
        loop (low39 < high40) {
            mid41 = low39 + high40 / 2;
            kstart24 = start + 4 + 8 + mid41 * 9;
            init scalar_25(lp_succ, lp_k, lp_counter, kstart24);
            key42 = next(scalar_25);
            if (key42[0] < lp_counter) {
                low39 = mid41 + 1;
            } else {
                 high40 = mid41;
            }
        }
        if (low39 < buf[start + 4 : 8] / 9) {
            kstart24 = start + 4 + 8 + low39 * 9;
            init scalar_25(lp_succ, lp_k, lp_counter, kstart24);
            key43 = next(scalar_25);
            key44 = key43;
            loop (key44[0] < lp_succ && low39 < buf[start + 4 : 8] / 9) {
                vstart27 = buf[start + 4 + 8 + low39 * 9 + 1 : 8];
                key26 = key44;
                init list_28(lp_succ, lp_k, lp_counter, vstart27, key26[0]);
                loop (not done(list_28)) {
                    tup45 = next(list_28);
                    if (not done(list_28)) {
                        yield (key44[0], tup45[0], tup45[1]);
                    } else {

                    }
                }
                low39 = low39 + 1;
                kstart24 = start + 4 + 8 + low39 * 9;
                init scalar_25(lp_succ, lp_k, lp_counter, kstart24);
                key46 = next(scalar_25);
                key44 = key46;
            }
        } else {

        }
    }
    // Locals:
    // lp_succ : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // lp_counter : Int[nonnull] (persists=true)
    // lp_k : Int[nonnull] (persists=true)
    // tup47 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull]] (persists=false)
    fun filter_22 (lp_succ,
        lp_k,
        lp_counter,
        start) : Tuple[Int[nonnull],
        Int[nonnull],
        Int[nonnull]] {
        init ordered_idx_23(lp_succ, lp_k, lp_counter, start);
        loop (not done(ordered_idx_23)) {
            tup47 = next(ordered_idx_23);
            if (not done(ordered_idx_23)) {
                if (tup47[1] = id_c) {
                    yield tup47;
                } else {

                }
            } else {

            }
        }
    }
    // Locals:
    // tup48 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull]] (persists=true)
    // cstart2 : Int[nonnull] (persists=true)
    // tup21 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull]] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    fun tuple_1 () : Tuple[Int[nonnull],
        Int[nonnull],
        Int[nonnull],
        Int[nonnull],
        Int[nonnull],
        Int[nonnull]] {
        cstart2 = 4;
        cstart3 = cstart2 + buf[cstart2 : 4];
        init hash_idx_4(cstart2);
        loop (not done(hash_idx_4)) {
            tup21 = next(hash_idx_4);
            if (not done(hash_idx_4)) {
                init filter_22(tup21[2], tup21[0], tup21[1], cstart3);
                loop (not done(filter_22)) {
                    tup48 = next(filter_22);
                    if (not done(filter_22)) {
                        yield
                        (tup21[0], tup21[1], tup21[2], tup48[0], tup48[1],
                         tup48[2]);
                    } else {

                    }
                }
            } else {

            }
        }
    }
    // Locals:
    // tup49 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull], Int[nonnull], Int[nonnull], Int[nonnull]] (persists=false)
    fun select_0 () : Tuple[Int[nonnull],
        Int[nonnull]] {
        init tuple_1();
        loop (not done(tuple_1)) {
            tup49 = next(tuple_1);
            if (not done(tuple_1)) {
                yield (tup49[1], tup49[5]);
            } else {

            }
        }
    }
    // Locals:
    // tup52 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    fun printer () : Void {
        init select_0();
        loop (not done(select_0)) {
            tup52 = next(select_0);
            if (not done(select_0)) {
                print(Tuple[Int[nonnull], Int[nonnull]], tup52);
            } else {

            }
        }
    }
    // Locals:
    // tup51 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    // c50 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c50 = 0;
        init select_0();
        loop (not done(select_0)) {
            tup51 = next(select_0);
            if (not done(select_0)) {
                c50 = c50 + 1;
            } else {

            }
        }
        return c50;
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
    // start : Int[nonnull] (persists=true)
    fun scalar_4 (start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // pcount5 : Int[nonnull] (persists=true)
    // tup6 : Tuple[Int[nonnull]] (persists=false)
    // cstart3 : Int[nonnull] (persists=true)
    fun list_2 () : Tuple[Int[nonnull]] {
        cstart3 = 0;
        pcount5 = 5;
        loop (0 < pcount5) {
            init scalar_4(cstart3);
            tup6 = next(scalar_4);
            yield tup6;
            cstart3 = cstart3 + 1;
            pcount5 = pcount5 - 1;
        }
    }
    // Locals:
    // start : Int[nonnull] (persists=true)
    // log_id : Int[nonnull] (persists=true)
    fun scalar_12 (log_id,
        start) : Tuple[Int[nonnull]] {
        yield (buf[start : 1]);
    }
    // Locals:
    // cstart11 : Int[nonnull] (persists=true)
    // log_id : Int[nonnull] (persists=true)
    // tup14 : Tuple[Int[nonnull]] (persists=false)
    // pcount13 : Int[nonnull] (persists=true)
    fun list_10 (log_id) : Tuple[Int[nonnull]] {
        cstart11 = 0;
        pcount13 = 5;
        loop (0 < pcount13) {
            init scalar_12(log_id, cstart11);
            tup14 = next(scalar_12);
            yield tup14;
            cstart11 = cstart11 + 1;
            pcount13 = pcount13 - 1;
        }
    }
    // Locals:
    // tup18 : Tuple[Int[nonnull]] (persists=false)
    // found_tup17 : Bool[nonnull] (persists=false)
    // min19 : Int[nonnull] (persists=false)
    // count20 : Int[nonnull] (persists=true)
    // tup16 : Tuple[Int[nonnull]] (persists=false)
    // log_id : Int[nonnull] (persists=true)
    fun select_9 (log_id) : Tuple[Int[nonnull]] {
        found_tup17 = false;
        init list_10(log_id);
        min19 = 4611686018427387903;
        count20 = 5;
        loop (0 < count20) {
            tup18 = next(list_10);
            min19 = tup18[0] < min19 ? tup18[0] : min19;
            tup16 = tup18;
            found_tup17 = true;
            count20 = count20 - 1;
        }
        if (found_tup17) {
            yield (min19);
        } else {

        }
    }
    // Locals:
    // count8 : Int[nonnull] (persists=true)
    // tup7 : Tuple[Int[nonnull]] (persists=false)
    // tup21 : Tuple[Int[nonnull]] (persists=true)
    fun filter_1 () : Tuple[Int[nonnull]] {
        init list_2();
        count8 = 5;
        loop (0 < count8) {
            tup7 = next(list_2);
            init select_9(tup7[0]);
            tup21 = next(select_9);
            if (tup21[0] = tup7[0]) {
                yield tup7;
            } else {

            }
            count8 = count8 - 1;
        }
    }
    // Locals:
    // tup22 : Tuple[Int[nonnull]] (persists=false)
    fun select_0 () : Tuple[Int[nonnull]] {
        init filter_1();
        loop (not done(filter_1)) {
            tup22 = next(filter_1);
            if (not done(filter_1)) {
                yield (tup22[0]);
            } else {

            }
        }
    }
    // Locals:
    // tup25 : Tuple[Int[nonnull]] (persists=false)
    fun printer () : Void {
        init select_0();
        loop (not done(select_0)) {
            tup25 = next(select_0);
            if (not done(select_0)) {
                print(Tuple[Int[nonnull]], tup25);
            } else {

            }
        }
    }
    // Locals:
    // tup24 : Tuple[Int[nonnull]] (persists=false)
    // c23 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c23 = 0;
        init select_0();
        loop (not done(select_0)) {
            tup24 = next(select_0);
            if (not done(select_0)) {
                c23 = c23 + 1;
            } else {

            }
        }
        return c23;
    } |}]
