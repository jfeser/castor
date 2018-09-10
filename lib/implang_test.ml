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
  I.irgen ~params ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter

let%expect_test "cross-tuple" =
  run_test "AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross))" ;
  [%expect
    {|
    fun scalar_3 (start) {
        yield (buf[start : 1]);
    }fun scalar_5 (r1_f,
         start) {
         yield (buf[start : 1]);
    }fun tuple_2 (start) {
         init scalar_3(start);
         tup4 = next(scalar_3);
         init scalar_5(tup4[0], start + 1);
         tup6 = next(scalar_5);
         yield (tup4[0], tup6[0]);
    }fun list_0 () {
         cstart = 0;
         pcount = 5;
         loop (0 < pcount) {
             init tuple_2(cstart);
             tup7 = next(tuple_2);
             yield tup7;
             cstart = cstart + 3;
             pcount = pcount - 1;
         }
    }fun printer () {
         init list_0();
         loop (not done(list_0)) {
             tup9 = next(list_0);
             if (not done(list_0)) {
                 print(Tuple[Int[nonnull], Int[nonnull]], tup9);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init list_0();
         loop (not done(list_0)) {
             tup8 = next(list_0);
             if (not done(list_0)) {
                 c = c + 1;
             } else {

             }
         }
         return c;
    } |}]

let%expect_test "hash-idx" =
  run_test
    "ATuple([AList(r1, AScalar(r1.f)) as f, AHashIdx(dedup(select([r1.f], r1)) as \
     k, ascalar(k.f+1), f.f)], cross)" ;
  [%expect
    {|
    fun scalar_3 (start) {
        yield (buf[start : 1]);
    }fun list_2 (start) {
         cstart = start;
         pcount = 5;
         loop (0 < pcount) {
             init scalar_3(cstart);
             tup4 = next(scalar_3);
             yield tup4;
             cstart = cstart + 1;
             pcount = pcount - 1;
         }
    }fun scalar_8 (f_f,
         start) {
         yield (buf[start : 1]);
    }fun scalar_9 (k_f,
         f_f,
         start) {
         yield (buf[start : 1]);
    }fun hash_idx_7 (f_f,
         start) {
         if (buf[start + 2 + 8 + buf[start + 2 : 8] + hash(start + 2 + 8, f_f) *
             8 : 8] = 0) {

         } else {
              kstart = buf[start + 2 + 8 + buf[start + 2 : 8] + hash(start + 2 +
              8, f_f) * 8 : 8];
              init scalar_8(f_f, kstart);
              key = next(scalar_8);
              vstart = buf[start + 2 + 8 + buf[start + 2 : 8] + hash(start + 2 +
              8, f_f) * 8 : 8] + 1;
              if (true && key[0] = f_f) {
                  init scalar_9(key[0], f_f, vstart);
                  tup10 = next(scalar_9);
                  yield (key[0], tup10[0]);
              } else {

              }
         }
    }fun tuple_1 () {
         init list_2(2);
         count6 = 5;
         loop (0 < count6) {
             tup5 = next(list_2);
             init hash_idx_7(tup5[0], 9);
             loop (not done(hash_idx_7)) {
                 tup11 = next(hash_idx_7);
                 if (not done(hash_idx_7)) {
                     yield (tup5[0], tup11[0], tup11[1]);
                 } else {

                 }
             }
             count6 = count6 - 1;
         }
    }fun printer () {
         init tuple_1();
         loop (not done(tuple_1)) {
             tup13 = next(tuple_1);
             if (not done(tuple_1)) {
                 print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull]], tup13);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init tuple_1();
         loop (not done(tuple_1)) {
             tup12 = next(tuple_1);
             if (not done(tuple_1)) {
                 c = c + 1;
             } else {

             }
         }
         return c;
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
    fun scalar_4 (start) {
        yield (buf[start : 1]);
    }fun scalar_6 (lp_id,
         start) {
         yield (buf[start : 1]);
    }fun scalar_11 (lp_id,
         lp_counter,
         start) {
         yield (buf[start : 1]);
    }fun scalar_13 (lp_id,
         lp_counter,
         lc_id,
         start) {
         yield (buf[start : 1]);
    }fun tuple_10 (lp_id,
         lp_counter,
         start) {
         init scalar_11(lp_id, lp_counter, start);
         tup12 = next(scalar_11);
         init scalar_13(lp_id, lp_counter, tup12[0], start + 1);
         tup14 = next(scalar_13);
         yield (tup12[0], tup14[0]);
    }fun list_8 (lp_id,
         lp_counter,
         start) {
         cstart = start + 1;
         pcount = buf[start : 1];
         loop (0 < pcount) {
             init tuple_10(lp_id, lp_counter, cstart);
             tup15 = next(tuple_10);
             yield tup15;
             cstart = cstart + 3;
             pcount = pcount - 1;
         }
    }fun tuple_3 (start) {
         init scalar_4(start);
         tup5 = next(scalar_4);
         init scalar_6(tup5[0], start + 1);
         tup7 = next(scalar_6);
         init list_8(tup5[0], tup7[0], start + 1 + 1);
         loop (not done(list_8)) {
             tup16 = next(list_8);
             if (not done(list_8)) {
                 yield (tup5[0], tup7[0], tup16[0], tup16[1]);
             } else {

             }
         }
    }fun list_1 () {
         cstart = 0;
         pcount = 3;
         loop (0 < pcount) {
             init tuple_3(cstart);
             loop (not done(tuple_3)) {
                 tup17 = next(tuple_3);
                 if (not done(tuple_3)) {
                     yield tup17;
                 } else {

                 }
             }
             cstart = cstart + 5;
             pcount = pcount - 1;
         }
    }fun filter_0 () {
         init list_1();
         count19 = 3;
         loop (0 < count19) {
             tup18 = next(list_1);
             if (tup18[2] = id_c && tup18[0] = id_p) {
                 yield tup18;
             } else {

             }
             count19 = count19 - 1;
         }
    }fun printer () {
         init filter_0();
         loop (not done(filter_0)) {
             tup21 = next(filter_0);
             if (not done(filter_0)) {
                 print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                 Int[nonnull]], tup21);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init filter_0();
         loop (not done(filter_0)) {
             tup20 = next(filter_0);
             if (not done(filter_0)) {
                 c = c + 1;
             } else {

             }
         }
         return c;
    } |}]

let%expect_test "example-1-db" =
  run_test_db ~params:example_db_params
    {|
filter(lc.id = id_c && lp.id = id_p,
alist(filter(succ > counter + 1, log_bench) as lp,
atuple([ascalar(lp.id), ascalar(lp.counter),
alist(filter(lp.counter < log_bench.counter &&
log_bench.counter < lp.succ, log_bench) as lc,
atuple([ascalar(lc.id), ascalar(lc.counter)], cross))], cross)))
|}

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
    fun scalar_3 (start) {
        yield (buf[start : 1]);
    }fun scalar_5 (start,
         lp_k) {
         yield (buf[start : 1]);
    }fun tuple_2 (start) {
         init scalar_3(start);
         tup4 = next(scalar_3);
         init scalar_5(start + 1, tup4[0]);
         tup6 = next(scalar_5);
         yield (tup4[0], tup6[0]);
    }fun scalar_10 (start,
         lp_k,
         lc_k) {
         yield (buf[start : 1]);
    }fun scalar_12 (lp_counter,
         start,
         lp_k,
         lc_k) {
         yield (buf[start : 1]);
    }fun tuple_9 (start,
         lp_k,
         lc_k) {
         init scalar_10(start, lp_k, lc_k);
         tup11 = next(scalar_10);
         init scalar_12(tup11[0], start + 1, lp_k, lc_k);
         tup13 = next(scalar_12);
         yield (tup11[0], tup13[0]);
    }fun list_7 (start,
         lp_k,
         lc_k) {
         cstart = start + 1;
         pcount = buf[start : 1];
         loop (0 < pcount) {
             init tuple_9(cstart, lp_k, lc_k);
             tup14 = next(tuple_9);
             yield tup14;
             cstart = cstart + 3;
             pcount = pcount - 1;
         }
    }fun hash_idx_0 () {
         if (buf[10 + buf[2 : 8] + hash(10, (id_p, id_c)) * 8 : 8] = 0) {

         } else {
              kstart = buf[10 + buf[2 : 8] + hash(10, (id_p, id_c)) * 8 : 8];
              init tuple_2(kstart);
              key = next(tuple_2);
              vstart = buf[10 + buf[2 : 8] + hash(10, (id_p, id_c)) * 8 : 8] + 3;
              if (true && key[0] = id_p && key[1] = id_c) {
                  init list_7(vstart, key[0], key[1]);
                  loop (not done(list_7)) {
                      tup15 = next(list_7);
                      if (not done(list_7)) {
                          yield (key[0], key[1], tup15[0], tup15[1]);
                      } else {

                      }
                  }
              } else {

              }
         }
    }fun printer () {
         init hash_idx_0();
         loop (not done(hash_idx_0)) {
             tup17 = next(hash_idx_0);
             if (not done(hash_idx_0)) {
                 print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                 Int[nonnull]], tup17);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init hash_idx_0();
         loop (not done(hash_idx_0)) {
             tup16 = next(hash_idx_0);
             if (not done(hash_idx_0)) {
                 c = c + 1;
             } else {

             }
         }
         return c;
    } |}]

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
    fun scalar_4 (start) {
        yield (buf[start : 1]);
    }fun scalar_8 (start,
         k) {
         yield (buf[start : 1]);
    }fun scalar_10 (log_counter,
         start,
         k) {
         yield (buf[start : 1]);
    }fun tuple_7 (start,
         k) {
         init scalar_8(start, k);
         tup9 = next(scalar_8);
         init scalar_10(tup9[0], start + 1, k);
         tup11 = next(scalar_10);
         yield (tup9[0], tup11[0]);
    }fun list_5 (start,
         k) {
         cstart = start;
         pcount = 1;
         loop (0 < pcount) {
             init tuple_7(cstart, k);
             tup12 = next(tuple_7);
             yield tup12;
             cstart = cstart + 3;
             pcount = pcount - 1;
         }
    }fun hash_idx_3 (start) {
         if (buf[start + 2 + 8 + buf[start + 2 : 8] + hash(start + 2 + 8, id_p) *
             8 : 8] = 0) {
  
         } else {
              kstart = buf[start + 2 + 8 + buf[start + 2 : 8] + hash(start + 2 +
              8, id_p) * 8 : 8];
              init scalar_4(kstart);
              key = next(scalar_4);
              vstart = buf[start + 2 + 8 + buf[start + 2 : 8] + hash(start + 2 +
              8, id_p) * 8 : 8] + 1;
              if (true && key[0] = id_p) {
                  init list_5(vstart, key[0]);
                  tup13 = next(list_5);
                  yield (key[0], tup13[0], tup13[1]);
              } else {
  
              }
         }
    }fun scalar_17 (lp_succ,
         lp_k,
         lp_counter,
         start) {
         yield (buf[start : 1]);
    }fun scalar_21 (lp_succ,
         lp_k,
         lp_counter,
         start,
         k) {
         yield (buf[start : 1]);
    }fun scalar_23 (lp_succ,
         lp_k,
         lp_counter,
         log_id,
         start,
         k) {
         yield (buf[start : 1]);
    }fun tuple_20 (lp_succ,
         lp_k,
         lp_counter,
         start,
         k) {
         init scalar_21(lp_succ, lp_k, lp_counter, start, k);
         tup22 = next(scalar_21);
         init scalar_23(lp_succ, lp_k, lp_counter, tup22[0], start + 1, k);
         tup24 = next(scalar_23);
         yield (tup22[0], tup24[0]);
    }fun list_18 (lp_succ,
         lp_k,
         lp_counter,
         start,
         k) {
         cstart = start + 1;
         pcount = buf[start : 1];
         loop (0 < pcount) {
             init tuple_20(lp_succ, lp_k, lp_counter, cstart, k);
             tup25 = next(tuple_20);
             yield tup25;
             cstart = cstart + 3;
             pcount = pcount - 1;
         }
    }fun ordered_idx_16 (lp_succ,
         lp_k,
         lp_counter,
         start) {
         low26 = 0;
         high27 = buf[start + 2 : 8] / 9;
         loop (low26 < high27) {
             mid28 = low26 + high27 / 2;
             kstart = start + 2 + 8 + mid28 * 9;
             init scalar_17(lp_succ, lp_k, lp_counter, kstart);
             key29 = next(scalar_17);
             if (key29[0] < lp_counter) {
                 low26 = mid28 + 1;
             } else {
                  high27 = mid28;
             }
         }
         if (low26 < buf[start + 2 : 8] / 9) {
             kstart = start + 2 + 8 + low26 * 9;
             init scalar_17(lp_succ, lp_k, lp_counter, kstart);
             key30 = next(scalar_17);
             key31 = key30;
             loop (key31[0] < lp_succ && low26 < buf[start + 2 : 8] / 9) {
                 vstart = buf[start + 2 + 8 + low26 * 9 + 1 : 8];
                 init list_18(lp_succ, lp_k, lp_counter, vstart, key[0]);
                 loop (not done(list_18)) {
                     tup32 = next(list_18);
                     if (not done(list_18)) {
                         yield (key31[0], tup32[0], tup32[1]);
                     } else {

                     }
                 }
                 low26 = low26 + 1;
                 kstart = start + 2 + 8 + low26 * 9;
                 init scalar_17(lp_succ, lp_k, lp_counter, kstart);
                 key33 = next(scalar_17);
                 key31 = key33;
             }
         } else {

         }
    }fun filter_15 (lp_succ,
         lp_k,
         lp_counter,
         start) {
         init ordered_idx_16(lp_succ, lp_k, lp_counter, start);
         loop (not done(ordered_idx_16)) {
             tup34 = next(ordered_idx_16);
             if (not done(ordered_idx_16)) {
                 if (tup34[1] = id_c) {
                     yield tup34;
                 } else {

                 }
             } else {

             }
         }
    }fun tuple_2 () {
         init hash_idx_3(2);
         loop (not done(hash_idx_3)) {
             tup14 = next(hash_idx_3);
             if (not done(hash_idx_3)) {
                 init filter_15(tup14[2], tup14[0], tup14[1], 2 + buf[2 : 2]);
                 loop (not done(filter_15)) {
                     tup35 = next(filter_15);
                     if (not done(filter_15)) {
                         yield
                         (tup14[0], tup14[1], tup14[2], tup35[0], tup35[1],
                          tup35[2]);
                     } else {

                     }
                 }
             } else {

             }
         }
    }fun select_0 () {
         init tuple_2();
         loop (not done(tuple_2)) {
             tup36 = next(tuple_2);
             if (not done(tuple_2)) {
                 yield (tup36[1], tup36[5]);
             } else {

             }
         }
    }fun printer () {
         init select_0();
         loop (not done(select_0)) {
             tup38 = next(select_0);
             if (not done(select_0)) {
                 print(Tuple[Int[nonnull], Int[nonnull]], tup38);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init select_0();
         loop (not done(select_0)) {
             tup37 = next(select_0);
             if (not done(select_0)) {
                 c = c + 1;
             } else {

             }
         }
         return c;
    } |}]
