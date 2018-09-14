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
  annotate_foreach layout ;
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
  annotate_foreach layout ;
  I.irgen ~params ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter

let%expect_test "cross-tuple" =
  run_test "AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross))" ;
  [%expect
    {|
    fun scalar_4 (start) {
        yield (buf[start : 1]);
    }fun scalar_6 (r1_f,
         start) {
         yield (buf[start : 1]);
    }fun tuple_1 (start) {
         cstart2 = start;
         cstart3 = cstart2 + 1;
         init scalar_4(cstart2);
         tup5 = next(scalar_4);
         init scalar_6(tup5[0], cstart3);
         tup7 = next(scalar_6);
         yield (tup5[0], tup7[0]);
    }fun list_0 () {
         cstart = 0;
         pcount = 5;
         loop (0 < pcount) {
             init tuple_1(cstart);
             tup8 = next(tuple_1);
             yield tup8;
             cstart = cstart + 2;
             pcount = pcount - 1;
         }
    }fun printer () {
         init list_0();
         loop (not done(list_0)) {
             tup10 = next(list_0);
             if (not done(list_0)) {
                 print(Tuple[Int[nonnull], Int[nonnull]], tup10);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init list_0();
         loop (not done(list_0)) {
             tup9 = next(list_0);
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
    fun scalar_4 (start) {
        yield (buf[start : 1]);
    }fun list_3 (start) {
         cstart = start;
         pcount = 5;
         loop (0 < pcount) {
             init scalar_4(cstart);
             tup5 = next(scalar_4);
             yield tup5;
             cstart = cstart + 1;
             pcount = pcount - 1;
         }
    }fun scalar_9 (f_f,
         start) {
         yield (buf[start : 1]);
    }fun scalar_10 (k_f,
         f_f,
         start) {
         yield (buf[start : 1]);
    }fun hash_idx_8 (f_f,
         start) {
         if (hash(start + 3 + 8, f_f) * 8 < 0 || buf[start + 3 + 8 + buf[start +
             3 : 8] : 8] - 1 < hash(start + 3 + 8, f_f) * 8 || buf[start + 3 +
             8 + buf[start + 3 : 8] + 8 + hash(start + 3 + 8, f_f) * 8 : 8] = 0) {

         } else {
              kstart = buf[start + 3 + 8 + buf[start + 3 : 8] + 8 + hash(start +
              3 + 8, f_f) * 8 : 8];
              init scalar_9(f_f, kstart);
              key = next(scalar_9);
              vstart = buf[start + 3 + 8 + buf[start + 3 : 8] + 8 + hash(start +
              3 + 8, f_f) * 8 : 8] + 1;
              if (true && key[0] = f_f) {
                  init scalar_10(key[0], f_f, vstart);
                  tup11 = next(scalar_10);
                  yield (key[0], tup11[0]);
              } else {

              }
         }
    }fun tuple_0 () {
         cstart1 = 3;
         cstart2 = cstart1 + 5;
         init list_3(cstart1);
         count7 = 5;
         loop (0 < count7) {
             tup6 = next(list_3);
             init hash_idx_8(tup6[0], cstart2);
             loop (not done(hash_idx_8)) {
                 tup12 = next(hash_idx_8);
                 if (not done(hash_idx_8)) {
                     yield (tup6[0], tup12[0], tup12[1]);
                 } else {

                 }
             }
             count7 = count7 - 1;
         }
    }fun printer () {
         init tuple_0();
         loop (not done(tuple_0)) {
             tup14 = next(tuple_0);
             if (not done(tuple_0)) {
                 print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull]], tup14);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init tuple_0();
         loop (not done(tuple_0)) {
             tup13 = next(tuple_0);
             if (not done(tuple_0)) {
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
    fun scalar_6 (start) {
        yield (buf[start : 1]);
    }fun scalar_8 (lp_id,
         start) {
         yield (buf[start : 1]);
    }fun scalar_14 (lp_id,
         lp_counter,
         start) {
         yield (buf[start : 1]);
    }fun scalar_16 (lp_id,
         lp_counter,
         lc_id,
         start) {
         yield (buf[start : 1]);
    }fun tuple_11 (lp_id,
         lp_counter,
         start) {
         cstart12 = start;
         cstart13 = cstart12 + 1;
         init scalar_14(lp_id, lp_counter, cstart12);
         tup15 = next(scalar_14);
         init scalar_16(lp_id, lp_counter, tup15[0], cstart13);
         tup17 = next(scalar_16);
         yield (tup15[0], tup17[0]);
    }fun list_10 (lp_id,
         lp_counter,
         start) {
         cstart = start + 1 + 1;
         pcount = buf[start : 1];
         loop (0 < pcount) {
             init tuple_11(lp_id, lp_counter, cstart);
             tup18 = next(tuple_11);
             yield tup18;
             cstart = cstart + 2;
             pcount = pcount - 1;
         }
    }fun tuple_2 (start) {
         cstart3 = start + 1;
         cstart4 = cstart3 + 1;
         cstart5 = cstart4 + 1;
         init scalar_6(cstart3);
         tup7 = next(scalar_6);
         init scalar_8(tup7[0], cstart4);
         tup9 = next(scalar_8);
         init list_10(tup7[0], tup9[0], cstart5);
         loop (not done(list_10)) {
             tup19 = next(list_10);
             if (not done(list_10)) {
                 yield (tup7[0], tup9[0], tup19[0], tup19[1]);
             } else {

             }
         }
    }fun list_1 () {
         cstart = 1;
         pcount = 3;
         loop (0 < pcount) {
             init tuple_2(cstart);
             loop (not done(tuple_2)) {
                 tup20 = next(tuple_2);
                 if (not done(tuple_2)) {
                     yield tup20;
                 } else {

                 }
             }
             cstart = cstart + buf[cstart : 1];
             pcount = pcount - 1;
         }
    }fun filter_0 () {
         init list_1();
         count22 = 3;
         loop (0 < count22) {
             tup21 = next(list_1);
             if (tup21[2] = id_c && tup21[0] = id_p) {
                 yield tup21;
             } else {

             }
             count22 = count22 - 1;
         }
    }fun printer () {
         init filter_0();
         loop (not done(filter_0)) {
             tup24 = next(filter_0);
             if (not done(filter_0)) {
                 print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                 Int[nonnull]], tup24);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init filter_0();
         loop (not done(filter_0)) {
             tup23 = next(filter_0);
             if (not done(filter_0)) {
                 c = c + 1;
             } else {

             }
         }
         return c;
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
    fun scalar_4 (start) {
        yield (buf[start : 1]);
    }fun scalar_6 (start,
         lp_k) {
         yield (buf[start : 1]);
    }fun tuple_1 (start) {
         cstart2 = start;
         cstart3 = cstart2 + 1;
         init scalar_4(cstart2);
         tup5 = next(scalar_4);
         init scalar_6(cstart3, tup5[0]);
         tup7 = next(scalar_6);
         yield (tup5[0], tup7[0]);
    }fun scalar_12 (start,
         lp_k,
         lc_k) {
         yield (buf[start : 1]);
    }fun scalar_14 (lp_counter,
         start,
         lp_k,
         lc_k) {
         yield (buf[start : 1]);
    }fun tuple_9 (start,
         lp_k,
         lc_k) {
         cstart10 = start;
         cstart11 = cstart10 + 1;
         init scalar_12(cstart10, lp_k, lc_k);
         tup13 = next(scalar_12);
         init scalar_14(tup13[0], cstart11, lp_k, lc_k);
         tup15 = next(scalar_14);
         yield (tup13[0], tup15[0]);
    }fun list_8 (start,
         lp_k,
         lc_k) {
         cstart = start + 1 + 1;
         pcount = buf[start : 1];
         loop (0 < pcount) {
             init tuple_9(cstart, lp_k, lc_k);
             tup16 = next(tuple_9);
             yield tup16;
             cstart = cstart + 2;
             pcount = pcount - 1;
         }
    }fun hash_idx_0 () {
         if (hash(11, (id_p, id_c)) * 8 < 0 || buf[11 + buf[3 : 8] : 8] - 1 <
             hash(11, (id_p, id_c)) * 8 || buf[11 + buf[3 : 8] + 8 +
             hash(11, (id_p, id_c)) * 8 : 8] = 0) {

         } else {
              kstart = buf[11 + buf[3 : 8] + 8 + hash(11, (id_p, id_c)) * 8 : 8];
              init tuple_1(kstart);
              key = next(tuple_1);
              vstart = buf[11 + buf[3 : 8] + 8 + hash(11, (id_p, id_c)) * 8 :
              8] + 2;
              if (true && key[0] = id_p && key[1] = id_c) {
                  init list_8(vstart, key[0], key[1]);
                  loop (not done(list_8)) {
                      tup17 = next(list_8);
                      if (not done(list_8)) {
                          yield (key[0], key[1], tup17[0], tup17[1]);
                      } else {

                      }
                  }
              } else {

              }
         }
    }fun printer () {
         init hash_idx_0();
         loop (not done(hash_idx_0)) {
             tup19 = next(hash_idx_0);
             if (not done(hash_idx_0)) {
                 print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                 Int[nonnull]], tup19);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init hash_idx_0();
         loop (not done(hash_idx_0)) {
             tup18 = next(hash_idx_0);
             if (not done(hash_idx_0)) {
                 c = c + 1;
             } else {

             }
         }
         return c;
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
    fun scalar_5 (start) {
        yield (buf[start : 1]);
    }fun scalar_10 (start,
         k) {
         yield (buf[start : 1]);
    }fun scalar_12 (log_counter,
         start,
         k) {
         yield (buf[start : 1]);
    }fun tuple_7 (start,
         k) {
         cstart8 = start;
         cstart9 = cstart8 + 1;
         init scalar_10(cstart8, k);
         tup11 = next(scalar_10);
         init scalar_12(tup11[0], cstart9, k);
         tup13 = next(scalar_12);
         yield (tup11[0], tup13[0]);
    }fun list_6 (start,
         k) {
         cstart = start;
         pcount = 1;
         loop (0 < pcount) {
             init tuple_7(cstart, k);
             tup14 = next(tuple_7);
             yield tup14;
             cstart = cstart + 2;
             pcount = pcount - 1;
         }
    }fun hash_idx_4 (start) {
         if (hash(start + 3 + 8, id_p) * 8 < 0 || buf[start + 3 + 8 + buf[start +
             3 : 8] : 8] - 1 < hash(start + 3 + 8, id_p) * 8 || buf[start + 3 +
             8 + buf[start + 3 : 8] + 8 + hash(start + 3 + 8, id_p) * 8 : 8] = 0) {

         } else {
              kstart = buf[start + 3 + 8 + buf[start + 3 : 8] + 8 + hash(start +
              3 + 8, id_p) * 8 : 8];
              init scalar_5(kstart);
              key = next(scalar_5);
              vstart = buf[start + 3 + 8 + buf[start + 3 : 8] + 8 + hash(start +
              3 + 8, id_p) * 8 : 8] + 1;
              if (true && key[0] = id_p) {
                  init list_6(vstart, key[0]);
                  tup15 = next(list_6);
                  yield (key[0], tup15[0], tup15[1]);
              } else {

              }
         }
    }fun scalar_19 (lp_succ,
         lp_k,
         lp_counter,
         start) {
         yield (buf[start : 1]);
    }fun scalar_24 (lp_succ,
         lp_k,
         lp_counter,
         start,
         k) {
         yield (buf[start : 1]);
    }fun scalar_26 (lp_succ,
         lp_k,
         lp_counter,
         log_id,
         start,
         k) {
         yield (buf[start : 1]);
    }fun tuple_21 (lp_succ,
         lp_k,
         lp_counter,
         start,
         k) {
         cstart22 = start;
         cstart23 = cstart22 + 1;
         init scalar_24(lp_succ, lp_k, lp_counter, cstart22, k);
         tup25 = next(scalar_24);
         init scalar_26(lp_succ, lp_k, lp_counter, tup25[0], cstart23, k);
         tup27 = next(scalar_26);
         yield (tup25[0], tup27[0]);
    }fun list_20 (lp_succ,
         lp_k,
         lp_counter,
         start,
         k) {
         cstart = start + 1 + 1;
         pcount = buf[start : 1];
         loop (0 < pcount) {
             init tuple_21(lp_succ, lp_k, lp_counter, cstart, k);
             tup28 = next(tuple_21);
             yield tup28;
             cstart = cstart + 2;
             pcount = pcount - 1;
         }
    }fun ordered_idx_18 (lp_succ,
         lp_k,
         lp_counter,
         start) {
         low29 = 0;
         high30 = buf[start + 3 : 8] / 9;
         loop (low29 < high30) {
             mid31 = low29 + high30 / 2;
             kstart = start + 3 + 8 + mid31 * 9;
             init scalar_19(lp_succ, lp_k, lp_counter, kstart);
             key32 = next(scalar_19);
             if (key32[0] < lp_counter) {
                 low29 = mid31 + 1;
             } else {
                  high30 = mid31;
             }
         }
         if (low29 < buf[start + 3 : 8] / 9) {
             kstart = start + 3 + 8 + low29 * 9;
             init scalar_19(lp_succ, lp_k, lp_counter, kstart);
             key33 = next(scalar_19);
             key34 = key33;
             loop (key34[0] < lp_succ && low29 < buf[start + 3 : 8] / 9) {
                 vstart = buf[start + 3 + 8 + low29 * 9 + 1 : 8];
                 init list_20(lp_succ, lp_k, lp_counter, vstart, key[0]);
                 loop (not done(list_20)) {
                     tup35 = next(list_20);
                     if (not done(list_20)) {
                         yield (key34[0], tup35[0], tup35[1]);
                     } else {

                     }
                 }
                 low29 = low29 + 1;
                 kstart = start + 3 + 8 + low29 * 9;
                 init scalar_19(lp_succ, lp_k, lp_counter, kstart);
                 key36 = next(scalar_19);
                 key34 = key36;
             }
         } else {

         }
    }fun filter_17 (lp_succ,
         lp_k,
         lp_counter,
         start) {
         init ordered_idx_18(lp_succ, lp_k, lp_counter, start);
         loop (not done(ordered_idx_18)) {
             tup37 = next(ordered_idx_18);
             if (not done(ordered_idx_18)) {
                 if (tup37[1] = id_c) {
                     yield tup37;
                 } else {

                 }
             } else {

             }
         }
    }fun tuple_1 () {
         cstart2 = 3;
         cstart3 = cstart2 + buf[cstart2 : 3];
         init hash_idx_4(cstart2);
         loop (not done(hash_idx_4)) {
             tup16 = next(hash_idx_4);
             if (not done(hash_idx_4)) {
                 init filter_17(tup16[2], tup16[0], tup16[1], cstart3);
                 loop (not done(filter_17)) {
                     tup38 = next(filter_17);
                     if (not done(filter_17)) {
                         yield
                         (tup16[0], tup16[1], tup16[2], tup38[0], tup38[1],
                          tup38[2]);
                     } else {

                     }
                 }
             } else {

             }
         }
    }fun select_0 () {
         init tuple_1();
         loop (not done(tuple_1)) {
             tup39 = next(tuple_1);
             if (not done(tuple_1)) {
                 yield (tup39[1], tup39[5]);
             } else {

             }
         }
    }fun printer () {
         init select_0();
         loop (not done(select_0)) {
             tup41 = next(select_0);
             if (not done(select_0)) {
                 print(Tuple[Int[nonnull], Int[nonnull]], tup41);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init select_0();
         loop (not done(select_0)) {
             tup40 = next(select_0);
             if (not done(select_0)) {
                 c = c + 1;
             } else {

             }
         }
         return c;
    } |}]
