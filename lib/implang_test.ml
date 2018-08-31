open Core
open Base
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

module Eval = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (Eval)

let _ =
  Test_util.create rels "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

let _ =
  Test_util.create rels "log" ["id"; "succ"; "counter"]
    [[1; 4; 1]; [2; 3; 2]; [3; 4; 3]; [4; 6; 1]; [5; 6; 3]]

let%expect_test "cross-tuple" =
  let module I =
    Implang.IRGen.Make (struct
        let code_only = true
      end)
      (Eval)
      () in
  let layout =
    of_string_exn "AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross))"
    |> M.resolve |> M.annotate_schema |> M.annotate_key_layouts
  in
  I.irgen_abstract ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter ;
  [%expect
    {|
    fun scalar_3 (start) {
        yield (buf[start : 1]);
    }fun scalar_5 (start) {
         yield (buf[start : 1]);
    }fun tuple_2 (start) {
         init scalar_3(start + 8);
         tup4 = next(scalar_3);
         init scalar_5(tup4[0], start + 8 + 1);
         tup6 = next(scalar_5);
         yield (tup4[0], tup6[0]);
    }fun list_0 () {
         cstart = 16;
         pcount = buf[0 : 8];
         loop (0 < pcount) {
             init tuple_2(cstart);
             tup7 = next(tuple_2);
             yield tup7;
             cstart = cstart + buf[cstart : 8];
             pcount = pcount - 1;
         }
    }fun printer () {
         init list_0();
         loop (not done(list_0)) {
             tup9 = next(list_0);
             if (not done(list_0)) {
                 print(Tuple[Int, Int[nonnull]], tup9);
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
  let module I =
    Implang.IRGen.Make (struct
        let code_only = true
      end)
      (Eval)
      () in
  let layout =
    of_string_exn
      "ATuple([AList(r1, AScalar(r1.f)) as f, AHashIdx(dedup(select([r1.f], r1)) \
       as k, ascalar(k.f+1), f.f)], cross)"
    |> M.resolve |> M.annotate_schema |> M.annotate_key_layouts
  in
  I.irgen_abstract ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter ;
  [%expect
    {|
    fun scalar_3 (start) {
        yield (buf[start : 1]);
    }fun list_2 (start) {
         cstart = start + 16;
         pcount = buf[start : 8];
         loop (0 < pcount) {
             init scalar_3(cstart);
             tup4 = next(scalar_3);
             yield tup4;
             cstart = cstart + 1;
             pcount = pcount - 1;
         }
    }fun scalar_8 (start) {
         yield (buf[start : 1]);
    }fun scalar_9 (start) {
         yield (buf[start : 1]);
    }fun hash_idx_7 (start,
         f_f) {
         if (buf[start + 16 + buf[start + 8 : 8] + hash(start + 16, f_f) * 8 :
             8] = 0) {

         } else {
              kstart = buf[start + 16 + buf[start + 8 : 8] + hash(start +
              16, f_f) * 8 : 8];
              init scalar_8(f_f, kstart);
              key = next(scalar_8);
              vstart = buf[start + 16 + buf[start + 8 : 8] + hash(start +
              16, f_f) * 8 : 8] + 1;
              if (key = (f_f)) {
                  init scalar_9(key[0], f_f, vstart);
                  tup10 = next(scalar_9);
                  yield tup10;
              } else {

              }
         }
    }fun tuple_1 () {
         init list_2(8);
         count6 = 5;
         loop (0 < count6) {
             tup5 = next(list_2);
             init hash_idx_7(tup5[0], 8 + buf[8 : 8]);
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
                 print(Tuple[Int, Int, Int[nonnull]], tup13);
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

let%expect_test "example-1" =
  let module I =
    Implang.IRGen.Make (struct
        let code_only = true
      end)
      (Eval)
      () in
  let params =
    [ Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_p"
    ; Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_c" ]
    |> Set.of_list (module Name.Compare_no_type)
  in
  let layout =
    of_string_exn
      {|
filter(lc.id = id_c && lp.id = id_p,
alist(filter(succ > counter + 1, log) as lp,
atuple([ascalar(lp.id), ascalar(lp.counter),
alist(filter(lp.counter < log.counter &&
log.counter < lp.succ, log) as lc,
atuple([ascalar(lc.id), ascalar(lc.counter)], cross))], cross)))
|}
    |> M.resolve ~params |> M.annotate_schema |> M.annotate_key_layouts
  in
  I.irgen_abstract ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter ;
  [%expect
    {|
    fun scalar_4 (start) {
        yield (buf[start : 1]);
    }fun scalar_6 (start) {
         yield (buf[start : 1]);
    }fun scalar_11 (start) {
         yield (buf[start : 1]);
    }fun scalar_13 (start) {
         yield (buf[start : 1]);
    }fun tuple_10 (start,
         lp_counter,
         lp_id) {
         init scalar_11(lp_id, lp_counter, start + 8);
         tup12 = next(scalar_11);
         init scalar_13(lp_id, lp_counter, tup12[0], start + 8 + 1);
         tup14 = next(scalar_13);
         yield (tup12[0], tup14[0]);
    }fun list_8 (start,
         lp_counter,
         lp_id) {
         cstart = start + 16;
         pcount = buf[start : 8];
         loop (0 < pcount) {
             init tuple_10(lp_id, lp_counter, cstart);
             tup15 = next(tuple_10);
             yield tup15;
             cstart = cstart + buf[cstart : 8];
             pcount = pcount - 1;
         }
    }fun tuple_3 (start) {
         init scalar_4(start + 8);
         tup5 = next(scalar_4);
         init scalar_6(tup5[0], start + 8 + 1);
         tup7 = next(scalar_6);
         init list_8(tup5[0], tup7[0], start + 8 + 1 + 1);
         loop (not done(list_8)) {
             tup16 = next(list_8);
             if (not done(list_8)) {
                 yield (tup5[0], tup7[0], tup16[0], tup16[1]);
             } else {

             }
         }
    }fun list_1 () {
         cstart = 16;
         pcount = buf[0 : 8];
         loop (0 < pcount) {
             init tuple_3(cstart);
             loop (not done(tuple_3)) {
                 tup17 = next(tuple_3);
                 if (not done(tuple_3)) {
                     yield tup17;
                 } else {

                 }
             }
             cstart = cstart + buf[cstart : 8];
             pcount = pcount - 1;
         }
    }fun filter_0 () {
         init list_1();
         count19 = 6;
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
                 print(Tuple[Int, Int, Int, Int], tup21);
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

let%expect_test "example-2" =
  let module I =
    Implang.IRGen.Make (struct
        let code_only = true
      end)
      (Eval)
      () in
  let params =
    [ Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_p"
    ; Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_c" ]
    |> Set.of_list (module Name.Compare_no_type)
  in
  let layout =
    of_string_exn
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
|}
    |> M.resolve ~params |> M.annotate_schema |> M.annotate_key_layouts
  in
  I.irgen_abstract ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter;
  [%expect {|
    fun scalar_3 (start) {
        yield (buf[start : 1]);
    }fun scalar_5 (start) {
         yield (buf[start : 1]);
    }fun tuple_2 (start) {
         init scalar_3(start + 8);
         tup4 = next(scalar_3);
         init scalar_5(start + 8 + 1, tup4[0]);
         tup6 = next(scalar_5);
         yield (tup4[0], tup6[0]);
    }fun scalar_10 (start) {
         yield (buf[start : 1]);
    }fun scalar_12 (start) {
         yield (buf[start : 1]);
    }fun tuple_9 (lc_k,
         lp_k,
         start) {
         init scalar_10(start + 8, lp_k, lc_k);
         tup11 = next(scalar_10);
         init scalar_12(tup11[0], start + 8 + 1, lp_k, lc_k);
         tup13 = next(scalar_12);
         yield (tup11[0], tup13[0]);
    }fun list_7 (lc_k,
         lp_k,
         start) {
         cstart = start + 16;
         pcount = buf[start : 8];
         loop (0 < pcount) {
             init tuple_9(cstart, lp_k, lc_k);
             tup14 = next(tuple_9);
             yield tup14;
             cstart = cstart + buf[cstart : 8];
             pcount = pcount - 1;
         }
    }fun hash_idx_0 () {
         if (buf[16 + buf[8 : 8] + hash(16, (id_p, id_c)) * 8 : 8] = 0) {

         } else {
              kstart = buf[16 + buf[8 : 8] + hash(16, (id_p, id_c)) * 8 : 8];
              init tuple_2(kstart);
              key = next(tuple_2);
              vstart = buf[16 + buf[8 : 8] + hash(16, (id_p, id_c)) * 8 : 8] +
              buf[buf[16 + buf[8 : 8] + hash(16, (id_p, id_c)) * 8 : 8] : 8];
              if (key = (id_p, id_c)) {
                  init list_7(vstart, key[0], key[1]);
                  loop (not done(list_7)) {
                      tup15 = next(list_7);
                      if (not done(list_7)) {
                          yield tup15;
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
                 print(Tuple[Int, Int, Int, Int], tup17);
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

(* let%expect_test "ordered-idx" =
 *   let layout =
 *     of_string_exn
 *       "AOrderedIdx(OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc) as k, \
 *        AScalar(k.f), 1, 3)"
 *     |> M.resolve |> M.annotate_schema |> annotate_key_layouts
 *   in
 *   I.irgen_abstract ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter ;
 *   [%expect
 *     {|
 *     fun scalar_1 (start) {
 *         yield (buf[start : 1]);
 *     }fun scalar_2 (start) {
 *          yield (buf[start : 1]);
 *     }fun ordered_idx_0 (start) {
 *          low3 = 0;
 *          high4 = buf[start + 8 : 8] / 9;
 *          loop (low3 < high4) {
 *              mid5 = low3 + high4 / 2;
 *              init scalar_1(start + 16 + mid5 * 9);
 *              key6 = next(scalar_1);
 *              if (key6 < 1) {
 *                  low3 = mid5 + 1;
 *              } else {
 *                   high4 = mid5;
 *              }
 *          }
 *          if (low3 < buf[start + 8 : 8] / 9) {
 *              init scalar_1(start + 16 + low3 * 9);
 *              key7 = next(scalar_1);
 *              loop (key7 < 3) {
 *                  init scalar_2(buf[start + 16 + low3 * 9 + 1 : 8]);
 *                  tup8 = next(scalar_2);
 *                  yield tup8;
 *                  low3 = low3 + 1;
 *              }
 *          } else {
 * 
 *          }
 *     }fun wrap_ordered_idx_0 (no_start) {
 *          init ordered_idx_0(0);
 *          loop (not done(ordered_idx_0)) {
 *              tup9 = next(ordered_idx_0);
 *              if (not done(ordered_idx_0)) {
 *                  yield tup9;
 *              } else {
 * 
 *              }
 *          }
 *     }fun printer () {
 *          init wrap_ordered_idx_0(0);
 *          loop (not done(wrap_ordered_idx_0)) {
 *              tup11 = next(wrap_ordered_idx_0);
 *              if (not done(wrap_ordered_idx_0)) {
 *                  print(Tuple[Int[nonnull]], tup11);
 *              } else {
 * 
 *              }
 *          }
 *     }fun counter () {
 *          c = 0;
 *          init wrap_ordered_idx_0(0);
 *          loop (not done(wrap_ordered_idx_0)) {
 *              tup10 = next(wrap_ordered_idx_0);
 *              if (not done(wrap_ordered_idx_0)) {
 *                  c = c + 1;
 *              } else {
 * 
 *              }
 *          }
 *          return c;
 *     } |}] *)
