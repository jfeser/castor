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

let%expect_test "example-3" =
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
select([lp.counter, lc.counter],
  atuple([ahashidx(select([id as k], log), 
    alist(select([counter, succ], 
        filter(k = id && counter > succ, log)), 
      atuple([ascalar(counter), ascalar(succ)], cross)), 
    id_p) as lp,
  filter(lc.id = id_c,
    aorderedidx(select([log.counter as k], log), 
      alist(filter(log.counter = k, log),
        atuple([ascalar(log.id), ascalar(log.counter)], cross)), 
      lp.counter, lp.succ) as lc)], cross))
|}
    |> M.resolve ~params |> M.annotate_schema |> M.annotate_key_layouts
  in
  I.irgen_abstract ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter;
  [%expect {|
    fun scalar_4 (start) {
        yield (buf[start : 1]);
    }fun tuple_6 (start) {

    }fun list_5 (k,
         start) {
         cstart = start + 16;
         pcount = buf[start : 8];
         loop (0 < pcount) {
             init tuple_6(cstart, k);
             cstart = cstart + 0;
             pcount = pcount - 1;
         }
    }fun hash_idx_3 (start) {
         if (buf[start + 16 + buf[start + 8 : 8] + hash(start + 16, id_p) * 8 :
             8] = 0) {

         } else {
              kstart = buf[start + 16 + buf[start + 8 : 8] + hash(start +
              16, id_p) * 8 : 8];
              init scalar_4(kstart);
              key = next(scalar_4);
              vstart = buf[start + 16 + buf[start + 8 : 8] + hash(start +
              16, id_p) * 8 : 8] + 1;
              if (key = (id_p)) {
                  init list_5(vstart, key[0]);
              } else {

              }
         }
    }fun scalar_12 (start) {
         yield (buf[start : 1]);
    }fun scalar_16 (start) {
         yield (buf[start : 1]);
    }fun scalar_18 (start) {
         yield (buf[start : 1]);
    }fun tuple_15 (k,
         start,
         lp_counter,
         lp_k,
         lp_succ) {
         init scalar_16(lp_succ, lp_k, lp_counter, start + 8, k);
         tup17 = next(scalar_16);
         init scalar_18(lp_succ, lp_k, lp_counter, tup17[0], start + 8 + 1, k);
         tup19 = next(scalar_18);
         yield (tup17[0], tup19[0]);
    }fun list_13 (k,
         start,
         lp_counter,
         lp_k,
         lp_succ) {
         cstart = start + 16;
         pcount = buf[start : 8];
         loop (0 < pcount) {
             init tuple_15(lp_succ, lp_k, lp_counter, cstart, k);
             tup20 = next(tuple_15);
             yield tup20;
             cstart = cstart + buf[cstart : 8];
             pcount = pcount - 1;
         }
    }fun ordered_idx_11 (start,
         lp_counter,
         lp_k,
         lp_succ) {
         low21 = 0;
         high22 = buf[start + 8 : 8] / 9;
         loop (low21 < high22) {
             mid23 = low21 + high22 / 2;
             kstart = start + 16 + mid23 * 9;
             init scalar_12(lp_succ, lp_k, lp_counter, kstart);
             key24 = next(scalar_12);
             if (key24 < lp_counter) {
                 low21 = mid23 + 1;
             } else {
                  high22 = mid23;
             }
         }
         if (low21 < buf[start + 8 : 8] / 9) {
             kstart = start + 16 + low21 * 9;
             init scalar_12(lp_succ, lp_k, lp_counter, kstart);
             key25 = next(scalar_12);
             loop (key25 < lp_succ) {
                 vstart = buf[start + 16 + low21 * 9 + 1 : 8];
                 init list_13(lp_succ, lp_k, lp_counter, vstart, key[0]);
                 loop (not done(list_13)) {
                     tup26 = next(list_13);
                     if (not done(list_13)) {
                         yield tup26;
                     } else {

                     }
                 }
                 low21 = low21 + 1;
             }
         } else {

         }
    }fun filter_10 (start,
         lp_counter,
         lp_k,
         lp_succ) {
         init ordered_idx_11(lp_succ, lp_k, lp_counter, start);
         loop (not done(ordered_idx_11)) {
             tup27 = next(ordered_idx_11);
             if (not done(ordered_idx_11)) {
                 if (tup27[1] = id_c) {
                     yield tup27;
                 } else {

                 }
             } else {

             }
         }
    }fun tuple_2 () {
         init hash_idx_3(8);
         loop (not done(hash_idx_3)) {
             tup9 = next(hash_idx_3);
             if (not done(hash_idx_3)) {
                 init filter_10(tup9[2], tup9[0], tup9[1], 8 + buf[8 : 8]);
                 loop (not done(filter_10)) {
                     tup28 = next(filter_10);
                     if (not done(filter_10)) {
                         yield (tup9[0], tup28[0], tup28[1], tup28[2]);
                     } else {

                     }
                 }
             } else {

             }
         }
    }fun select_0 () {
         init tuple_2();
         loop (not done(tuple_2)) {
             tup29 = next(tuple_2);
             if (not done(tuple_2)) {
                 yield (tup29[1], tup29[5]);
             } else {

             }
         }
    }fun printer () {
         init select_0();
         loop (not done(select_0)) {
             tup31 = next(select_0);
             if (not done(select_0)) {
                 print(Tuple[Int, Int], tup31);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init select_0();
         loop (not done(select_0)) {
             tup30 = next(select_0);
             if (not done(select_0)) {
                 c = c + 1;
             } else {

             }
         }
         return c;
    } |}]
