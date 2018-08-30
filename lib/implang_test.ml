open Core
open Base
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

module Eval = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (Eval)

module I =
  Implang.IRGen.Make (struct
      let code_only = true
    end)
    (Eval)
    ()

let _ =
  Test_util.create rels "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

let _ =
  Test_util.create rels "log" ["id"; "succ"; "counter"]
    [[1; 4; 1]; [2; 3; 2]; [3; 4; 3]; [4; 6; 1]; [5; 6; 3]]

let%expect_test "cross-tuple" =
  let layout =
    of_string_exn "AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross))"
    |> M.resolve |> M.annotate_schema |> annotate_key_layouts
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
  let layout =
    of_string_exn
      "ATuple([AList(r1, AScalar(r1.f)) as f, AHashIdx(dedup(select([r1.f], r1)) \
       as k, ascalar(k.f+1), f.f)], cross)"
    |> M.resolve |> M.annotate_schema |> annotate_key_layouts
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
    }fun scalar_13 (start) {
         yield (buf[start : 1]);
    }fun list_12 (start) {
         cstart = start + 16;
         pcount = buf[start : 8];
         loop (0 < pcount) {
             init scalar_13(cstart);
             tup14 = next(scalar_13);
             yield tup14;
             cstart = cstart + 1;
             pcount = pcount - 1;
         }
    }fun scalar_18 (start) {
         yield (buf[start : 1]);
    }fun scalar_19 (start) {
         yield (buf[start : 1]);
    }fun hash_idx_17 (start,
         f_f) {
         if (buf[start + 16 + buf[start + 8 : 8] + hash(start + 16, f_f) * 8 :
             8] = 0) {

         } else {
              kstart = buf[start + 16 + buf[start + 8 : 8] + hash(start +
              16, f_f) * 8 : 8];
              init scalar_18(f_f, kstart);
              key = next(scalar_18);
              vstart = buf[start + 16 + buf[start + 8 : 8] + hash(start +
              16, f_f) * 8 : 8] + 1;
              if (key[0] = f_f) {
                  init scalar_19(key[0], f_f, vstart);
                  tup20 = next(scalar_19);
                  yield tup20;
              } else {

              }
         }
    }fun tuple_11 () {
         init list_12(8);
         count16 = 5;
         loop (0 < count16) {
             tup15 = next(list_12);
             init hash_idx_17(tup15[0], 8 + buf[8 : 8]);
             loop (not done(hash_idx_17)) {
                 tup21 = next(hash_idx_17);
                 if (not done(hash_idx_17)) {
                     yield (tup15[0], tup21[0], tup21[1]);
                 } else {

                 }
             }
             count16 = count16 - 1;
         }
    }fun printer () {
         init tuple_11();
         loop (not done(tuple_11)) {
             tup23 = next(tuple_11);
             if (not done(tuple_11)) {
                 print(Tuple[Int, Int, Int[nonnull]], tup23);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init tuple_11();
         loop (not done(tuple_11)) {
             tup22 = next(tuple_11);
             if (not done(tuple_11)) {
                 c = c + 1;
             } else {

             }
         }
         return c;
    } |}]

let%expect_test "example-1" =
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
    |> M.resolve ~params |> M.annotate_schema |> annotate_key_layouts
  in
  I.irgen_abstract ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter

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
