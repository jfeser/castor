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

[@@@warning "-8"]

let _, [f; _] =
  Test_util.create rels "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

[@@@warning "+8"]

let%expect_test "cross-tuple" =
  let layout =
    of_string_exn "AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross))"
    |> M.resolve |> M.annotate_schema |> annotate_key_layouts
  in
  [%sexp_of : t] layout |> print_s

(* I.irgen_abstract ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter *)
(* let%expect_test "hash-idx" =
 *   let layout =
 *     of_string_exn
 *       "ATuple([AList(r1, AScalar(r1.f)) as f, AHashIdx(dedup(select([r1.f], r1)) \
 *        as k, ascalar(k.f+1), f.f)], cross)"
 *     |> M.resolve |> M.annotate_schema |> annotate_key_layouts
 *   in
 *   [%sexp_of : t] layout |> print_s ;
 *   I.irgen_abstract ~data_fn:"/tmp/buf" layout |> I.pp Caml.Format.std_formatter *)
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
