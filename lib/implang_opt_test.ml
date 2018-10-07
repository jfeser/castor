open Core
open Base
open Abslayout
open Implang_test
open Implang_opt

let run_test ?(params = []) layout_str opt_func =
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
  I.irgen ~params ~data_fn:"/tmp/buf" layout
  |> opt_func
  |> I.pp Caml.Format.std_formatter

let%expect_test "example-1" =
  run_test ~params:example_params
    {|
filter(lc.id = id_c && lp.id = id_p,
alist(filter(succ > counter + 1, log) as lp,
atuple([ascalar(lp.id), ascalar(lp.counter),
alist(filter(lp.counter < log.counter &&
log.counter < lp.succ, log) as lc,
atuple([ascalar(lc.id), ascalar(lc.counter)], cross))], cross)))
|}
    prune_args;
  [%expect {|
    fun scalar_3 (start) {
        yield (buf[start : 1]);
    }fun scalar_4 (start) {
         yield (buf[start : 1]);
    }fun scalar_7 (start) {
         yield (buf[start : 1]);
    }fun scalar_8 (start) {
         yield (buf[start : 1]);
    }fun tuple_6 (start) {
         cstart0 = start;
         cstart1 = cstart0 + 1;
         init scalar_7(cstart0);
         tup2 = next(scalar_7);
         init scalar_8(cstart1);
         tup3 = next(scalar_8);
         yield (tup2[0], tup3[0]);
    }fun list_5 (start) {
         cstart = start + 1 + 1;
         pcount = buf[start : 1];
         loop (0 < pcount) {
             init tuple_6(cstart);
             tup0 = next(tuple_6);
             yield tup0;
             cstart = cstart + 2;
             pcount = pcount - 1;
         }
    }fun tuple_2 (start) {
         cstart0 = start + 1;
         cstart1 = cstart0 + 1;
         cstart2 = cstart1 + 1;
         init scalar_3(cstart0);
         tup3 = next(scalar_3);
         init scalar_4(cstart1);
         tup4 = next(scalar_4);
         init list_5(cstart2);
         loop (not done(list_5)) {
             tup5 = next(list_5);
             if (not done(list_5)) {
                 yield (tup3[0], tup4[0], tup5[0], tup5[1]);
             } else {

             }
         }
    }fun list_1 () {
         cstart = 1;
         pcount = 3;
         loop (0 < pcount) {
             init tuple_2(cstart);
             loop (not done(tuple_2)) {
                 tup0 = next(tuple_2);
                 if (not done(tuple_2)) {
                     yield tup0;
                 } else {

                 }
             }
             cstart = cstart + buf[cstart : 1];
             pcount = pcount - 1;
         }
    }fun filter_0 () {
         init list_1();
         count1 = 3;
         loop (0 < count1) {
             tup0 = next(list_1);
             if (tup0[2] = id_c && tup0[0] = id_p) {
                 yield tup0;
             } else {

             }
             count1 = count1 - 1;
         }
    }fun printer () {
         init filter_0();
         loop (not done(filter_0)) {
             tup0 = next(filter_0);
             if (not done(filter_0)) {
                 print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                 Int[nonnull]], tup0);
             } else {

             }
         }
    }fun counter () {
         c = 0;
         init filter_0();
         loop (not done(filter_0)) {
             tup0 = next(filter_0);
             if (not done(filter_0)) {
                 c = c + 1;
             } else {

             }
         }
         return c;
    } |}]
