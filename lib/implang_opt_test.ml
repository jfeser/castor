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
    opt ;
  [%expect
    {|
    // Locals:
    // cstart12 : Int[nonnull] (persists=true)
    // cstart14 : Int[nonnull] (persists=false)
    // cstart15 : Int[nonnull] (persists=false)
    // pcount20 : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // tup17 : Tuple[Int[nonnull]] (persists=false)
    // tup19 : Tuple[Int[nonnull]] (persists=false)
    // tup21 : Tuple[Int[nonnull], Int[nonnull]] (persists=false)
    fun list_11 (start) : Tuple[Int[nonnull],
        Int[nonnull]] {
        cstart12 = start + 1 + 1;
        pcount20 = buf[start : 1];
        loop (0 < pcount20) {
            cstart14 = cstart12;
            cstart15 = cstart14 + 1;
            tup17 = (buf[cstart14 : 1]);
            tup19 = (buf[cstart15 : 1]);
            tup21 = (tup17[0], tup19[0]);
            yield tup21;
            cstart12 = cstart12 + 2;
            pcount20 = pcount20 - 1;
        }
    }
    // Locals:
    // cstart4 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // start : Int[nonnull] (persists=true)
    // tup10 : Tuple[Int[nonnull]] (persists=true)
    // tup22 : Tuple[Int[nonnull], Int[nonnull]] (persists=true)
    // tup8 : Tuple[Int[nonnull]] (persists=true)
    fun tuple_3 (start) : Tuple[Int[nonnull],
        Int[nonnull],
        Int[nonnull],
        Int[nonnull]] {
        cstart4 = start + 1;
        cstart5 = cstart4 + 1;
        cstart6 = cstart5 + 1;
        tup8 = (buf[cstart4 : 1]);
        tup10 = (buf[cstart5 : 1]);
        init list_11(cstart6);
        loop (not done(list_11)) {
            tup22 = next(list_11);
            if (not done(list_11)) {
                yield (tup8[0], tup10[0], tup22[0], tup22[1]);
            } else {

            }
        }
    }
    // Locals:
    // cstart2 : Int[nonnull] (persists=true)
    // pcount23 : Int[nonnull] (persists=true)
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
    // count26 : Int[nonnull] (persists=true)
    // tup25 : Tuple[Int[nonnull], Int[nonnull], Int[nonnull], Int[nonnull]] (persists=false)
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
