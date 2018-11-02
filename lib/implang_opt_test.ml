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
  let layout = of_string_exn layout_str |> M.resolve ~params:sparams in
  M.annotate_schema layout ;
  let layout = M.annotate_key_layouts layout in
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
    // cstart18 : Int[nonnull] (persists=true)
    // cstart21 : Int[nonnull] (persists=true)
    // cstart16 : Int[nonnull] (persists=true)
    // cstart12 : Int[nonnull] (persists=true)
    // cstart22 : Int[nonnull] (persists=true)
    // cstart17 : Int[nonnull] (persists=true)
    // count14 : Int[nonnull] (persists=true)
    // cstart15 : Int[nonnull] (persists=true)
    // count20 : Int[nonnull] (persists=true)
    // i13 : Int[nonnull] (persists=true)
    // i19 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart12 = 0;
        i13 = 0;
        count14 = 2;
        loop (i13 < count14) {
            cstart15 = cstart12;
            cstart16 = cstart15 + 1;
            cstart17 = cstart16 + 1;
            cstart18 = cstart17;
            i19 = 0;
            count20 = 3;
            loop (i19 < count20) {
                cstart21 = cstart18;
                cstart22 = cstart21 + 1;
                if (buf[cstart21 : 1] = id_c && buf[cstart15 : 1] = id_p) {
                    print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                    Int[nonnull]],
                    (buf[cstart15 : 1], buf[cstart16 : 1], buf[cstart21 : 1],
                     buf[cstart22 : 1]));
                } else {

                }
                cstart18 = cstart18 + 2;
                i19 = i19 + 1;
            }
            cstart12 = cstart12 + 8;
            i13 = i13 + 1;
        }
    }
    // Locals:
    // cstart7 : Int[nonnull] (persists=true)
    // count9 : Int[nonnull] (persists=true)
    // i2 : Int[nonnull] (persists=true)
    // i8 : Int[nonnull] (persists=true)
    // cstart11 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // count3 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 0;
        i2 = 0;
        count3 = 2;
        loop (i2 < count3) {
            cstart4 = cstart1;
            cstart5 = cstart4 + 1;
            cstart6 = cstart5 + 1;
            cstart7 = cstart6;
            i8 = 0;
            count9 = 3;
            loop (i8 < count9) {
                cstart10 = cstart7;
                cstart11 = cstart10 + 1;
                if (buf[cstart10 : 1] = id_c && buf[cstart4 : 1] = id_p) {
                    c0 = c0 + 1;
                } else {

                }
                cstart7 = cstart7 + 2;
                i8 = i8 + 1;
            }
            cstart1 = cstart1 + 8;
            i2 = i2 + 1;
        }
        return c0;
    } |}]
