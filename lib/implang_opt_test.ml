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
    // cstart18 : Int[nonnull] (persists=true)
    // pcount16 : Int[nonnull] (persists=true)
    // cstart12 : Int[nonnull] (persists=true)
    // pcount11 : Int[nonnull] (persists=true)
    // cstart17 : Int[nonnull] (persists=true)
    // cstart15 : Int[nonnull] (persists=true)
    // cstart13 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // cstart14 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart10 = 1;
        pcount11 = 3;
        loop (0 < pcount11) {
            cstart12 = cstart10 + 1;
            cstart13 = cstart12 + 1;
            cstart14 = cstart13 + 1;
            cstart15 = cstart14 + 1 + 1;
            pcount16 = buf[cstart14 : 1];
            loop (0 < pcount16) {
                cstart17 = cstart15;
                cstart18 = cstart17 + 1;
                if (buf[cstart17 : 1] = id_c && buf[cstart12 : 1] = id_p) {
                    print(Tuple[Int[nonnull], Int[nonnull], Int[nonnull],
                    Int[nonnull]],
                    (buf[cstart12 : 1], buf[cstart13 : 1], buf[cstart17 : 1],
                     buf[cstart18 : 1]));
                } else {

                }
                cstart15 = cstart15 + 2;
                pcount16 = pcount16 - 1;
            }
            cstart10 = cstart10 + buf[cstart10 : 1];
            pcount11 = pcount11 - 1;
        }
    }
    // Locals:
    // cstart9 : Int[nonnull] (persists=true)
    // pcount7 : Int[nonnull] (persists=true)
    // pcount2 : Int[nonnull] (persists=true)
    // cstart8 : Int[nonnull] (persists=true)
    // cstart1 : Int[nonnull] (persists=true)
    // c0 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    fun counter () : Int[nonnull] {
        c0 = 0;
        cstart1 = 1;
        pcount2 = 3;
        loop (0 < pcount2) {
            cstart3 = cstart1 + 1;
            cstart4 = cstart3 + 1;
            cstart5 = cstart4 + 1;
            cstart6 = cstart5 + 1 + 1;
            pcount7 = buf[cstart5 : 1];
            loop (0 < pcount7) {
                cstart8 = cstart6;
                cstart9 = cstart8 + 1;
                if (buf[cstart8 : 1] = id_c && buf[cstart3 : 1] = id_p) {
                    c0 = c0 + 1;
                } else {

                }
                cstart6 = cstart6 + 2;
                pcount7 = pcount7 - 1;
            }
            cstart1 = cstart1 + buf[cstart1 : 1];
            pcount2 = pcount2 - 1;
        }
        return c0;
    } |}]
