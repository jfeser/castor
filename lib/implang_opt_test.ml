open! Core
open Test_util
open Implang_opt

let run_test ?(params = []) layout_str opt_func =
  let (module M), (module S), (module I), _ =
    Setup.make_modules ~code_only:true ()
  in
  let param_names = List.map params ~f:(fun (n, _) -> n) in
  let layout =
    M.load_string ~params:(Set.of_list (module Name) param_names) layout_str
  in
  M.annotate_type layout;
  I.irgen ~params:param_names ~data_fn:"/tmp/buf" layout
  |> opt_func
  |> I.pp Caml.Format.std_formatter

let%expect_test "example-1" =
  Demomatch.(run_test ~params:Demomatch.example_params (example1 "log") opt);
  [%expect
    {|
    // Locals:
    // i18 : Int[nonnull] (persists=true)
    // cstart21 : Int[nonnull] (persists=true)
    // cstart16 : Int[nonnull] (persists=true)
    // count13 : Int[nonnull] (persists=true)
    // cstart17 : Int[nonnull] (persists=true)
    // cstart15 : Int[nonnull] (persists=true)
    // cstart11 : Int[nonnull] (persists=true)
    // i12 : Int[nonnull] (persists=true)
    // cstart14 : Int[nonnull] (persists=true)
    // count19 : Int[nonnull] (persists=true)
    // cstart20 : Int[nonnull] (persists=true)
    fun printer () : Void {
        cstart11 = 1;
        i12 = 0;
        count13 = 2;
        loop (i12 < count13) {
            cstart14 = cstart11 + 1;
            cstart15 = cstart14 + 1;
            cstart16 = cstart15 + 1;
            cstart17 = cstart16 + 1 + 1;
            i18 = 0;
            count19 = buf[cstart16 : 1];
            loop (i18 < count19) {
                cstart20 = cstart17;
                cstart21 = cstart20 + 1;
                if (buf[cstart20 : 1] == id_c && buf[cstart14 : 1] == id_p) {
                    print(Tuple[Int[nonnull], Int[nonnull]],
                    (buf[cstart15 : 1], buf[cstart21 : 1]));
                } else {

                }
                cstart17 = cstart17 + 2;
                i18 = i18 + 1;
            }
            cstart11 = cstart11 + buf[cstart11 : 1];
            i12 = i12 + 1;
        }
    }
    // Locals:
    // i1 : Int[nonnull] (persists=true)
    // count8 : Int[nonnull] (persists=true)
    // i7 : Int[nonnull] (persists=true)
    // cstart9 : Int[nonnull] (persists=true)
    // count2 : Int[nonnull] (persists=true)
    // cstart4 : Int[nonnull] (persists=true)
    // cstart3 : Int[nonnull] (persists=true)
    // cstart10 : Int[nonnull] (persists=true)
    // cstart0 : Int[nonnull] (persists=true)
    // cstart6 : Int[nonnull] (persists=true)
    // cstart5 : Int[nonnull] (persists=true)
    fun consumer () : Void {
        cstart0 = 1;
        i1 = 0;
        count2 = 2;
        loop (i1 < count2) {
            cstart3 = cstart0 + 1;
            cstart4 = cstart3 + 1;
            cstart5 = cstart4 + 1;
            cstart6 = cstart5 + 1 + 1;
            i7 = 0;
            count8 = buf[cstart5 : 1];
            loop (i7 < count8) {
                cstart9 = cstart6;
                cstart10 = cstart9 + 1;
                if (buf[cstart9 : 1] == id_c && buf[cstart3 : 1] == id_p) {
                    consume(Tuple[Int[nonnull], Int[nonnull]],
                    (buf[cstart4 : 1], buf[cstart10 : 1]));
                } else {

                }
                cstart6 = cstart6 + 2;
                i7 = i7 + 1;
            }
            cstart0 = cstart0 + buf[cstart0 : 1];
            i1 = i1 + 1;
        }
    } |}]
