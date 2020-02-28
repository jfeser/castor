open Test_util
open Abslayout_visitors
open Implang_opt

let run_test ?(params = []) layout_str opt_func =
  let open Abslayout_load in
  let conn = Lazy.force test_db_conn in
  let (module I), _ = Setup.make_modules ~code_only:true () in
  let layout =
    let params =
      List.map params ~f:(fun (n, t, _) -> Name.copy ~type_:(Some t) n)
      |> Set.of_list (module Name)
    in
    load_string conn ~params layout_str |> Type.annotate conn
  in
  let layout, len = Serialize.serialize conn "/tmp/buf" layout in
  let params = List.map params ~f:(fun (n, t, _) -> (n, t)) in
  I.irgen ~params ~len layout |> opt_func |> I.pp Caml.Format.std_formatter

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

let%test_module _ =
  ( module struct
    open Abslayout_load

    let conn = Lazy.force Test_util.test_db_conn

    module I =
      Irgen.Make
        (struct
          let debug = false

          let code_only = true
        end)
        ()

    let%expect_test "" =
      let r =
        load_string conn "filter(c > 0, select([count() as c], ascalar(0)))"
        |> Type.annotate conn
        |> map_meta (fun m ->
               object
                 method type_ = m#type_

                 method pos = None
               end)
      in
      let ir = I.irgen ~params:[] ~len:0 r in
      Format.printf "%a" I.pp ir;
      [%expect
        {|
        [ERROR] Tried to get schema of unnamed predicate 0.
        [ERROR] Tried to get schema of unnamed predicate 0.
        [ERROR] Tried to get schema of unnamed predicate 0.
        [ERROR] Tried to get schema of unnamed predicate 0.
        // Locals:
        // count7 : Int[nonnull] (persists=false)
        // found_tup6 : Bool[nonnull] (persists=false)
        // tup5 : Tuple[Int[nonnull]] (persists=false)
        fun printer () : Void {
            found_tup6 = false;
            count7 = 0;
            tup5 = (buf[0 : 1]);
            count7 = count7 + 1;
            found_tup6 = true;
            if (found_tup6) {
                if (not(count7 < 0 || count7 == 0)) {
                    print(Tuple[Int[nonnull]], (count7));
                } else {

                }
            } else {

            }
        }
        // Locals:
        // found_tup2 : Bool[nonnull] (persists=false)
        // tup1 : Tuple[Int[nonnull]] (persists=false)
        // count3 : Int[nonnull] (persists=false)
        fun consumer () : Void {
            found_tup2 = false;
            count3 = 0;
            tup1 = (buf[0 : 1]);
            count3 = count3 + 1;
            found_tup2 = true;
            if (found_tup2) {
                if (not(count3 < 0 || count3 == 0)) {
                    consume(Tuple[Int[nonnull]], (count3));
                } else {

                }
            } else {

            }
        } |}];
      let ir' = opt ir in
      Format.printf "%a" I.pp ir';
      [%expect
        {|
        // Locals:
        // hoisted0 : Int[nonnull] (persists=false)
        // hoisted1 : Tuple[Int[nonnull]] (persists=false)
        // count7 : Int[nonnull] (persists=false)
        // found_tup6 : Bool[nonnull] (persists=false)
        // tup5 : Tuple[Int[nonnull]] (persists=false)
        fun printer () : Void {
            hoisted0 = buf[0 : 1];
            hoisted1 = (hoisted0);
            found_tup6 = false;
            count7 = 0;
            tup5 = hoisted1;
            count7 = count7 + 1;
            found_tup6 = true;
            if (found_tup6) {
                if (not(count7 < 0 || count7 == 0)) {
                    print(Tuple[Int[nonnull]], (count7));
                } else {

                }
            } else {

            }
        }
        // Locals:
        // hoisted2 : Int[nonnull] (persists=false)
        // hoisted3 : Tuple[Int[nonnull]] (persists=false)
        // found_tup2 : Bool[nonnull] (persists=false)
        // tup1 : Tuple[Int[nonnull]] (persists=false)
        // count3 : Int[nonnull] (persists=false)
        fun consumer () : Void {
            hoisted2 = buf[0 : 1];
            hoisted3 = (hoisted2);
            found_tup2 = false;
            count3 = 0;
            tup1 = hoisted3;
            count3 = count3 + 1;
            found_tup2 = true;
            if (found_tup2) {
                if (not(count3 < 0 || count3 == 0)) {
                    consume(Tuple[Int[nonnull]], (count3));
                } else {

                }
            } else {

            }
        } |}]
  end )
