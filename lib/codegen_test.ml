open Core
open Test_util

let run_test ?(params = []) ?print_layout ?(fork = false) ?irgen_debug layout_str =
  let layout_file = Filename.temp_file "layout" "txt" in
  let (module M), (module S), (module I), (module C) =
    make_modules ~layout_file ?irgen_debug ()
  in
  let run_compiler ?(print_layout = true) layout =
    let out_dir = Filename.temp_dir "bin" "" in
    let exe_fn, data_fn =
      let params = List.map ~f:Tuple.T2.get1 params in
      C.compile ~out_dir ~gprof:false ~params layout
    in
    if print_layout then
      In_channel.input_all (In_channel.create layout_file) |> print_endline ;
    let cmd =
      let params_str =
        List.map params ~f:(fun (_, v) -> Value.to_sql v) |> String.concat ~sep:" "
      in
      sprintf "%s -p %s %s" exe_fn data_fn params_str
    in
    let cmd_ch = Unix.open_process_in cmd in
    let cmd_output = In_channel.input_all cmd_ch in
    let ret = Unix.close_process_in cmd_ch in
    print_endline cmd_output ;
    print_endline (Unix.Exit_or_signal.to_string_hum ret) ;
    Out_channel.flush stdout
  in
  let run () =
    let layout =
      let params =
        List.map params ~f:(fun (n, _) -> n) |> Set.of_list (module Name)
      in
      M.load_string ~params layout_str
    in
    if fork then run_in_fork (fun () -> run_compiler ?print_layout layout)
    else run_compiler ?print_layout layout
  in
  Exn.handle_uncaught ~exit:false run

let%expect_test "ordered-idx" =
  run_test ~print_layout:false
    "AOrderedIdx(OrderBy([f asc], Dedup(Select([f], r1))) as k, AList(Filter(f = \
     k.f, r1) as lk, ascalar(lk.g)), 1, 3)" ;
  [%expect {|
    1|2
    1|3
    2|1
    2|2

    exited normally |}]

let%expect_test "agg" =
  [%expect {||}] ;
  run_test ~print_layout:false ~fork:false
    "select([1.0 + 2.0, avg(a), count(), sum(a), min(a), max(a)], alist(r2 as k, \
     ascalar(k.a)))" ;
  [%expect
    {|
    3.000000|7.978000|5|39.890000|-0.420000|34.420000

    exited normally |}]

let%expect_test "hash-idx" =
  run_test ~print_layout:false
    "AHashIdx(Dedup(Select([f], r1)) as k, AList(Filter(f = k.f, r1) as lk, \
     ascalar(lk.g)), 2)" ;
  [%expect {|
    2|1
    2|2

    exited normally |}]

let%expect_test "strops" =
  run_test ~params:[] ~print_layout:false
    {|
select([strlen("test"), strpos("testing", "in")], ascalar(0))
|} ;
  [%expect {|
    4|5

    exited normally |}]

let%expect_test "example-1" =
  Demomatch.(run_test ~params:example_params ~print_layout:false (example1 "log")) ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-1-str" =
  Demomatch.(
    run_test ~params:example_str_params ~print_layout:false (example1 "log_str")) ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-2" =
  Demomatch.(run_test ~params:example_params ~print_layout:false (example2 "log")) ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-2-str" =
  Demomatch.(
    run_test ~params:example_str_params ~print_layout:false (example2 "log_str")) ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-3" =
  Demomatch.(run_test ~params:example_params ~print_layout:false (example3 "log")) ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-3-str" =
  Demomatch.(
    run_test ~params:example_str_params ~print_layout:false (example3 "log_str")) ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "output-test" =
  run_test ~print_layout:false
    {|select([1, 100, 1.0, 0.0001, 1.0001, "this is a test", "test with, commas", date("1994-01-04"), true, false], ascalar(0))|} ;
  [%expect
    {|
    1|100|1.000000|0.000100|1.000100|this is a test|test with, commas|1994-01-04|t|f

    exited normally |}]

let%expect_test "ordering" =
  run_test ~print_layout:false
    {|alist(orderby([f desc], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross))|} ;
  [%expect {|
    1|2
    1|3
    2|1
    2|2
    3|4

    exited normally |}] ;
  run_test ~print_layout:false
    {|alist(orderby([f], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross))|} ;
  [%expect {|
    1|2
    1|3
    2|1
    2|2
    3|4

    exited normally |}] ;
  run_test ~print_layout:false
    {|atuple([alist(orderby([f desc], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross)), atuple([ascalar(9), ascalar(9)], cross), alist(orderby([f], r1) as k1, atuple([ascalar(k1.f), ascalar(k1.g)], cross))], concat)|} ;
  [%expect
    {|
    1|2
    1|3
    2|1
    2|2
    3|4
    9|9
    1|2
    1|3
    2|1
    2|2
    3|4

    exited normally |}] ;
  run_test ~print_layout:false
    {|atuple([alist(orderby([f], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross)), atuple([ascalar(9), ascalar(9)], cross), alist(orderby([f desc], r1) as k1, atuple([ascalar(k1.f), ascalar(k1.g)], cross))], concat)|} ;
  [%expect
    {|
    1|2
    1|3
    2|1
    2|2
    3|4
    9|9
    1|2
    1|3
    2|1
    2|2
    3|4

    exited normally |}]

let%expect_test "ordered-idx-dates" =
  run_test
    {|AOrderedIdx(OrderBy([f desc], Dedup(Select([f], r_date))) as k, 
     AScalar(k.f as v), date("2017-10-04"), date("2018-10-04"))|} ;
  [%expect
    {|
    0:4 Ordered idx len (=72)
    4:8 Ordered idx index len (=50)
    12:2 Scalar (=(Date 2016-12-01))
    12:2 Scalar (=(Date 2016-12-01))
    12:2 Ordered idx key
    14:8 Ordered idx value ptr (=62)
    22:2 Scalar (=(Date 2017-10-05))
    22:2 Scalar (=(Date 2017-10-05))
    22:2 Ordered idx key
    24:8 Ordered idx value ptr (=64)
    32:2 Scalar (=(Date 2018-01-01))
    32:2 Scalar (=(Date 2018-01-01))
    32:2 Ordered idx key
    34:8 Ordered idx value ptr (=66)
    42:2 Scalar (=(Date 2018-01-23))
    42:2 Scalar (=(Date 2018-01-23))
    42:2 Ordered idx key
    44:8 Ordered idx value ptr (=68)
    52:2 Scalar (=(Date 2018-09-01))
    52:2 Scalar (=(Date 2018-09-01))
    52:2 Ordered idx key
    54:8 Ordered idx value ptr (=70)
    62:2 Scalar (=(Date 2016-12-01))
    64:2 Scalar (=(Date 2017-10-05))
    66:2 Scalar (=(Date 2018-01-01))
    68:2 Scalar (=(Date 2018-01-23))
    70:2 Scalar (=(Date 2018-09-01))

    2017-10-05|2017-10-05
    2018-01-01|2018-01-01
    2018-01-23|2018-01-23
    2018-09-01|2018-09-01

    exited normally |}]

let%expect_test "date-arith" =
  run_test ~print_layout:false
    {|select([date("1997-07-01") + month(3), date("1997-07-01") + day(90)], ascalar(0))|} ;
  [%expect {|
    1997-10-01|1997-09-29

    exited normally |}]
