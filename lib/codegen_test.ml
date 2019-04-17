open Core
open Abslayout
open Test_util

let run_test ?(params = []) ?(print_layout = true) ?(fork = false) ?irgen_debug
    layout_str =
  let layout_file = Filename.temp_file "layout" "txt" in
  let (module M), (module S), (module I), (module C) =
    make_modules ~layout_file ?irgen_debug ()
  in
  let run_compiler out_dir layout =
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
  try
    let layout =
      let params =
        List.map params ~f:(fun (n, _) -> n) |> Set.of_list (module Name)
      in
      of_string_exn layout_str |> M.resolve ~params
    in
    let layout = M.annotate_key_layouts layout in
    let out_dir = Filename.temp_dir "bin" "" in
    if fork then run_in_fork (fun () -> run_compiler out_dir layout)
    else run_compiler out_dir layout
  with exn -> printf "Error: %s\n" (Exn.to_string exn)

let%expect_test "ordered-idx" =
  run_test ~print_layout:false
    "AOrderedIdx(OrderBy([r1.f asc], Dedup(Select([r1.f], r1))) as k, \
     AList(Filter(r1.f = k.f, r1), ascalar(r1.g)), 1, 3)" ;
  [%expect {|
    1|2
    1|3
    2|1
    2|2

    exited normally |}]

let%expect_test "agg" =
  [%expect {||}] ;
  run_test ~print_layout:false ~fork:false
    "select([1.0 + 2.0, avg(r2.a), count(), sum(r2.a), min(r2.a), max(r2.a)], \
     alist(r2, ascalar(r2.a)))" ;
  [%expect
    {|
    3.000000|7.978000|5|39.890000|-0.420000|34.420000

    exited normally |}]

let%expect_test "hash-idx" =
  run_test ~print_layout:false
    "AHashIdx(Dedup(Select([r1.f], r1)) as k, AList(Filter(r1.f = k.f, r1), \
     ascalar(r1.g)), 2)" ;
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
  run_test ~params:Demomatch.example_params ~print_layout:false
    {|
select([lp.counter, lc.counter], filter(lc.id = id_c && lp.id = id_p,
alist(filter(succ > counter + 1, log) as lp,
atuple([ascalar(lp.id), ascalar(lp.counter),
alist(filter(lp.counter < log.counter &&
log.counter < lp.succ, log) as lc,
atuple([ascalar(lc.id), ascalar(lc.counter)], cross))], cross))))
|} ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-1-str" =
  run_test ~params:Demomatch.example_str_params ~print_layout:false
    {|
select([lp.counter, lc.counter], filter(lc.id = id_c && lp.id = id_p,
alist(filter(succ > counter + 1, log_str) as lp,
atuple([ascalar(lp.id), ascalar(lp.counter),
alist(filter(lp.counter < log_str.counter &&
log_str.counter < lp.succ, log_str) as lc,
atuple([ascalar(lc.id), ascalar(lc.counter)], cross))], cross))))
|} ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-2" =
  run_test ~params:Demomatch.example_params ~print_layout:false
    {|
select([lp.counter, lc.counter], ahashidx(dedup(select([lp.id as lp_k, lc.id as lc_k], 
      join(true, log as lp, log as lc))),
  alist(select([lp.counter, lc.counter], 
    join(lp.counter < lc.counter && 
         lc.counter < lp.succ, 
      filter(log.id = lp_k, log) as lp, 
      filter(log.id = lc_k, log) as lc)),
    atuple([ascalar(lp.counter), ascalar(lc.counter)], cross)),
  (id_p, id_c)))
|} ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-2-str" =
  run_test ~params:Demomatch.example_str_params ~print_layout:false
    {|
select([lp.counter, lc.counter], ahashidx(dedup(select([lp.id as lp_k, lc.id as lc_k], 
      join(true, log_str as lp, log_str as lc))),
  alist(select([lp.counter, lc.counter], 
    join(lp.counter < lc.counter && 
         lc.counter < lp.succ, 
      filter(log_str.id = lp_k, log_str) as lp, 
      filter(log_str.id = lc_k, log_str) as lc)),
    atuple([ascalar(lp.counter), ascalar(lc.counter)], cross)),
  (id_p, id_c)))
|} ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-3" =
  run_test ~params:Demomatch.example_params ~print_layout:false
    {|
select([lp.counter, lc.counter],
  atuple([ahashidx(dedup(select([id as k1], log)), 
    alist(select([counter, succ], 
        filter(k1 = id && counter < succ, log)), 
      atuple([ascalar(counter), ascalar(succ)], cross)), 
    id_p) as lp,
  filter(lc.id = id_c,
    aorderedidx(select([log.counter as k2], log), 
      alist(filter(log.counter = k2, log),
        atuple([ascalar(log.id), ascalar(log.counter)], cross)), 
      lp.counter, lp.succ) as lc)], cross))
|} ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-3-str" =
  run_test ~params:Demomatch.example_str_params ~print_layout:false
    {|
select([lp.counter, lc.counter],
  atuple([ahashidx(dedup(select([id as k1], log_str)), 
    alist(select([counter, succ], 
        filter(k1 = id && counter < succ, log_str)), 
      atuple([ascalar(counter), ascalar(succ)], cross)), 
    id_p) as lp,
  filter(lc.id = id_c,
    aorderedidx(select([log_str.counter as k2], log_str), 
      alist(filter(log_str.counter = k2, log_str),
        atuple([ascalar(log_str.id), ascalar(log_str.counter)], cross)), 
      lp.counter, lp.succ) as lc)], cross))
|} ;
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
    {|alist(orderby([r1.f desc], r1), atuple([ascalar(r1.f), ascalar(r1.g)], cross))|} ;
  run_test ~print_layout:false
    {|alist(orderby([r1.f], r1), atuple([ascalar(r1.f), ascalar(r1.g)], cross))|} ;
  run_test ~print_layout:false
    {|atuple([alist(orderby([r1.f desc], r1), atuple([ascalar(r1.f), ascalar(r1.g)], cross)), atuple([ascalar(9), ascalar(9)], cross), alist(orderby([r1.f], r1), atuple([ascalar(r1.f), ascalar(r1.g)], cross))], concat)|} ;
  run_test ~print_layout:false
    {|atuple([alist(orderby([r1.f], r1), atuple([ascalar(r1.f), ascalar(r1.g)], cross)), atuple([ascalar(9), ascalar(9)], cross), alist(orderby([r1.f desc], r1), atuple([ascalar(r1.f), ascalar(r1.g)], cross))], concat)|} ;
  [%expect
    {|
    3|4
    2|1
    2|2
    1|2
    1|3

    exited normally
    1|2
    1|3
    2|1
    2|2
    3|4

    exited normally
    3|4
    2|1
    2|2
    1|2
    1|3
    9|9
    1|2
    1|3
    2|1
    2|2
    3|4

    exited normally
    1|2
    1|3
    2|1
    2|2
    3|4
    9|9
    3|4
    2|1
    2|2
    1|2
    1|3

    exited normally |}]

let%expect_test "ordered-idx-dates" =
  run_test
    {|AOrderedIdx(OrderBy([f desc], Dedup(Select([f], r_date))) as k, 
     AScalar(k.f), date("2017-10-04"), date("2018-10-04"))|} ;
  [%expect
    {|
    [WARNING] Output shadowing of k.f.
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
