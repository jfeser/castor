open! Core
open Test_util

let run_test ?(params = []) ?(print_layout = false) ?(fork = false) ?irgen_debug
    layout_str =
  let layout_file = Filename.temp_file "layout" "txt" in
  let (module M), (module S), (module I), (module C) =
    Setup.make_modules ~layout_file ?irgen_debug ()
  in
  let run_compiler layout =
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
    M.annotate_type layout ;
    if fork then run_in_fork (fun () -> run_compiler layout)
    else run_compiler layout
  in
  Exn.handle_uncaught ~exit:false run

let%expect_test "ordered-idx" =
  run_test
    "AOrderedIdx(OrderBy([f asc], Dedup(Select([f], r1))) as k, AList(Filter(f = \
     k.f, r1) as lk, ascalar(lk.g)), 1, 3)" ;
  [%expect {|
    1|2
    1|3
    2|1
    2|2

    exited normally |}]

let%expect_test "agg" =
  run_test
    "select([1.0 + 2.0, avg(a), count(), sum(a), min(a), max(a)], alist(r2 as k, \
     ascalar(k.a)))" ;
  [%expect
    {|
    3.000000|7.978000|5|39.890000|-0.420000|34.420000

    exited normally |}]

let%expect_test "hash-idx" =
  run_test
    "AHashIdx(Dedup(Select([f], r1)) as k, AList(Filter(f = k.f, r1) as lk, \
     ascalar(lk.g)), 2)" ;
  [%expect {|
    2|1
    2|2

    exited normally |}]

let%expect_test "strops" =
  run_test ~params:[]
    {|
select([strlen("test"), strpos("testing", "in")], ascalar(0))
|} ;
  [%expect
    {|
    [ERROR] Tried to get schema of unnamed predicate 0.
    [ERROR] Tried to get schema of unnamed predicate 0.
    4|5

    exited normally |}]

let%expect_test "example-1" =
  Demomatch.(run_test ~params:example_params (example1 "log")) ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-1-str" =
  Demomatch.(run_test ~params:example_str_params (example1 "log_str")) ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-2" =
  Demomatch.(run_test ~params:example_params (example2 "log")) ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-2-str" =
  Demomatch.(
    run_test ~print_layout:true ~params:example_str_params (example2 "log_str")) ;
  [%expect
    {|
    0:4 Table len
    4:8 Table hash len
    12:104 Table hash
    116:8 Table map len
    124:16 Table key map
    124:8 Map entry (0 => 14)
    132:8 Map entry (1 => 0)
    140:31 Table values
    140:1 Tuple len (=8)
    141:7 Tuple body
    141:3 Scalar (=(String foo))
    141:3 String body
    141:0 String length (=3)
    144:4 Scalar (=(String bar))
    144:1 String length (=3)
    145:3 String body
    148:1 List count (=2)
    149:1 List len (=6)
    150:4 List body
    150:2 Tuple body
    150:1 Scalar (=(Int 1))
    150:0 Tuple len (=2)
    151:1 Scalar (=(Int 3))
    152:2 Tuple body
    152:1 Scalar (=(Int 4))
    152:0 Tuple len (=2)
    153:1 Scalar (=(Int 5))
    154:1 Tuple len (=13)
    155:12 Tuple body
    155:3 Scalar (=(String foo))
    155:3 String body
    155:0 String length (=3)
    158:9 Scalar (=(String fizzbuzz))
    158:1 String length (=8)
    159:8 String body
    167:1 List count (=1)
    168:1 List len (=4)
    169:2 List body
    169:2 Tuple body
    169:1 Scalar (=(Int 1))
    169:0 Tuple len (=2)
    170:1 Scalar (=(Int 2))

    1|2

    exited normally |}]

let%expect_test "example-3" =
  Demomatch.(run_test ~params:example_params (example3 "log")) ;
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-3-str" =
  Demomatch.(
    run_test ~print_layout:true ~params:example_str_params (example3 "log_str")) ;
  [%expect {|
    0:4 Tuple len (=236)
    4:232 Tuple body
    4:4 Table len
    8:8 Table hash len
    16:104 Table hash
    120:8 Table map len
    128:24 Table key map
    128:8 Map entry (0 => 23)
    136:8 Map entry (1 => 10)
    144:8 Map entry (2 => 0)
    152:33 Table values
    152:4 Scalar (=(String bar))
    152:1 String length (=3)
    153:3 String body
    156:1 List count (=2)
    157:1 List len (=6)
    158:4 List body
    158:2 Tuple body
    158:1 Scalar (=(Int 3))
    158:0 Tuple len (=2)
    159:1 Scalar (=(Int 4))
    160:2 Tuple body
    160:1 Scalar (=(Int 5))
    160:0 Tuple len (=2)
    161:1 Scalar (=(Int 6))
    162:9 Scalar (=(String fizzbuzz))
    162:1 String length (=8)
    163:8 String body
    171:1 List count (=1)
    172:1 List len (=4)
    173:2 Tuple body
    173:2 List body
    173:1 Scalar (=(Int 2))
    173:0 Tuple len (=2)
    174:1 Scalar (=(Int 3))
    175:4 Scalar (=(String foo))
    175:1 String length (=3)
    176:3 String body
    179:1 List count (=2)
    180:1 List len (=6)
    181:4 List body
    181:2 Tuple body
    181:1 Scalar (=(Int 1))
    181:0 Tuple len (=2)
    182:1 Scalar (=(Int 4))
    183:2 Tuple body
    183:1 Scalar (=(Int 4))
    183:0 Tuple len (=2)
    184:1 Scalar (=(Int 6))
    185:1 Ordered idx len (=51)
    186:10 Ordered idx map
    186:1 Ordered idx key
    186:1 Scalar (=(Int 1))
    186:1 Ordered idx key
    186:0 Ordered idx index len (=10)
    187:1 Ordered idx ptr (=0)
    188:1 Ordered idx key
    188:1 Scalar (=(Int 2))
    188:1 Ordered idx key
    189:1 Ordered idx ptr (=7)
    190:1 Ordered idx key
    190:1 Scalar (=(Int 3))
    190:1 Ordered idx key
    191:1 Ordered idx ptr (=19)
    192:1 Ordered idx key
    192:1 Scalar (=(Int 4))
    192:1 Ordered idx key
    193:1 Ordered idx ptr (=26)
    194:1 Ordered idx key
    194:1 Scalar (=(Int 5))
    194:1 Ordered idx key
    195:1 Ordered idx ptr (=33)
    196:40 Ordered idx body
    196:1 List len (=7)
    196:0 List count (=1)
    197:6 List body
    197:1 Tuple len (=6)
    198:5 Tuple body
    198:4 Scalar (=(String foo))
    198:1 String length (=3)
    199:3 String body
    202:1 Scalar (=(Int 1))
    203:1 List len (=12)
    203:0 List count (=1)
    204:11 List body
    204:1 Tuple len (=11)
    205:10 Tuple body
    205:9 Scalar (=(String fizzbuzz))
    205:1 String length (=8)
    206:8 String body
    214:1 Scalar (=(Int 2))
    215:1 List len (=7)
    215:0 List count (=1)
    216:6 List body
    216:1 Tuple len (=6)
    217:5 Tuple body
    217:4 Scalar (=(String bar))
    217:1 String length (=3)
    218:3 String body
    221:1 Scalar (=(Int 3))
    222:1 List len (=7)
    222:0 List count (=1)
    223:6 List body
    223:1 Tuple len (=6)
    224:5 Tuple body
    224:4 Scalar (=(String foo))
    224:1 String length (=3)
    225:3 String body
    228:1 Scalar (=(Int 4))
    229:1 List len (=7)
    229:0 List count (=1)
    230:6 List body
    230:1 Tuple len (=6)
    231:5 Tuple body
    231:4 Scalar (=(String bar))
    231:1 String length (=3)
    232:3 String body
    235:1 Scalar (=(Int 5))

    1|2

    exited normally |}]

let%expect_test "output-test" =
  run_test
    {|select([1, 100, 1.0, 0.0001, 1.0001, "this is a test", "test with, commas", date("1994-01-04"), true, false], ascalar(0))|} ;
  [%expect
    {|
    [ERROR] Tried to get schema of unnamed predicate 0.
    [ERROR] Tried to get schema of unnamed predicate 0.
    1|100|1.000000|0.000100|1.000100|this is a test|test with, commas|1994-01-04|t|f

    exited normally |}]

let%expect_test "ordering" =
  run_test
    {|alist(orderby([f desc], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross))|} ;
  [%expect {|
    3|4
    2|1
    2|2
    1|2
    1|3

    exited normally |}] ;
  run_test
    {|alist(orderby([f], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross))|} ;
  [%expect {|
    1|2
    1|3
    2|1
    2|2
    3|4

    exited normally |}] ;
  run_test
    {|atuple([alist(orderby([f desc], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross)), atuple([ascalar(9), ascalar(9)], cross), alist(orderby([f], r1) as k1, atuple([ascalar(k1.f), ascalar(k1.g)], cross))], concat)|} ;
  [%expect
    {|
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

    exited normally |}] ;
  run_test
    {|atuple([alist(orderby([f], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross)), atuple([ascalar(9), ascalar(9)], cross), alist(orderby([f desc], r1) as k1, atuple([ascalar(k1.f), ascalar(k1.g)], cross))], concat)|} ;
  [%expect
    {|
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
     AScalar(k.f as v), date("2017-10-04"), date("2018-10-04"))|} ;
  [%expect
    {|
    2018-09-01|2018-09-01
    2018-01-23|2018-01-23
    2018-01-01|2018-01-01
    2017-10-05|2017-10-05
    2016-12-01|2016-12-01

    exited normally |}]

let%expect_test "date-arith" =
  run_test
    {|select([date("1997-07-01") + month(3), date("1997-07-01") + day(90)], ascalar(0))|} ;
  [%expect
    {|
    [ERROR] Tried to get schema of unnamed predicate 0.
    [ERROR] Tried to get schema of unnamed predicate 0.
    1997-10-01|1997-09-29

    exited normally |}]
