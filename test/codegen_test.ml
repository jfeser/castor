open Util
open Test_util

let run_test ?(params = []) ?(print_layout = false) ?(fork = false) ?irgen_debug
    layout_str =
  let layout_file = Filename.temp_file "layout" "txt" in
  let open Abslayout_load in
  let conn = Lazy.force test_db_conn in
  let (module I), (module C) = Setup.make_modules ?irgen_debug () in
  let run_compiler layout =
    let out_dir = Filename.temp_dir "bin" "" in
    let exe_fn, data_fn =
      let params = List.map ~f:Tuple.T2.get1 params in
      C.compile ~out_dir ~layout_log:layout_file ~gprof:false ~params conn
        layout
    in
    if print_layout then
      In_channel.input_all (In_channel.create layout_file) |> print_endline;
    let cmd =
      let params_str =
        List.map params ~f:(fun (_, v) -> Value.to_sql v)
        |> String.concat ~sep:" "
      in
      sprintf "%s -p %s %s" exe_fn data_fn params_str
    in
    let cmd_ch = Unix.open_process_in cmd in
    let cmd_output = In_channel.input_all cmd_ch in
    let ret = Unix.close_process_in cmd_ch in
    print_endline cmd_output;
    print_endline (Unix.Exit_or_signal.to_string_hum ret);
    Out_channel.flush stdout
  in
  let run () =
    let layout =
      let params =
        List.map params ~f:(fun (n, _) -> n) |> Set.of_list (module Name)
      in
      load_string conn ~params layout_str |> Type.annotate conn
    in

    if fork then run_in_fork (fun () -> run_compiler layout)
    else run_compiler layout
  in
  Exn.handle_uncaught ~exit:false run

let%expect_test "ordered-idx" =
  run_test
    "AOrderedIdx(OrderBy([f asc], Dedup(Select([f], r1))) as k, AList(Filter(f \
     = k.f, r1) as lk, ascalar(lk.g)), 1, 3)";
  [%expect {|
    1|2
    1|3
    2|1
    2|2

    exited normally |}]

let%expect_test "agg" =
  run_test
    "select([1.0 + 2.0, avg(a), count(), sum(a), min(a), max(a)], alist(r2 as \
     k, ascalar(k.a)))";
  [%expect
    {|
    3.000000|7.978000|5|39.890000|-0.420000|34.420000

    exited normally |}]

let%expect_test "hash-idx" =
  run_test
    "AHashIdx(Dedup(Select([f], r1)) as k, AList(Filter(f = k.f, r1) as lk, \
     ascalar(lk.g)), 2)";
  [%expect {|
    2|1
    2|2

    exited normally |}]

let%expect_test "strops" =
  run_test ~params:[]
    {|
select([strlen("test"), strpos("testing", "in")], ascalar(0))
|};
  [%expect
    {|
    [ERROR] Tried to get schema of unnamed predicate 0.
    [ERROR] Tried to get schema of unnamed predicate 0.
    4|5

    exited normally |}]

let%expect_test "example-1" =
  Demomatch.(run_test ~params:example_params (example1 "log"));
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-1-str" =
  Demomatch.(run_test ~params:example_str_params (example1 "log_str"));
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-2" =
  Demomatch.(run_test ~params:example_params (example2 "log"));
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-2-str" =
  Demomatch.(
    run_test ~print_layout:true ~params:example_str_params (example2 "log_str"));
  [%expect
    {|
    0:2 Table len (=142)
    2:2 Table hash len (=104)
    4:104 Table hash
    108:1 Table map len (=2)
    109:2 Table key map
    109:1 Map entry (0 => 14)
    110:1 Map entry (1 => 0)
    111:31 Table values
    111:1 Tuple len (=8)
    112:7 Tuple body
    112:3 Scalar (=(String foo))
    112:3 String body
    112:0 String length (=3)
    115:4 Scalar (=(String bar))
    115:1 String length (=3)
    116:3 String body
    119:1 List count (=2)
    120:1 List len (=6)
    121:4 List body
    121:2 Tuple body
    121:1 Scalar (=(Int 1))
    121:0 Tuple len (=2)
    122:1 Scalar (=(Int 3))
    123:2 Tuple body
    123:1 Scalar (=(Int 4))
    123:0 Tuple len (=2)
    124:1 Scalar (=(Int 5))
    125:1 Tuple len (=13)
    126:12 Tuple body
    126:3 Scalar (=(String foo))
    126:3 String body
    126:0 String length (=3)
    129:9 Scalar (=(String fizzbuzz))
    129:1 String length (=8)
    130:8 String body
    138:1 List count (=1)
    139:1 List len (=4)
    140:2 List body
    140:2 Tuple body
    140:1 Scalar (=(Int 1))
    140:0 Tuple len (=2)
    141:1 Scalar (=(Int 2))

    1|2

    exited normally |}]

let%expect_test "example-3" =
  Demomatch.(run_test ~params:example_params (example3 "log"));
  [%expect {|
    1|2

    exited normally |}]

let%expect_test "example-3-str" =
  Demomatch.(
    run_test ~print_layout:true ~params:example_str_params (example3 "log_str"));
  [%expect
    {|
    0:2 Tuple len (=198)
    2:196 Tuple body
    2:2 Table len (=145)
    4:2 Table hash len (=104)
    6:104 Table hash
    110:1 Table map len (=3)
    111:3 Table key map
    111:1 Map entry (0 => 23)
    112:1 Map entry (1 => 10)
    113:1 Map entry (2 => 0)
    114:33 Table values
    114:4 Scalar (=(String bar))
    114:1 String length (=3)
    115:3 String body
    118:1 List count (=2)
    119:1 List len (=6)
    120:4 List body
    120:2 Tuple body
    120:1 Scalar (=(Int 3))
    120:0 Tuple len (=2)
    121:1 Scalar (=(Int 4))
    122:2 Tuple body
    122:1 Scalar (=(Int 5))
    122:0 Tuple len (=2)
    123:1 Scalar (=(Int 6))
    124:9 Scalar (=(String fizzbuzz))
    124:1 String length (=8)
    125:8 String body
    133:1 List count (=1)
    134:1 List len (=4)
    135:2 List body
    135:2 Tuple body
    135:1 Scalar (=(Int 2))
    135:0 Tuple len (=2)
    136:1 Scalar (=(Int 3))
    137:4 Scalar (=(String foo))
    137:1 String length (=3)
    138:3 String body
    141:1 List count (=2)
    142:1 List len (=6)
    143:4 List body
    143:2 Tuple body
    143:1 Scalar (=(Int 1))
    143:0 Tuple len (=2)
    144:1 Scalar (=(Int 4))
    145:2 Tuple body
    145:1 Scalar (=(Int 4))
    145:0 Tuple len (=2)
    146:1 Scalar (=(Int 6))
    147:1 Ordered idx len (=51)
    148:10 Ordered idx map
    148:1 Ordered idx key
    148:1 Scalar (=(Int 1))
    148:0 Ordered idx index len (=10)
    149:1 Ordered idx ptr (=0)
    150:1 Ordered idx key
    150:1 Scalar (=(Int 2))
    151:1 Ordered idx ptr (=7)
    152:1 Ordered idx key
    152:1 Scalar (=(Int 3))
    153:1 Ordered idx ptr (=19)
    154:1 Ordered idx key
    154:1 Scalar (=(Int 4))
    155:1 Ordered idx ptr (=26)
    156:1 Ordered idx key
    156:1 Scalar (=(Int 5))
    157:1 Ordered idx ptr (=33)
    158:40 Ordered idx body
    158:1 List len (=7)
    158:0 List count (=1)
    159:6 List body
    159:1 Tuple len (=6)
    160:5 Tuple body
    160:4 Scalar (=(String foo))
    160:1 String length (=3)
    161:3 String body
    164:1 Scalar (=(Int 1))
    165:1 List len (=12)
    165:0 List count (=1)
    166:11 List body
    166:1 Tuple len (=11)
    167:10 Tuple body
    167:9 Scalar (=(String fizzbuzz))
    167:1 String length (=8)
    168:8 String body
    176:1 Scalar (=(Int 2))
    177:1 List len (=7)
    177:0 List count (=1)
    178:6 List body
    178:1 Tuple len (=6)
    179:5 Tuple body
    179:4 Scalar (=(String bar))
    179:1 String length (=3)
    180:3 String body
    183:1 Scalar (=(Int 3))
    184:1 List len (=7)
    184:0 List count (=1)
    185:6 List body
    185:1 Tuple len (=6)
    186:5 Tuple body
    186:4 Scalar (=(String foo))
    186:1 String length (=3)
    187:3 String body
    190:1 Scalar (=(Int 4))
    191:1 List len (=7)
    191:0 List count (=1)
    192:6 List body
    192:1 Tuple len (=6)
    193:5 Tuple body
    193:4 Scalar (=(String bar))
    193:1 String length (=3)
    194:3 String body
    197:1 Scalar (=(Int 5))

    1|2

    exited normally |}]

let%expect_test "output-test" =
  run_test
    {|select([1, 100, 1.0, 0.0001, 1.0001, "this is a test", "test with, commas", date("1994-01-04"), true, false], ascalar(0))|};
  [%expect
    {|
    [ERROR] Tried to get schema of unnamed predicate 0.
    [ERROR] Tried to get schema of unnamed predicate 0.
    1|100|1.000000|0.000100|1.000100|this is a test|test with, commas|1994-01-04|t|f

    exited normally |}]

let%expect_test "ordering" =
  run_test
    {|alist(orderby([f desc], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross))|};
  [%expect {|
    3|4
    2|1
    2|2
    1|2
    1|3

    exited normally |}];
  run_test
    {|alist(orderby([f], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross))|};
  [%expect {|
    1|2
    1|3
    2|1
    2|2
    3|4

    exited normally |}];
  run_test
    {|atuple([alist(orderby([f desc], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross)), atuple([ascalar(9), ascalar(9)], cross), alist(orderby([f], r1) as k1, atuple([ascalar(k1.f), ascalar(k1.g)], cross))], concat)|};
  [%expect
    {|
    [WARNING] Name does not appear in all concat fields: f
    [WARNING] Name does not appear in all concat fields: g
    [WARNING] Name does not appear in all concat fields: f
    [WARNING] Name does not appear in all concat fields: g
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

    exited normally |}];
  run_test
    {|atuple([alist(orderby([f], r1) as k, atuple([ascalar(k.f), ascalar(k.g)], cross)), atuple([ascalar(9), ascalar(9)], cross), alist(orderby([f desc], r1) as k1, atuple([ascalar(k1.f), ascalar(k1.g)], cross))], concat)|};
  [%expect
    {|
    [WARNING] Name does not appear in all concat fields: f
    [WARNING] Name does not appear in all concat fields: g
    [WARNING] Name does not appear in all concat fields: f
    [WARNING] Name does not appear in all concat fields: g
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
    {|AOrderedIdx(OrderBy([f asc], Dedup(Select([f], r_date))) as k, 
     AScalar(k.f as v), date("2017-10-05"), date("2018-09-01"))|};
  [%expect
    {|
    2017-10-05|2017-10-05
    2018-01-01|2018-01-01
    2018-01-23|2018-01-23

    exited normally |}]

let%expect_test "ordered-idx-dates" =
  run_test
    {|
AOrderedIdx(OrderBy([f asc], Dedup(Select([f], r_date))) as k,
     AScalar(k.f as v), >= date("2017-10-05"), < date("2018-09-01"))
|};
  [%expect
    {|
    2017-10-05|2017-10-05
    2018-01-01|2018-01-01
    2018-01-23|2018-01-23

    exited normally |}]

let%expect_test "ordered-idx-dates" =
  run_test
    {|
AOrderedIdx(OrderBy([f asc], Dedup(Select([f], r_date))) as k,
     AScalar(k.f as v), > date("2017-10-05"), <= date("2018-09-01"))
|};
  [%expect
    {|
    2018-01-01|2018-01-01
    2018-01-23|2018-01-23
    2018-09-01|2018-09-01

    exited normally |}]

let%expect_test "ordered-idx-dates" =
  run_test
    {|
AOrderedIdx(OrderBy([f asc], Dedup(Select([f], r_date))) as k,
     AScalar(k.f as v), > date("2017-10-05"), < date("2018-09-01"))
|};
  [%expect
    {|
    2018-01-01|2018-01-01
    2018-01-23|2018-01-23

    exited normally |}]

let%expect_test "ordered-idx-dates" =
  run_test
    {|
AOrderedIdx(OrderBy([f asc], Dedup(Select([f], r_date))) as k,
     AScalar(k.f as v), >= date("2017-10-05"), <= date("2018-09-01"))
|};
  [%expect
    {|
    2017-10-05|2017-10-05
    2018-01-01|2018-01-01
    2018-01-23|2018-01-23
    2018-09-01|2018-09-01

    exited normally |}]

let%expect_test "ordered-idx-dates" =
  run_test
    {|
AOrderedIdx(OrderBy([f asc], Dedup(Select([f], r_date))) as k,
     AScalar(k.f as v), >= date("0000-01-01"), <= date("9999-01-01"))
|};
  [%expect
    {|
    2016-12-01|2016-12-01
    2017-10-05|2017-10-05
    2018-01-01|2018-01-01
    2018-01-23|2018-01-23
    2018-09-01|2018-09-01

    exited normally |}]

let%expect_test "ordered-idx-dates" =
  run_test
    {|
AOrderedIdx(OrderBy([f1 asc, f2 asc], Dedup(Select([f as f1, f as f2], r_date))) as k,
     AScalar(k.f1 as v), >= date("0000-01-01"), <= date("9999-01-01"), >= date("0000-01-01"), < date("2018-09-01"))
|};
  [%expect
    {|
    2016-12-01|2016-12-01|2016-12-01
    2017-10-05|2017-10-05|2017-10-05
    2018-01-01|2018-01-01|2018-01-01
    2018-01-23|2018-01-23|2018-01-23

    exited normally |}]

let%expect_test "ordered-idx-dates" =
  run_test
    {|
AOrderedIdx(join(true, ints, select([x as y], ints)) as k,
     atuple([ascalar(k.x), ascalar(k.y)], cross), >= 3, <= 5, >= 4, <= 6)
|};
  [%expect
    {|
    3|4|3|4
    3|5|3|5
    3|6|3|6
    4|4|4|4
    4|5|4|5
    5|4|5|4
    5|5|5|5
    4|6|4|6
    5|6|5|6

    exited normally |}]

let%expect_test "date-arith" =
  run_test
    {|select([date("1997-07-01") + month(3), date("1997-07-01") + day(90)], ascalar(0))|};
  [%expect
    {|
    [ERROR] Tried to get schema of unnamed predicate 0.
    [ERROR] Tried to get schema of unnamed predicate 0.
    1997-10-01|1997-09-29

    exited normally |}]

let%expect_test "depjoin" =
  run_test
    "depjoin(alist(r1 as k1, ascalar(k1.f)) as k3, select([k3.f + g], \
     ascalar(5 as g)))";
  [%expect {|
    6
    6
    7
    7
    8

    exited normally |}]
