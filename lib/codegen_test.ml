open Core
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

[@@@warning "-8"]

let _, [f; _] =
  Test_util.create rels "r1" ["f"; "g"]
    [[0; 5]; [1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]; [4; 6]]

let _ =
  Test_util.create rels "log" ["counter"; "succ"; "id"]
    [[1; 4; 1]; [2; 3; 2]; [3; 4; 3]; [4; 6; 1]; [5; 6; 3]]

[@@@warning "+8"]

module Eval = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (Eval)

let make_modules layout_file =
  let module S =
    Serialize.Make (struct
        let layout_map_channel = Some (Out_channel.create layout_file)
      end)
      (Eval)
  in
  let module I =
    Implang.IRGen.Make (struct
        let code_only = false
      end)
      (Eval)
      (S)
      ()
  in
  let module C =
    Codegen.Make (struct
        let debug = false
      end)
      (I)
      ()
  in
  ((module S : Serialize.S), (module I : Implang.IRGen.S), (module C : Codegen.S))

let run_in_fork thunk =
  match Unix.fork () with
  | `In_the_child ->
      Backtrace.elide := false ;
      Logs.set_reporter (Logs.format_reporter ()) ;
      Logs.set_level (Some Logs.Debug) ;
      thunk () ;
      exit 0
  | `In_the_parent pid ->
      let _, err = Unix.wait (`Pid pid) in
      Unix.Exit_or_signal.to_string_hum err |> print_endline

let run_test ?(params = []) layout_str =
  let layout_file = Filename.temp_file "layout" "txt" in
  let (module S), (module I), (module C) = make_modules layout_file in
  let layout =
    let params =
      List.map params ~f:(fun (n, _) -> n)
      |> Set.of_list (module Name.Compare_no_type)
    in
    of_string_exn layout_str |> M.resolve ~params |> M.annotate_schema
    |> M.annotate_key_layouts
  in
  let out_dir = Filename.temp_dir "bin" "" in
  let exe_fn, data_fn =
    let params = List.map ~f:Tuple.T2.get1 params in
    C.compile ~out_dir ~gprof:false ~params layout
  in
  In_channel.input_all (In_channel.create layout_file) |> print_endline ;
  let cmd =
    let params_str =
      List.map params ~f:(fun (_, v) -> Db.primvalue_to_sql v)
      |> String.concat ~sep:" "
    in
    sprintf "%s -p %s %s" exe_fn data_fn params_str
  in
  Unix.system cmd |> ignore

let%expect_test "ordered-idx" =
  run_test
    "AOrderedIdx(OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc) as k, \
     AList(Filter(r1.f = k.f, r1), ascalar(r1.g)), 1, 3)" ;
  [%expect
    {|
    0:8 Ordered idx len (=148)
    8:8 Ordered idx index len (=45)
    16:1 Scalar (=(Int 0))
    16:1 Ordered idx key
    17:8 Ordered idx value ptr (=0)
    25:1 Scalar (=(Int 1))
    25:1 Ordered idx key
    26:8 Ordered idx value ptr (=17)
    34:1 Scalar (=(Int 2))
    34:1 Ordered idx key
    35:8 Ordered idx value ptr (=35)
    43:1 Scalar (=(Int 3))
    43:1 Ordered idx key
    44:8 Ordered idx value ptr (=53)
    52:1 Scalar (=(Int 4))
    52:1 Ordered idx key
    53:8 Ordered idx value ptr (=70)
    61:8 List count
    69:8 List len
    77:1 Scalar (=(Int 5))
    77:1 List body
    78:8 List count
    86:8 List len
    94:1 Scalar (=(Int 2))
    94:2 List body
    95:1 Scalar (=(Int 3))
    96:8 List count
    104:8 List len
    112:1 Scalar (=(Int 1))
    112:2 List body
    113:1 Scalar (=(Int 2))
    114:8 List count
    122:8 List len
    130:1 Scalar (=(Int 4))
    130:1 List body
    131:8 List count
    139:8 List len
    147:1 Scalar (=(Int 6))
    147:1 List body

    1,2,
    1,3,
    2,1,
    2,2, |}]

let%expect_test "hash-idx" =
  run_test
    "AHashIdx(Dedup(Select([r1.f], r1)) as k, AList(Filter(r1.f = k.f, r1), \
     ascalar(r1.g)), 2)" ;
  [%expect
    {|
    0:8 Table len
    8:8 Table len
    16:104 Table hash
    120:40 Table key map
    160:1 Scalar (=(Int 3))
    160:92 Table values
    161:8 List count
    169:8 List len
    177:1 Scalar (=(Int 4))
    177:1 List body
    178:1 Scalar (=(Int 0))
    179:8 List count
    187:8 List len
    195:1 Scalar (=(Int 5))
    195:1 List body
    196:1 Scalar (=(Int 4))
    197:8 List count
    205:8 List len
    213:1 Scalar (=(Int 6))
    213:1 List body
    214:1 Scalar (=(Int 1))
    215:8 List count
    223:8 List len
    231:1 Scalar (=(Int 2))
    231:2 List body
    232:1 Scalar (=(Int 3))
    233:1 Scalar (=(Int 2))
    234:8 List count
    242:8 List len
    250:1 Scalar (=(Int 1))
    250:2 List body
    251:1 Scalar (=(Int 2))

    2,1,
    2,2, |}]

let example_params =
  [ (Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_p", `Int 1)
  ; (Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_c", `Int 2) ]

let%expect_test "example-1" =
  run_test ~params:example_params
    {|
select([lp.counter, lc.counter], filter(lc.id = id_c && lp.id = id_p,
alist(filter(succ > counter + 1, log) as lp,
atuple([ascalar(lp.id), ascalar(lp.counter),
alist(filter(lp.counter < log.counter &&
log.counter < lp.succ, log) as lc,
atuple([ascalar(lc.id), ascalar(lc.counter)], cross))], cross))))
|} ;
  [%expect
    {|
    0:8 List count
    8:8 List len
    16:8 Tuple len
    16:82 List body
    24:1 Scalar (=(Int 1))
    24:38 Tuple body
    25:1 Scalar (=(Int 1))
    26:8 List count
    34:8 List len
    42:8 Tuple len
    42:20 List body
    50:1 Scalar (=(Int 2))
    50:2 Tuple body
    51:1 Scalar (=(Int 2))
    52:8 Tuple len
    60:1 Scalar (=(Int 3))
    60:2 Tuple body
    61:1 Scalar (=(Int 3))
    62:8 Tuple len
    70:1 Scalar (=(Int 1))
    70:28 Tuple body
    71:1 Scalar (=(Int 4))
    72:8 List count
    80:8 List len
    88:8 Tuple len
    88:10 List body
    96:1 Scalar (=(Int 3))
    96:2 Tuple body
    97:1 Scalar (=(Int 5))

    1,2, |}]

let%expect_test "example-2" =
  run_test ~params:example_params
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
  [%expect
    {|
    0:8 Table len
    8:8 Table len
    16:112 Table hash
    128:72 Table key map
    200:8 Tuple len
    200:264 Table values
    208:1 Scalar (=(Int 2))
    208:2 Tuple body
    209:1 Scalar (=(Int 1))
    210:8 List count
    218:8 List len
    226:0 List body
    226:8 Tuple len
    234:1 Scalar (=(Int 3))
    234:2 Tuple body
    235:1 Scalar (=(Int 1))
    236:8 List count
    244:8 List len
    252:0 List body
    252:8 Tuple len
    260:1 Scalar (=(Int 1))
    260:2 Tuple body
    261:1 Scalar (=(Int 2))
    262:8 List count
    270:8 List len
    278:8 Tuple len
    278:10 List body
    286:1 Scalar (=(Int 1))
    286:2 Tuple body
    287:1 Scalar (=(Int 2))
    288:8 Tuple len
    296:1 Scalar (=(Int 2))
    296:2 Tuple body
    297:1 Scalar (=(Int 3))
    298:8 List count
    306:8 List len
    314:0 List body
    314:8 Tuple len
    322:1 Scalar (=(Int 3))
    322:2 Tuple body
    323:1 Scalar (=(Int 3))
    324:8 List count
    332:8 List len
    340:0 List body
    340:8 Tuple len
    348:1 Scalar (=(Int 1))
    348:2 Tuple body
    349:1 Scalar (=(Int 1))
    350:8 List count
    358:8 List len
    366:0 List body
    366:8 Tuple len
    374:1 Scalar (=(Int 1))
    374:2 Tuple body
    375:1 Scalar (=(Int 3))
    376:8 List count
    384:8 List len
    392:8 Tuple len
    392:20 List body
    400:1 Scalar (=(Int 1))
    400:2 Tuple body
    401:1 Scalar (=(Int 3))
    402:8 Tuple len
    410:1 Scalar (=(Int 4))
    410:2 Tuple body
    411:1 Scalar (=(Int 5))
    412:8 Tuple len
    420:1 Scalar (=(Int 3))
    420:2 Tuple body
    421:1 Scalar (=(Int 2))
    422:8 List count
    430:8 List len
    438:0 List body
    438:8 Tuple len
    446:1 Scalar (=(Int 2))
    446:2 Tuple body
    447:1 Scalar (=(Int 2))
    448:8 List count
    456:8 List len
    464:0 List body

    1,2, |}]

let%expect_test "example-3" =
  run_test ~params:example_params
    {|
select([lp.counter, lc.counter],
  atuple([ahashidx(dedup(select([id as k], log)), 
    alist(select([counter, succ], 
        filter(k = id && counter < succ, log)), 
      atuple([ascalar(counter), ascalar(succ)], cross)), 
    id_p) as lp,
  filter(lc.id = id_c,
    aorderedidx(select([log.counter as k], log), 
      alist(filter(log.counter = k, log),
        atuple([ascalar(log.id), ascalar(log.counter)], cross)), 
      lp.counter, lp.succ) as lc)], cross))
|} ;
  [%expect
    {|
    0:8 Tuple len
    8:8 Table len
    8:436 Tuple body
    16:8 Table len
    24:104 Table hash
    128:24 Table key map
    152:1 Scalar (=(Int 1))
    152:101 Table values
    153:8 List count
    161:8 List len
    169:8 Tuple len
    169:20 List body
    177:1 Scalar (=(Int 1))
    177:2 Tuple body
    178:1 Scalar (=(Int 4))
    179:8 Tuple len
    187:1 Scalar (=(Int 4))
    187:2 Tuple body
    188:1 Scalar (=(Int 6))
    189:1 Scalar (=(Int 2))
    190:8 List count
    198:8 List len
    206:8 Tuple len
    206:10 List body
    214:1 Scalar (=(Int 2))
    214:2 Tuple body
    215:1 Scalar (=(Int 3))
    216:1 Scalar (=(Int 3))
    217:8 List count
    225:8 List len
    233:8 Tuple len
    233:20 List body
    241:1 Scalar (=(Int 3))
    241:2 Tuple body
    242:1 Scalar (=(Int 4))
    243:8 Tuple len
    251:1 Scalar (=(Int 5))
    251:2 Tuple body
    252:1 Scalar (=(Int 6))
    253:8 Ordered idx len (=191)
    261:8 Ordered idx index len (=45)
    269:1 Scalar (=(Int 1))
    269:1 Ordered idx key
    270:8 Ordered idx value ptr (=0)
    278:1 Scalar (=(Int 2))
    278:1 Ordered idx key
    279:8 Ordered idx value ptr (=26)
    287:1 Scalar (=(Int 3))
    287:1 Ordered idx key
    288:8 Ordered idx value ptr (=52)
    296:1 Scalar (=(Int 4))
    296:1 Ordered idx key
    297:8 Ordered idx value ptr (=78)
    305:1 Scalar (=(Int 5))
    305:1 Ordered idx key
    306:8 Ordered idx value ptr (=104)
    314:8 List count
    322:8 List len
    330:8 Tuple len
    330:10 List body
    338:1 Scalar (=(Int 1))
    338:2 Tuple body
    339:1 Scalar (=(Int 1))
    340:8 List count
    348:8 List len
    356:8 Tuple len
    356:10 List body
    364:1 Scalar (=(Int 2))
    364:2 Tuple body
    365:1 Scalar (=(Int 2))
    366:8 List count
    374:8 List len
    382:8 Tuple len
    382:10 List body
    390:1 Scalar (=(Int 3))
    390:2 Tuple body
    391:1 Scalar (=(Int 3))
    392:8 List count
    400:8 List len
    408:8 Tuple len
    408:10 List body
    416:1 Scalar (=(Int 1))
    416:2 Tuple body
    417:1 Scalar (=(Int 4))
    418:8 List count
    426:8 List len
    434:8 Tuple len
    434:10 List body
    442:1 Scalar (=(Int 3))
    442:2 Tuple body
    443:1 Scalar (=(Int 5))

    1,2, |}]
