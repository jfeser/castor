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
    16:138 List body
    24:1 Scalar (=(Int 1))
    24:48 Tuple body
    25:1 Scalar (=(Int 1))
    26:8 List count
    34:8 List len
    42:8 Tuple len
    42:30 List body
    50:1 Scalar (=(Int 2))
    50:2 Tuple body
    51:1 Scalar (=(Int 2))
    52:8 Tuple len
    60:1 Scalar (=(Int 3))
    60:2 Tuple body
    61:1 Scalar (=(Int 3))
    62:8 Tuple len
    70:1 Scalar (=(Int 5))
    70:2 Tuple body
    71:1 Scalar (=(Int 3))
    72:8 Tuple len
    80:1 Scalar (=(Int 4))
    80:48 Tuple body
    81:1 Scalar (=(Int 1))
    82:8 List count
    90:8 List len
    98:8 Tuple len
    98:30 List body
    106:1 Scalar (=(Int 2))
    106:2 Tuple body
    107:1 Scalar (=(Int 2))
    108:8 Tuple len
    116:1 Scalar (=(Int 3))
    116:2 Tuple body
    117:1 Scalar (=(Int 3))
    118:8 Tuple len
    126:1 Scalar (=(Int 5))
    126:2 Tuple body
    127:1 Scalar (=(Int 3))
    128:8 Tuple len
    136:1 Scalar (=(Int 5))
    136:18 Tuple body
    137:1 Scalar (=(Int 3))
    138:8 List count
    146:8 List len
    154:0 List body |}]

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
    128:200 Table key map
    328:8 Tuple len
    328:710 Table values
    336:1 Scalar (=(Int 3))
    336:2 Tuple body
    337:1 Scalar (=(Int 4))
    338:8 List count
    346:8 List len
    354:0 List body
    354:8 Tuple len
    362:1 Scalar (=(Int 4))
    362:2 Tuple body
    363:1 Scalar (=(Int 3))
    364:8 List count
    372:8 List len
    380:8 Tuple len
    380:10 List body
    388:1 Scalar (=(Int 1))
    388:2 Tuple body
    389:1 Scalar (=(Int 3))
    390:8 Tuple len
    398:1 Scalar (=(Int 3))
    398:2 Tuple body
    399:1 Scalar (=(Int 5))
    400:8 List count
    408:8 List len
    416:0 List body
    416:8 Tuple len
    424:1 Scalar (=(Int 4))
    424:2 Tuple body
    425:1 Scalar (=(Int 2))
    426:8 List count
    434:8 List len
    442:8 Tuple len
    442:10 List body
    450:1 Scalar (=(Int 1))
    450:2 Tuple body
    451:1 Scalar (=(Int 2))
    452:8 Tuple len
    460:1 Scalar (=(Int 2))
    460:2 Tuple body
    461:1 Scalar (=(Int 5))
    462:8 List count
    470:8 List len
    478:0 List body
    478:8 Tuple len
    486:1 Scalar (=(Int 4))
    486:2 Tuple body
    487:1 Scalar (=(Int 1))
    488:8 List count
    496:8 List len
    504:0 List body
    504:8 Tuple len
    512:1 Scalar (=(Int 2))
    512:2 Tuple body
    513:1 Scalar (=(Int 1))
    514:8 List count
    522:8 List len
    530:0 List body
    530:8 Tuple len
    538:1 Scalar (=(Int 2))
    538:2 Tuple body
    539:1 Scalar (=(Int 4))
    540:8 List count
    548:8 List len
    556:0 List body
    556:8 Tuple len
    564:1 Scalar (=(Int 5))
    564:2 Tuple body
    565:1 Scalar (=(Int 4))
    566:8 List count
    574:8 List len
    582:0 List body
    582:8 Tuple len
    590:1 Scalar (=(Int 3))
    590:2 Tuple body
    591:1 Scalar (=(Int 1))
    592:8 List count
    600:8 List len
    608:0 List body
    608:8 Tuple len
    616:1 Scalar (=(Int 1))
    616:2 Tuple body
    617:1 Scalar (=(Int 2))
    618:8 List count
    626:8 List len
    634:8 Tuple len
    634:10 List body
    642:1 Scalar (=(Int 1))
    642:2 Tuple body
    643:1 Scalar (=(Int 2))
    644:8 Tuple len
    652:1 Scalar (=(Int 4))
    652:2 Tuple body
    653:1 Scalar (=(Int 5))
    654:8 List count
    662:8 List len
    670:8 Tuple len
    670:10 List body
    678:1 Scalar (=(Int 1))
    678:2 Tuple body
    679:1 Scalar (=(Int 3))
    680:8 Tuple len
    688:1 Scalar (=(Int 4))
    688:2 Tuple body
    689:1 Scalar (=(Int 4))
    690:8 List count
    698:8 List len
    706:0 List body
    706:8 Tuple len
    714:1 Scalar (=(Int 2))
    714:2 Tuple body
    715:1 Scalar (=(Int 3))
    716:8 List count
    724:8 List len
    732:0 List body
    732:8 Tuple len
    740:1 Scalar (=(Int 5))
    740:2 Tuple body
    741:1 Scalar (=(Int 2))
    742:8 List count
    750:8 List len
    758:0 List body
    758:8 Tuple len
    766:1 Scalar (=(Int 3))
    766:2 Tuple body
    767:1 Scalar (=(Int 3))
    768:8 List count
    776:8 List len
    784:0 List body
    784:8 Tuple len
    792:1 Scalar (=(Int 1))
    792:2 Tuple body
    793:1 Scalar (=(Int 1))
    794:8 List count
    802:8 List len
    810:0 List body
    810:8 Tuple len
    818:1 Scalar (=(Int 1))
    818:2 Tuple body
    819:1 Scalar (=(Int 4))
    820:8 List count
    828:8 List len
    836:0 List body
    836:8 Tuple len
    844:1 Scalar (=(Int 5))
    844:2 Tuple body
    845:1 Scalar (=(Int 1))
    846:8 List count
    854:8 List len
    862:0 List body
    862:8 Tuple len
    870:1 Scalar (=(Int 5))
    870:2 Tuple body
    871:1 Scalar (=(Int 3))
    872:8 List count
    880:8 List len
    888:0 List body
    888:8 Tuple len
    896:1 Scalar (=(Int 1))
    896:2 Tuple body
    897:1 Scalar (=(Int 5))
    898:8 List count
    906:8 List len
    914:8 Tuple len
    914:10 List body
    922:1 Scalar (=(Int 1))
    922:2 Tuple body
    923:1 Scalar (=(Int 3))
    924:8 Tuple len
    932:1 Scalar (=(Int 5))
    932:2 Tuple body
    933:1 Scalar (=(Int 5))
    934:8 List count
    942:8 List len
    950:0 List body
    950:8 Tuple len
    958:1 Scalar (=(Int 1))
    958:2 Tuple body
    959:1 Scalar (=(Int 3))
    960:8 List count
    968:8 List len
    976:8 Tuple len
    976:10 List body
    984:1 Scalar (=(Int 1))
    984:2 Tuple body
    985:1 Scalar (=(Int 3))
    986:8 Tuple len
    994:1 Scalar (=(Int 3))
    994:2 Tuple body
    995:1 Scalar (=(Int 2))
    996:8 List count
    1004:8 List len
    1012:0 List body
    1012:8 Tuple len
    1020:1 Scalar (=(Int 2))
    1020:2 Tuple body
    1021:1 Scalar (=(Int 2))
    1022:8 List count
    1030:8 List len
    1038:0 List body |}]

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
    8:526 Tuple body
    16:8 Table len
    24:104 Table hash
    128:40 Table key map
    168:1 Scalar (=(Int 1))
    168:135 Table values
    169:8 List count
    177:8 List len
    185:8 Tuple len
    185:10 List body
    193:1 Scalar (=(Int 1))
    193:2 Tuple body
    194:1 Scalar (=(Int 4))
    195:1 Scalar (=(Int 2))
    196:8 List count
    204:8 List len
    212:8 Tuple len
    212:10 List body
    220:1 Scalar (=(Int 2))
    220:2 Tuple body
    221:1 Scalar (=(Int 3))
    222:1 Scalar (=(Int 3))
    223:8 List count
    231:8 List len
    239:8 Tuple len
    239:10 List body
    247:1 Scalar (=(Int 3))
    247:2 Tuple body
    248:1 Scalar (=(Int 4))
    249:1 Scalar (=(Int 4))
    250:8 List count
    258:8 List len
    266:8 Tuple len
    266:10 List body
    274:1 Scalar (=(Int 1))
    274:2 Tuple body
    275:1 Scalar (=(Int 6))
    276:1 Scalar (=(Int 5))
    277:8 List count
    285:8 List len
    293:8 Tuple len
    293:10 List body
    301:1 Scalar (=(Int 3))
    301:2 Tuple body
    302:1 Scalar (=(Int 6))
    303:8 Ordered idx len (=231)
    311:8 Ordered idx index len (=45)
    319:1 Scalar (=(Int 1))
    319:1 Ordered idx key
    320:8 Ordered idx value ptr (=0)
    328:1 Scalar (=(Int 1))
    328:1 Ordered idx key
    329:8 Ordered idx value ptr (=36)
    337:1 Scalar (=(Int 2))
    337:1 Ordered idx key
    338:8 Ordered idx value ptr (=72)
    346:1 Scalar (=(Int 3))
    346:1 Ordered idx key
    347:8 Ordered idx value ptr (=98)
    355:1 Scalar (=(Int 3))
    355:1 Ordered idx key
    356:8 Ordered idx value ptr (=134)
    364:8 List count
    372:8 List len
    380:8 Tuple len
    380:20 List body
    388:1 Scalar (=(Int 1))
    388:2 Tuple body
    389:1 Scalar (=(Int 1))
    390:8 Tuple len
    398:1 Scalar (=(Int 4))
    398:2 Tuple body
    399:1 Scalar (=(Int 1))
    400:8 List count
    408:8 List len
    416:8 Tuple len
    416:20 List body
    424:1 Scalar (=(Int 1))
    424:2 Tuple body
    425:1 Scalar (=(Int 1))
    426:8 Tuple len
    434:1 Scalar (=(Int 4))
    434:2 Tuple body
    435:1 Scalar (=(Int 1))
    436:8 List count
    444:8 List len
    452:8 Tuple len
    452:10 List body
    460:1 Scalar (=(Int 2))
    460:2 Tuple body
    461:1 Scalar (=(Int 2))
    462:8 List count
    470:8 List len
    478:8 Tuple len
    478:20 List body
    486:1 Scalar (=(Int 3))
    486:2 Tuple body
    487:1 Scalar (=(Int 3))
    488:8 Tuple len
    496:1 Scalar (=(Int 5))
    496:2 Tuple body
    497:1 Scalar (=(Int 3))
    498:8 List count
    506:8 List len
    514:8 Tuple len
    514:20 List body
    522:1 Scalar (=(Int 3))
    522:2 Tuple body
    523:1 Scalar (=(Int 3))
    524:8 Tuple len
    532:1 Scalar (=(Int 5))
    532:2 Tuple body
    533:1 Scalar (=(Int 3)) |}]
