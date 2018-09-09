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
    Irgen.Make (struct
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
  ((module S : Serialize.S), (module I : Irgen.S), (module C : Codegen.S))

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
  let ret = Unix.system cmd in
  print_endline (Unix.Exit_or_signal.to_string_hum ret)

let%expect_test "ordered-idx" =
  run_test
    "AOrderedIdx(OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc) as k, \
     AList(Filter(r1.f = k.f, r1), ascalar(r1.g)), 1, 3)" ;
  [%expect
    {|
    0:2 Ordered idx len (=67)
    2:8 Ordered idx index len (=45)
    10:1 Scalar (=(Int 0))
    10:1 Ordered idx key
    11:8 Ordered idx value ptr (=0)
    19:1 Scalar (=(Int 1))
    19:1 Ordered idx key
    20:8 Ordered idx value ptr (=2)
    28:1 Scalar (=(Int 2))
    28:1 Ordered idx key
    29:8 Ordered idx value ptr (=5)
    37:1 Scalar (=(Int 3))
    37:1 Ordered idx key
    38:8 Ordered idx value ptr (=8)
    46:1 Scalar (=(Int 4))
    46:1 Ordered idx key
    47:8 Ordered idx value ptr (=10)
    55:1 List count
    56:0 List len
    56:1 Scalar (=(Int 5))
    56:1 List body
    57:1 List count
    58:0 List len
    58:1 Scalar (=(Int 2))
    58:2 List body
    59:1 Scalar (=(Int 3))
    60:1 List count
    61:0 List len
    61:1 Scalar (=(Int 1))
    61:2 List body
    62:1 Scalar (=(Int 2))
    63:1 List count
    64:0 List len
    64:1 Scalar (=(Int 4))
    64:1 List body
    65:1 List count
    66:0 List len
    66:1 Scalar (=(Int 6))
    66:1 List body

    1,2,
    1,3,
    2,1,
    2,2,
    exited normally |}]

let%expect_test "hash-idx" =
  run_test
    "AHashIdx(Dedup(Select([r1.f], r1)) as k, AList(Filter(r1.f = k.f, r1), \
     ascalar(r1.g)), 2)" ;
  [%expect
    {|
    0:8 Table len
    8:8 Table hash len
    16:104 Table hash
    120:40 Table key map
    160:1 Scalar (=(Int 3))
    160:17 Table values
    161:1 List count
    162:0 List len
    162:1 Scalar (=(Int 4))
    162:1 List body
    163:1 Scalar (=(Int 0))
    164:1 List count
    165:0 List len
    165:1 Scalar (=(Int 5))
    165:1 List body
    166:1 Scalar (=(Int 4))
    167:1 List count
    168:0 List len
    168:1 Scalar (=(Int 6))
    168:1 List body
    169:1 Scalar (=(Int 1))
    170:1 List count
    171:0 List len
    171:1 Scalar (=(Int 2))
    171:2 List body
    172:1 Scalar (=(Int 3))
    173:1 Scalar (=(Int 2))
    174:1 List count
    175:0 List len
    175:1 Scalar (=(Int 1))
    175:2 List body
    176:1 Scalar (=(Int 2))

    2,1,
    2,2,
    exited normally |}]

let%expect_test "zip-tuple" =
  run_test
    "atuple([alist(select([r1.f], orderby([r1.f], r1, desc)), ascalar(r1.f)),\n             \
     alist(select([r1.g], orderby([r1.f], r1, desc)), ascalar(r1.g))], zip)"

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
    0:0 List count
    0:0 List len
    0:0 Tuple len
    0:1 Scalar (=(Int 1))
    0:7 Tuple body
    0:12 List body
    1:1 Scalar (=(Int 1))
    2:1 List count
    3:0 List len
    3:0 Tuple len
    3:1 Scalar (=(Int 2))
    3:2 Tuple body
    3:4 List body
    4:1 Scalar (=(Int 2))
    5:0 Tuple len
    5:1 Scalar (=(Int 3))
    5:2 Tuple body
    6:1 Scalar (=(Int 3))
    7:0 Tuple len
    7:1 Scalar (=(Int 1))
    7:5 Tuple body
    8:1 Scalar (=(Int 4))
    9:1 List count
    10:0 List len
    10:0 Tuple len
    10:1 Scalar (=(Int 3))
    10:2 Tuple body
    10:2 List body
    11:1 Scalar (=(Int 5))

    1,2,
    exited normally |}]

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
    8:8 Table hash len
    16:112 Table hash
    128:72 Table key map
    200:0 Tuple len
    200:1 Scalar (=(Int 2))
    200:2 Tuple body
    200:33 Table values
    201:1 Scalar (=(Int 1))
    202:1 List count
    203:0 List len
    203:0 List body
    203:0 Tuple len
    203:1 Scalar (=(Int 3))
    203:2 Tuple body
    204:1 Scalar (=(Int 1))
    205:1 List count
    206:0 List len
    206:0 List body
    206:0 Tuple len
    206:1 Scalar (=(Int 1))
    206:2 Tuple body
    207:1 Scalar (=(Int 2))
    208:1 List count
    209:0 List len
    209:0 Tuple len
    209:1 Scalar (=(Int 1))
    209:2 Tuple body
    209:2 List body
    210:1 Scalar (=(Int 2))
    211:0 Tuple len
    211:1 Scalar (=(Int 2))
    211:2 Tuple body
    212:1 Scalar (=(Int 3))
    213:1 List count
    214:0 List len
    214:0 List body
    214:0 Tuple len
    214:1 Scalar (=(Int 3))
    214:2 Tuple body
    215:1 Scalar (=(Int 3))
    216:1 List count
    217:0 List len
    217:0 List body
    217:0 Tuple len
    217:1 Scalar (=(Int 1))
    217:2 Tuple body
    218:1 Scalar (=(Int 1))
    219:1 List count
    220:0 List len
    220:0 List body
    220:0 Tuple len
    220:1 Scalar (=(Int 1))
    220:2 Tuple body
    221:1 Scalar (=(Int 3))
    222:1 List count
    223:0 List len
    223:0 Tuple len
    223:1 Scalar (=(Int 1))
    223:2 Tuple body
    223:4 List body
    224:1 Scalar (=(Int 3))
    225:0 Tuple len
    225:1 Scalar (=(Int 4))
    225:2 Tuple body
    226:1 Scalar (=(Int 5))
    227:0 Tuple len
    227:1 Scalar (=(Int 3))
    227:2 Tuple body
    228:1 Scalar (=(Int 2))
    229:1 List count
    230:0 List len
    230:0 List body
    230:0 Tuple len
    230:1 Scalar (=(Int 2))
    230:2 Tuple body
    231:1 Scalar (=(Int 2))
    232:1 List count
    233:0 List len
    233:0 List body

    2,2,
    exited normally |}]

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
    0:2 Tuple len
    2:8 Table len
    2:225 Tuple body
    10:8 Table hash len
    18:104 Table hash
    122:24 Table key map
    146:1 Scalar (=(Int 1))
    146:16 Table values
    147:1 List count
    148:0 List len
    148:0 Tuple len
    148:1 Scalar (=(Int 1))
    148:2 Tuple body
    148:4 List body
    149:1 Scalar (=(Int 4))
    150:0 Tuple len
    150:1 Scalar (=(Int 4))
    150:2 Tuple body
    151:1 Scalar (=(Int 6))
    152:1 Scalar (=(Int 2))
    153:1 List count
    154:0 List len
    154:0 Tuple len
    154:1 Scalar (=(Int 2))
    154:2 Tuple body
    154:2 List body
    155:1 Scalar (=(Int 3))
    156:1 Scalar (=(Int 3))
    157:1 List count
    158:0 List len
    158:0 Tuple len
    158:1 Scalar (=(Int 3))
    158:2 Tuple body
    158:4 List body
    159:1 Scalar (=(Int 4))
    160:0 Tuple len
    160:1 Scalar (=(Int 5))
    160:2 Tuple body
    161:1 Scalar (=(Int 6))
    162:2 Ordered idx len (=65)
    164:8 Ordered idx index len (=45)
    172:1 Scalar (=(Int 1))
    172:1 Ordered idx key
    173:8 Ordered idx value ptr (=0)
    181:1 Scalar (=(Int 2))
    181:1 Ordered idx key
    182:8 Ordered idx value ptr (=2)
    190:1 Scalar (=(Int 3))
    190:1 Ordered idx key
    191:8 Ordered idx value ptr (=4)
    199:1 Scalar (=(Int 4))
    199:1 Ordered idx key
    200:8 Ordered idx value ptr (=6)
    208:1 Scalar (=(Int 5))
    208:1 Ordered idx key
    209:8 Ordered idx value ptr (=8)
    217:0 List count
    217:0 List len
    217:0 Tuple len
    217:1 Scalar (=(Int 1))
    217:2 Tuple body
    217:2 List body
    218:1 Scalar (=(Int 1))
    219:0 List count
    219:0 List len
    219:0 Tuple len
    219:1 Scalar (=(Int 2))
    219:2 Tuple body
    219:2 List body
    220:1 Scalar (=(Int 2))
    221:0 List count
    221:0 List len
    221:0 Tuple len
    221:1 Scalar (=(Int 3))
    221:2 Tuple body
    221:2 List body
    222:1 Scalar (=(Int 3))
    223:0 List count
    223:0 List len
    223:0 Tuple len
    223:1 Scalar (=(Int 1))
    223:2 Tuple body
    223:2 List body
    224:1 Scalar (=(Int 4))
    225:0 List count
    225:0 List len
    225:0 Tuple len
    225:1 Scalar (=(Int 3))
    225:2 Tuple body
    225:2 List body
    226:1 Scalar (=(Int 5))

    1,2,
    exited normally |}]
