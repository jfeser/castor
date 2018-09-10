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
    10:1 Scalar (=(Int 0))
    10:1 Ordered idx key
    11:8 Ordered idx value ptr (=55)
    19:1 Scalar (=(Int 1))
    19:1 Ordered idx key
    19:1 Scalar (=(Int 1))
    19:1 Ordered idx key
    20:8 Ordered idx value ptr (=57)
    28:1 Scalar (=(Int 2))
    28:1 Ordered idx key
    28:1 Scalar (=(Int 2))
    28:1 Ordered idx key
    29:8 Ordered idx value ptr (=60)
    37:1 Scalar (=(Int 3))
    37:1 Ordered idx key
    37:1 Scalar (=(Int 3))
    37:1 Ordered idx key
    38:8 Ordered idx value ptr (=63)
    46:1 Scalar (=(Int 4))
    46:1 Ordered idx key
    46:1 Scalar (=(Int 4))
    46:1 Ordered idx key
    47:8 Ordered idx value ptr (=65)
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
    0:2 Table len
    2:8 Table hash len
    10:104 Table hash
    114:40 Table key map
    154:1 Scalar (=(Int 3))
    154:17 Table values
    155:1 List count
    156:0 List len
    156:1 Scalar (=(Int 4))
    156:1 List body
    157:1 Scalar (=(Int 0))
    158:1 List count
    159:0 List len
    159:1 Scalar (=(Int 5))
    159:1 List body
    160:1 Scalar (=(Int 4))
    161:1 List count
    162:0 List len
    162:1 Scalar (=(Int 6))
    162:1 List body
    163:1 Scalar (=(Int 1))
    164:1 List count
    165:0 List len
    165:1 Scalar (=(Int 2))
    165:2 List body
    166:1 Scalar (=(Int 3))
    167:1 Scalar (=(Int 2))
    168:1 List count
    169:0 List len
    169:1 Scalar (=(Int 1))
    169:2 List body
    170:1 Scalar (=(Int 2))

    2,1,
    2,2,
    exited normally |}]

(* let%expect_test "zip-tuple" =
 *   run_test
 *     "atuple([alist(select([r1.f], orderby([r1.f], r1, desc)), ascalar(r1.f)),\n             \
 *      alist(select([r1.g], orderby([r1.f], r1, desc)), ascalar(r1.g))], zip)" *)

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
    0:2 Table len
    2:8 Table hash len
    10:108 Table hash
    118:72 Table key map
    190:0 Tuple len
    190:1 Scalar (=(Int 2))
    190:2 Tuple body
    190:33 Table values
    191:1 Scalar (=(Int 1))
    192:1 List count
    193:0 List len
    193:0 List body
    193:0 Tuple len
    193:1 Scalar (=(Int 3))
    193:2 Tuple body
    194:1 Scalar (=(Int 1))
    195:1 List count
    196:0 List len
    196:0 List body
    196:0 Tuple len
    196:1 Scalar (=(Int 1))
    196:2 Tuple body
    197:1 Scalar (=(Int 2))
    198:1 List count
    199:0 List len
    199:0 Tuple len
    199:1 Scalar (=(Int 1))
    199:2 Tuple body
    199:2 List body
    200:1 Scalar (=(Int 2))
    201:0 Tuple len
    201:1 Scalar (=(Int 2))
    201:2 Tuple body
    202:1 Scalar (=(Int 3))
    203:1 List count
    204:0 List len
    204:0 List body
    204:0 Tuple len
    204:1 Scalar (=(Int 3))
    204:2 Tuple body
    205:1 Scalar (=(Int 3))
    206:1 List count
    207:0 List len
    207:0 List body
    207:0 Tuple len
    207:1 Scalar (=(Int 1))
    207:2 Tuple body
    208:1 Scalar (=(Int 1))
    209:1 List count
    210:0 List len
    210:0 List body
    210:0 Tuple len
    210:1 Scalar (=(Int 1))
    210:2 Tuple body
    211:1 Scalar (=(Int 3))
    212:1 List count
    213:0 List len
    213:0 Tuple len
    213:1 Scalar (=(Int 1))
    213:2 Tuple body
    213:4 List body
    214:1 Scalar (=(Int 3))
    215:0 Tuple len
    215:1 Scalar (=(Int 4))
    215:2 Tuple body
    216:1 Scalar (=(Int 5))
    217:0 Tuple len
    217:1 Scalar (=(Int 3))
    217:2 Tuple body
    218:1 Scalar (=(Int 2))
    219:1 List count
    220:0 List len
    220:0 List body
    220:0 Tuple len
    220:1 Scalar (=(Int 2))
    220:2 Tuple body
    221:1 Scalar (=(Int 2))
    222:1 List count
    223:0 List len
    223:0 List body

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
    2:2 Table len
    2:219 Tuple body
    4:8 Table hash len
    12:104 Table hash
    116:24 Table key map
    140:1 Scalar (=(Int 1))
    140:16 Table values
    141:1 List count
    142:0 List len
    142:0 Tuple len
    142:1 Scalar (=(Int 1))
    142:2 Tuple body
    142:4 List body
    143:1 Scalar (=(Int 4))
    144:0 Tuple len
    144:1 Scalar (=(Int 4))
    144:2 Tuple body
    145:1 Scalar (=(Int 6))
    146:1 Scalar (=(Int 2))
    147:1 List count
    148:0 List len
    148:0 Tuple len
    148:1 Scalar (=(Int 2))
    148:2 Tuple body
    148:2 List body
    149:1 Scalar (=(Int 3))
    150:1 Scalar (=(Int 3))
    151:1 List count
    152:0 List len
    152:0 Tuple len
    152:1 Scalar (=(Int 3))
    152:2 Tuple body
    152:4 List body
    153:1 Scalar (=(Int 4))
    154:0 Tuple len
    154:1 Scalar (=(Int 5))
    154:2 Tuple body
    155:1 Scalar (=(Int 6))
    156:2 Ordered idx len (=65)
    158:8 Ordered idx index len (=45)
    166:1 Scalar (=(Int 1))
    166:1 Ordered idx key
    166:1 Scalar (=(Int 1))
    166:1 Ordered idx key
    167:8 Ordered idx value ptr (=211)
    175:1 Scalar (=(Int 2))
    175:1 Ordered idx key
    175:1 Scalar (=(Int 2))
    175:1 Ordered idx key
    176:8 Ordered idx value ptr (=213)
    184:1 Scalar (=(Int 3))
    184:1 Ordered idx key
    184:1 Scalar (=(Int 3))
    184:1 Ordered idx key
    185:8 Ordered idx value ptr (=215)
    193:1 Scalar (=(Int 4))
    193:1 Ordered idx key
    193:1 Scalar (=(Int 4))
    193:1 Ordered idx key
    194:8 Ordered idx value ptr (=217)
    202:1 Scalar (=(Int 5))
    202:1 Ordered idx key
    202:1 Scalar (=(Int 5))
    202:1 Ordered idx key
    203:8 Ordered idx value ptr (=219)
    211:0 List count
    211:0 List len
    211:0 Tuple len
    211:1 Scalar (=(Int 1))
    211:2 Tuple body
    211:2 List body
    212:1 Scalar (=(Int 1))
    213:0 List count
    213:0 List len
    213:0 Tuple len
    213:1 Scalar (=(Int 2))
    213:2 Tuple body
    213:2 List body
    214:1 Scalar (=(Int 2))
    215:0 List count
    215:0 List len
    215:0 Tuple len
    215:1 Scalar (=(Int 3))
    215:2 Tuple body
    215:2 List body
    216:1 Scalar (=(Int 3))
    217:0 List count
    217:0 List len
    217:0 Tuple len
    217:1 Scalar (=(Int 1))
    217:2 Tuple body
    217:2 List body
    218:1 Scalar (=(Int 4))
    219:0 List count
    219:0 List len
    219:0 Tuple len
    219:1 Scalar (=(Int 3))
    219:2 Tuple body
    219:2 List body
    220:1 Scalar (=(Int 5))

    1,2,
    exited normally |}]
