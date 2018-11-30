open Core
open Abslayout
open Test_util

let make_modules ?layout_file () =
  let module M = Abslayout_db.Make (Test_db) in
  let module S =
    Serialize.Make (struct
        let layout_map_channel = Option.map layout_file ~f:Out_channel.create
      end)
      (M)
  in
  let module I =
    Irgen.Make (struct
        let code_only = false

        let debug = false
      end)
      (M)
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
  ( (module M : Abslayout_db.S)
  , (module S : Serialize.S)
  , (module I : Irgen.S)
  , (module C : Codegen.S) )

let make_modules_db ?layout_file () =
  let module M = Abslayout_db.Make (struct
    let conn = create_db "postgresql://localhost:5433/demomatch"
  end) in
  let module S =
    Serialize.Make (struct
        let layout_map_channel = Option.map layout_file ~f:Out_channel.create
      end)
      (M)
  in
  let module I =
    Irgen.Make (struct
        let code_only = false

        let debug = false
      end)
      (M)
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
  ( (module M : Abslayout_db.S)
  , (module S : Serialize.S)
  , (module I : Irgen.S)
  , (module C : Codegen.S) )

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

let run_test ?(params = []) ?(modules = make_modules) ?(print_layout = true)
    ?(fork = false) layout_str =
  let layout_file = Filename.temp_file "layout" "txt" in
  let (module M), (module S), (module I), (module C) = modules ~layout_file () in
  let layout =
    let params =
      List.map params ~f:(fun (n, _) -> n)
      |> Set.of_list (module Name.Compare_no_type)
    in
    of_string_exn layout_str |> M.resolve ~params
  in
  M.annotate_schema layout ;
  let layout = M.annotate_key_layouts layout in
  annotate_foreach layout ;
  let out_dir = Filename.temp_dir "bin" "" in
  let run_compiler () =
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
  if fork then run_in_fork run_compiler else run_compiler ()

let%expect_test "ordered-idx" =
  run_test ~print_layout:false
    "AOrderedIdx(OrderBy([r1.f], Dedup(Select([r1.f], r1)), asc) as k, \
     AList(Filter(r1.f = k.f, r1), ascalar(r1.g)), 1, 3)" ;
  [%expect {|
    1,2,
    1,3,
    2,1,
    2,2,

    exited normally |}]

let%expect_test "agg" =
  [%expect {||}] ;
  run_test ~print_layout:false ~fork:false
    "select([1.0 + 2.0, avg(r2.a), count(), sum(r2.a), min(r2.a), max(r2.a)], \
     alist(r2, ascalar(r2.a)))" ;
  [%expect
    {|
    3.000000,7.978000,5,39.890000,-0.420000,34.420000,

    exited normally |}]

let%expect_test "hash-idx" =
  run_test ~print_layout:false
    "AHashIdx(Dedup(Select([r1.f], r1)) as k, AList(Filter(r1.f = k.f, r1), \
     ascalar(r1.g)), 2)" ;
  [%expect {|
    2,1,
    2,2,

    exited normally |}]

let example_params =
  [ (Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_p", Value.Int 1)
  ; (Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_c", Int 2) ]

let example_str_params =
  [ ( Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_p"
    , Value.String "foo" )
  ; ( Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_c"
    , String "fizzbuzz" ) ]

let example_db_params =
  [ ( Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_p"
    , Value.String "-1451410871729396224" )
  ; ( Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_c"
    , String "8557539814359574196" ) ]

let%expect_test "strops" =
  run_test ~params:[] ~print_layout:false
    {|
select([strlen("test"), strpos("testing", "in")], ascalar(0))
|} ;
  [%expect {|
    4,5,

    exited normally |}]

let%expect_test "example-1" =
  run_test ~params:example_params ~print_layout:false
    {|
select([lp.counter, lc.counter], filter(lc.id = id_c && lp.id = id_p,
alist(filter(succ > counter + 1, log) as lp,
atuple([ascalar(lp.id), ascalar(lp.counter),
alist(filter(lp.counter < log.counter &&
log.counter < lp.succ, log) as lc,
atuple([ascalar(lc.id), ascalar(lc.counter)], cross))], cross))))
|} ;
  [%expect {|
    1,2,

    exited normally |}]

let%expect_test "example-1-str" =
  run_test ~params:example_str_params ~print_layout:false
    {|
select([lp.counter, lc.counter], filter(lc.id = id_c && lp.id = id_p,
alist(filter(succ > counter + 1, log_str) as lp,
atuple([ascalar(lp.id), ascalar(lp.counter),
alist(filter(lp.counter < log_str.counter &&
log_str.counter < lp.succ, log_str) as lc,
atuple([ascalar(lc.id), ascalar(lc.counter)], cross))], cross))))
|} ;
  [%expect {|
    1,2,

    exited normally |}]

let%expect_test "example-2" =
  run_test ~params:example_params ~print_layout:false
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
    1,2,

    exited normally |}]

let%expect_test "example-2-str" =
  run_test ~params:example_str_params ~print_layout:false
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
    1,2,

    exited normally |}]

let%expect_test "example-3" =
  run_test ~params:example_params ~print_layout:false
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
    1,2,

    exited normally |}]

let%expect_test "example-3-str" =
  run_test ~params:example_str_params ~print_layout:false
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
    1,2,

    exited normally |}]
