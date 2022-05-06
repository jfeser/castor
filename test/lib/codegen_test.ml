open Util
open Test_util
open Abslayout_load

let run_test ?(params = []) ?(print_layout = false) ?(fork = false) layout_str =
  let layout_file = Filename_unix.temp_file "layout" "txt" in
  let conn = Lazy.force test_db_conn in
  let (module C) = Setup.make_modules () in

  let run_compiler layout =
    let out_dir = Filename_unix.temp_dir "bin" "" in
    let exe_fn, data_fn =
      let params = List.map ~f:(fun (n, t, _) -> (n, t)) params in
      C.compile ~out_dir ~layout_log:layout_file ~gprof:false ~params layout
    in
    if print_layout then
      In_channel.input_all (In_channel.create layout_file) |> print_endline;
    let cmd =
      let params_str =
        List.map params ~f:(fun (_, _, v) -> Value.to_sql v)
        |> String.concat ~sep:" "
      in
      sprintf "%s -p %s %s" exe_fn data_fn params_str
    in
    let cmd_ch = Core_unix.open_process_in cmd in
    let cmd_output = In_channel.input_all cmd_ch in
    let ret = Core_unix.close_process_in cmd_ch in
    print_endline cmd_output;
    print_endline (Core_unix.Exit_or_signal.to_string_hum ret);
    Out_channel.flush stdout
  in

  let run () =
    let layout =
      let params =
        List.map params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
        |> Set.of_list (module Name)
      in
      load_string_nostrip_exn conn ~params layout_str
      |> Equiv.annotate
      |> Abslayout_fold.Data.annotate conn
      |> Type.annotate
      |> V.map_meta (fun m ->
             object
               method fold_stream = m#fold_stream
               method resolved = m#meta#meta#meta#resolved
               method type_ = m#type_
               method eq = m#meta#meta#eq
             end)
    in

    if fork then run_in_fork (fun () -> run_compiler layout)
    else run_compiler layout
  in
  Exn.handle_uncaught ~exit:false run

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

(* let%expect_test "example-2" = *)
(*   Demomatch.(run_test ~params:example_params (example2 "log")); *)
(* [%expect {| *)
   (*           1|2 *)

   (*           exited normally |}] *)

(* let%expect_test "example-2-str" = *)
(*   Demomatch.( *)
(*     run_test ~print_layout:true ~params:example_str_params (example2 "log_str")); *)
(*   [%expect *)
(* {| *)
   (*           0:2 Table len (=142) *)
   (*           2:2 Table hash len (=104) *)
   (*           4:104 Table hash *)
   (*           108:1 Table map len (=2) *)
   (*           109:2 Table key map *)
   (*           109:1 Map entry (0 => 14) *)
   (*           110:1 Map entry (1 => 0) *)
   (*           111:31 Table values *)
   (*           111:1 Tuple len (=8) *)
   (*           112:7 Tuple body *)
   (*           112:3 Scalar (=(String foo)) *)
   (*           112:3 String body *)
   (*           112:0 String length (=3) *)
   (*           115:4 Scalar (=(String bar)) *)
   (*           115:1 String length (=3) *)
   (*           116:3 String body *)
   (*           119:1 List count (=2) *)
   (*           120:1 List len (=6) *)
   (*           121:4 List body *)
   (*           121:2 Tuple body *)
   (*           121:1 Scalar (=(Int 1)) *)
   (*           121:0 Tuple len (=2) *)
   (*           122:1 Scalar (=(Int 3)) *)
   (*           123:2 Tuple body *)
   (*           123:1 Scalar (=(Int 4)) *)
   (*           123:0 Tuple len (=2) *)
   (*           124:1 Scalar (=(Int 5)) *)
   (*           125:1 Tuple len (=13) *)
   (*           126:12 Tuple body *)
   (*           126:3 Scalar (=(String foo)) *)
   (*           126:3 String body *)
   (*           126:0 String length (=3) *)
   (*           129:9 Scalar (=(String fizzbuzz)) *)
   (*           129:1 String length (=8) *)
   (*           130:8 String body *)
   (*           138:1 List count (=1) *)
   (*           139:1 List len (=4) *)
   (*           140:2 List body *)
   (*           140:2 Tuple body *)
   (*           140:1 Scalar (=(Int 1)) *)
   (*           140:0 Tuple len (=2) *)
   (*           141:1 Scalar (=(Int 2)) *)

   (*           1|2 *)

   (*           exited normally |}] *)

(* let%expect_test "example-3" = *)
(*   Demomatch.(run_test ~params:example_params (example3 "log")); *)
(* [%expect {| *)
   (*           1|2 *)

   (*           exited normally |}] *)

(* let%expect_test "example-3-str" = *)
(*   Demomatch.( *)
(*     run_test ~print_layout:true ~params:example_str_params (example3 "log_str")); *)
(*   [%expect *)
(* {| *)
   (*           0:2 Tuple len (=198) *)
   (*           2:196 Tuple body *)
   (*           2:2 Table len (=145) *)
   (*           4:2 Table hash len (=104) *)
   (*           6:104 Table hash *)
   (*           110:1 Table map len (=3) *)
   (*           111:3 Table key map *)
   (*           111:1 Map entry (0 => 0) *)
   (*           112:1 Map entry (1 => 23) *)
   (*           113:1 Map entry (2 => 10) *)
   (*           114:33 Table values *)
   (*           114:4 Scalar (=(String bar)) *)
   (*           114:1 String length (=3) *)
   (*           115:3 String body *)
   (*           118:1 List count (=2) *)
   (*           119:1 List len (=6) *)
   (*           120:4 List body *)
   (*           120:2 Tuple body *)
   (*           120:1 Scalar (=(Int 3)) *)
   (*           120:0 Tuple len (=2) *)
   (*           121:1 Scalar (=(Int 4)) *)
   (*           122:2 Tuple body *)
   (*           122:1 Scalar (=(Int 5)) *)
   (*           122:0 Tuple len (=2) *)
   (*           123:1 Scalar (=(Int 6)) *)
   (*           124:9 Scalar (=(String fizzbuzz)) *)
   (*           124:1 String length (=8) *)
   (*           125:8 String body *)
   (*           133:1 List count (=1) *)
   (*           134:1 List len (=4) *)
   (*           135:2 List body *)
   (*           135:2 Tuple body *)
   (*           135:1 Scalar (=(Int 2)) *)
   (*           135:0 Tuple len (=2) *)
   (*           136:1 Scalar (=(Int 3)) *)
   (*           137:4 Scalar (=(String foo)) *)
   (*           137:1 String length (=3) *)
   (*           138:3 String body *)
   (*           141:1 List count (=2) *)
   (*           142:1 List len (=6) *)
   (*           143:4 List body *)
   (*           143:2 Tuple body *)
   (*           143:1 Scalar (=(Int 1)) *)
   (*           143:0 Tuple len (=2) *)
   (*           144:1 Scalar (=(Int 4)) *)
   (*           145:2 Tuple body *)
   (*           145:1 Scalar (=(Int 4)) *)
   (*           145:0 Tuple len (=2) *)
   (*           146:1 Scalar (=(Int 6)) *)
   (*           147:1 Ordered idx len (=51) *)
   (*           148:10 Ordered idx map *)
   (*           148:1 Ordered idx key *)
   (*           148:1 Scalar (=(Int 1)) *)
   (*           148:0 Ordered idx index len (=10) *)
   (*           149:1 Ordered idx ptr (=0) *)
   (*           150:1 Ordered idx key *)
   (*           150:1 Scalar (=(Int 2)) *)
   (*           151:1 Ordered idx ptr (=7) *)
   (*           152:1 Ordered idx key *)
   (*           152:1 Scalar (=(Int 3)) *)
   (*           153:1 Ordered idx ptr (=19) *)
   (*           154:1 Ordered idx key *)
   (*           154:1 Scalar (=(Int 4)) *)
   (*           155:1 Ordered idx ptr (=26) *)
   (*           156:1 Ordered idx key *)
   (*           156:1 Scalar (=(Int 5)) *)
   (*           157:1 Ordered idx ptr (=33) *)
   (*           158:40 Ordered idx body *)
   (*           158:1 List len (=7) *)
   (*           158:0 List count (=1) *)
   (*           159:6 List body *)
   (*           159:1 Tuple len (=6) *)
   (*           160:5 Tuple body *)
   (*           160:4 Scalar (=(String foo)) *)
   (*           160:1 String length (=3) *)
   (*           161:3 String body *)
   (*           164:1 Scalar (=(Int 1)) *)
   (*           165:1 List len (=12) *)
   (*           165:0 List count (=1) *)
   (*           166:11 List body *)
   (*           166:1 Tuple len (=11) *)
   (*           167:10 Tuple body *)
   (*           167:9 Scalar (=(String fizzbuzz)) *)
   (*           167:1 String length (=8) *)
   (*           168:8 String body *)
   (*           176:1 Scalar (=(Int 2)) *)
   (*           177:1 List len (=7) *)
   (*           177:0 List count (=1) *)
   (*           178:6 List body *)
   (*           178:1 Tuple len (=6) *)
   (*           179:5 Tuple body *)
   (*           179:4 Scalar (=(String bar)) *)
   (*           179:1 String length (=3) *)
   (*           180:3 String body *)
   (*           183:1 Scalar (=(Int 3)) *)
   (*           184:1 List len (=7) *)
   (*           184:0 List count (=1) *)
   (*           185:6 List body *)
   (*           185:1 Tuple len (=6) *)
   (*           186:5 Tuple body *)
   (*           186:4 Scalar (=(String foo)) *)
   (*           186:1 String length (=3) *)
   (*           187:3 String body *)
   (*           190:1 Scalar (=(Int 4)) *)
   (*           191:1 List len (=7) *)
   (*           191:0 List count (=1) *)
   (*           192:6 List body *)
   (*           192:1 Tuple len (=6) *)
   (*           193:5 Tuple body *)
   (*           193:4 Scalar (=(String bar)) *)
   (*           193:1 String length (=3) *)
   (*           194:3 String body *)
   (*           197:1 Scalar (=(Int 5)) *)

   (*           1|2 *)

   (*           exited normally |}] *)
