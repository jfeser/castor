open Core
open OUnit2
open Cmph
open Util

let seeds = [0; 1; 2; 3; 4; 5]

let of_int ~byte_width x =
  if byte_width > 0 && Float.(of_int x >= 2.0 ** (of_int byte_width * 8.0)) then
    failwith "Integer too large." ;
  let buf = Bytes.make byte_width '\x00' in
  for i = 0 to byte_width - 1 do
    Bytes.set buf i ((x lsr (i * 8)) land 0xFF |> Caml.char_of_int)
  done ;
  Bytes.to_string buf

let check_result ctxt out =
  let ret, cmph_output = out in
  match ret with
  | Ok (kv_config, kv_packed) -> assert_equal ~ctxt kv_config kv_packed
  | Error (Cmph.Error (`Hash_new_failed msg)) ->
      logf ctxt `Warning "Cmph internal error: %s" msg
  | Error e ->
      let msg = cmph_output ^ "\n\n" ^ Exn.to_string e in
      assert_failure msg

let gc_test_1 =
  "gc-fixed"
  >:: fun _ ->
  let keys = List.map [0; 1; 2; 3] ~f:(of_int ~byte_width:4) in
  let keyset = KeySet.create keys in
  Caml.Gc.compact () ;
  let config = Config.create keyset in
  Caml.Gc.compact () ;
  Hash.of_config config |> ignore ;
  assert_bool "" true

let gc_test_2 =
  "gc-cstring"
  >:: fun _ ->
  let keys = ["0"; "1"; "2"; "3"; "4"; "5"; "6"] in
  let keyset = KeySet.create keys in
  Caml.Gc.compact () ;
  let config = Config.create keyset in
  Caml.Gc.compact () ;
  Hash.of_config config |> ignore ;
  assert_bool "" true

let packed_strings_test algo seed fn =
  let name = Printf.sprintf "%s:%d:%s" (Config.string_of_algo algo) seed fn in
  name
  >:: fun ctxt ->
  let ch = In_channel.create fn in
  let rec read lines =
    try
      let line = In_channel.input_line_exn ch in
      read (line :: lines)
    with End_of_file -> List.rev lines
  in
  let keys = read [] in
  with_output (fun () ->
      let keyset = KeySet.create keys in
      let config = Config.create ~seed:0 ~verbose:true ~algo keyset in
      let chash = Hash.of_config config in
      let kv_config = List.map ~f:(Hash.hash chash) keys in
      let phash = Hash.to_packed chash |> Hash.of_packed in
      let kv_packed = List.map ~f:(Hash.hash phash) keys in
      (kv_config, kv_packed))
  |> check_result ctxt

let packed_fw_test algo seed fn =
  let name = Printf.sprintf "%s:%d:%s" (Config.string_of_algo algo) seed fn in
  name
  >:: fun ctxt ->
  let ch = In_channel.create fn in
  let rec read keys =
    try
      let key = Caml.really_input_string ch 8 in
      read (key :: keys)
    with End_of_file -> List.rev keys
  in
  let keys = read [] in
  with_output (fun () ->
      let keyset = KeySet.create keys in
      let config = Config.create ~seed:0 ~verbose:true ~algo keyset in
      let chash = Hash.of_config config in
      let kv_config = List.map ~f:(Hash.hash chash) keys in
      let phash = Hash.to_packed chash |> Hash.of_packed in
      let kv_packed = List.map ~f:(Hash.hash phash) keys in
      (kv_config, kv_packed))
  |> check_result ctxt

let product_3 l1 l2 l3 =
  List.map
    ~f:(fun x1 ->
      List.map ~f:(fun x2 -> List.map ~f:(fun x3 -> (x1, x2, x3)) l3) l2
      |> List.concat)
    l1
  |> List.concat

let suite =
  "tests"
  >::: [ gc_test_1
       ; gc_test_2
       ; ( "packed-strings"
         >:::
         (* Disable Bmz8 algorithm because it only works for key sets with < 256
            keys. *)
         let algos = Config.[`Bmz; default_chd; default_chd_ph; `Bdz; `Bdz_ph] in
         product_3 algos seeds ["keys-long.txt"]
         |> List.map ~f:(fun (algo, seed, fn) -> packed_strings_test algo seed fn)
         )
       ; ( "packed-fixedwidth"
         >:::
         let algos =
           Config.[`Bmz; `Bmz8; default_chd; default_chd_ph; `Bdz; `Bdz_ph]
         in
         product_3 algos seeds ["keys-fw.buf"; "keys-fw-1.buf"]
         |> List.map ~f:(fun (algo, seed, fn) -> packed_fw_test algo seed fn) ) ]

let () = run_test_tt_main suite
