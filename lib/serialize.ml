open Base
open Printf

open Collections
open Locality

let bsize = 8 (* boolean size *)
let isize = 8 (* integer size *)
let hsize = 2 * isize (* block header size *)

let bytes_of_int : int -> bytes = fun x ->
  let buf = Bytes.make isize '\x00' in
  for i = 0 to isize - 1 do
    Bytes.set buf i ((x lsr (i * 8)) land 0xFF |> Caml.char_of_int)
  done;
  buf

let int_of_bytes_exn : bytes -> int = fun x ->
  if Bytes.length x > isize then failwith "Unexpected byte sequence";
  let r = ref 0 in
  for i = 0 to Bytes.length x - 1 do
    r := !r + (Bytes.get x i |> Caml.int_of_char) lsl (i * 8)
  done;
  !r

let bytes_of_bool : bool -> bytes = function
  | true -> bytes_of_int 1
  | false -> bytes_of_int 0

let bool_of_bytes_exn : bytes -> bool = fun b ->
  match int_of_bytes_exn b with
  | 0 -> false
  | 1 -> true
  | _ -> failwith "Unexpected byte sequence."

let align : int -> bytes -> bytes = fun align b ->
  let slop = Bytes.length b % align in
  if slop = 0 then b else
    let padding = align - slop in
    Bytes.cat b (Bytes.make padding '\x00')

let econcat = Bytes.concat Bytes.empty

let rec serialize : Locality.layout -> bytes = fun l ->
  let count, body = match l with
    | Scalar { rel; field; idx; value } ->
      let count = 1 in
      let body = match value with
        | `Bool true -> bytes_of_int 1
        | `Bool false -> bytes_of_int 0
        | `Int x -> bytes_of_int x
        | `Unknown x
        | `String x -> Bytes.of_string x |> align bsize
      in
      (count, body)
    | CrossTuple ls
    | ZipTuple ls
    | UnorderedList ls
    | OrderedList { elems = ls} ->
      let count = ntuples l in
      let body = List.map ls ~f:serialize |> econcat in
      (count, body)
    | Table _ -> failwith "Unsupported"
    | Empty -> (0, Bytes.empty)
  in
  let len = Bytes.length body in
  econcat [bytes_of_int count; bytes_of_int len; body]

let tests =
  let open OUnit2 in

  "serialize" >::: [
    "to-byte" >:: (fun ctxt ->
        let x = 0xABCDEF01 in
        assert_equal ~ctxt x (bytes_of_int x |> int_of_bytes_exn));
    "from-byte" >:: (fun ctxt ->
        let b = Bytes.of_string "\031\012\000\000" in
        let x = 3103 in
        assert_equal ~ctxt ~printer:Caml.string_of_int x (int_of_bytes_exn b));
    "align" >:: (fun ctxt ->
        let b = Bytes.of_string "\001\002\003" in
        let b' = align 8 b in
        assert_equal ~ctxt ~printer:Caml.string_of_int 8 (Bytes.length b');
        assert_equal ~ctxt ~printer:Bytes.to_string (Bytes.of_string "\001\002\003\000\000\000\000\000") b');
  ]
