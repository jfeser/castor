open Base
open Printf

open Collections

let bsize = 8 (* boolean size *)
let isize = 8 (* integer size *)
let hsize = 2 * isize (* block header size *)

(** Serialize an integer. Little endian. Width is the number of bits to use,
   must be a multiple of the byte size. *)
let bytes_of_int : width:int -> int -> bytes = fun ~width x ->
  if width % 8 <> 0 then
    Error.(create "Not a multiple of 8." width [%sexp_of:int] |> raise);

  let nbytes = width / 8 in
  let buf = Bytes.make nbytes '\x00' in
  for i = 0 to nbytes - 1 do
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
  | true -> bytes_of_int ~width:64 1
  | false -> bytes_of_int ~width:64 0

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

let rec serialize : Type.t -> Layout.t -> Bitstring.t =
  let open Bitstring in
  fun type_ layout ->
    match type_, layout with
    | IntT { bitwidth }, Scalar { value = `Int x } -> of_int ~width:64 x
    | BoolT _, Scalar { value = `Bool x } -> of_bytes (bytes_of_bool x)
    | StringT _,
      (Scalar { value = `String x } | Scalar { value = `Unknown x }) ->
      let unpadded_body = Bytes.of_string x in
      let body = unpadded_body |> align bsize in
      let len = Bytes.length body in
      Bytes.econcat [bytes_of_int ~width:64 len; body] |> of_bytes
    | CrossTupleT ts, (CrossTuple ls as l) ->
      let count = Layout.ntuples l in
      let body =
        List.map2_exn ts ls ~f:(fun (t, _) l -> serialize t l) |> concat
      in
      let len = byte_length body in
      concat [of_int ~width:64 count; of_int ~width:64 len; body]
    | ZipTupleT (ts, _), (ZipTuple ls as l) ->
      let count = Layout.ntuples l in
      let body = List.map2_exn ts ls ~f:serialize |> concat in
      let len = byte_length body in
      concat [of_int ~width:64 count; of_int ~width:64 len; body]
    | OrderedListT (t, _), (OrderedList (ls, _) as l)
    | UnorderedListT t, (UnorderedList ls as l) ->
      let count = Layout.ntuples l in
      let body = List.map ls ~f:(serialize t) |> concat in
      let len = byte_length body in
      concat [of_int ~width:64 count; of_int ~width:64 len; body]
    | TableT (elem_t, _, _), Table (m, _) ->
      (* let serialized_keys = Map.keys m |> List.map ~f;(fun )
       * let keyset = Map.keys m |> Cmph.KeySet.of_list in *)
      failwith "Unsupported"
    | _, Empty -> empty
    | t, l -> Error.(create "Unexpected layout type." (t, l)
                       [%sexp_of:Type.t * Layout.t] |> raise)

let tests =
  let open OUnit2 in

  "serialize" >::: [
    "to-byte" >:: (fun ctxt ->
        let x = 0xABCDEF01 in
        assert_equal ~ctxt x (bytes_of_int ~width:64 x |> int_of_bytes_exn));
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
