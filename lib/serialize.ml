open Base
open Stdio
open Printf
open Collections

let bsize = 8

(* boolean size *)
let isize = 8

(* integer size *)
let hsize = 2 * isize

(* block header size *)

(** Serialize an integer. Little endian. Width is the number of bits to use,
   must be a multiple of the byte size. *)
let bytes_of_int : width:int -> int -> bytes =
 fun ~width x ->
  ( if width % 8 <> 0 then
    Error.(create "Not a multiple of 8." width [%sexp_of : int] |> raise) ) ;
  let nbytes = width / 8 in
  let buf = Bytes.make nbytes '\x00' in
  for i = 0 to nbytes - 1 do
    Bytes.set buf i ((x lsr (i * 8)) land 0xFF |> Caml.char_of_int)
  done ;
  buf

let int_of_bytes_exn : bytes -> int =
 fun x ->
  if Bytes.length x > isize then failwith "Unexpected byte sequence" ;
  let r = ref 0 in
  for i = 0 to Bytes.length x - 1 do
    r := !r + ((Bytes.get x i |> Caml.int_of_char) lsl (i * 8))
  done ;
  !r

let bytes_of_bool : bool -> bytes = function
  | true -> bytes_of_int ~width:64 1
  | false -> bytes_of_int ~width:64 0

let bool_of_bytes_exn : bytes -> bool =
 fun b ->
  match int_of_bytes_exn b with
  | 0 -> false
  | 1 -> true
  | _ -> failwith "Unexpected byte sequence."

let align : ?pad:char -> int -> bytes -> bytes =
 fun ?(pad= '\x00') align b ->
  let slop = Bytes.length b % align in
  if slop = 0 then b
  else
    let padding = align - slop in
    Bytes.cat b (Bytes.make padding '\x00')

open Type

let serialize_null t _ _ =
  let open Bitstring in
  match t with
  | NullT _ -> empty
  | IntT {range= _, max; nullable= true} ->
      of_int ~width:64 (max + 1) |> label "Int (null)"
  | BoolT _ -> of_int ~width:64 2 |> label "Bool (null)"
  | StringT {nchars= min, _} ->
      let len_flag = of_int ~width:64 (min - 1) in
      concat [len_flag |> label "String len (null)"]
  | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

let serialize_int t _ x _ =
  let open Bitstring in
  match t with
  | IntT _ -> of_int ~width:64 x |> label "Int"
  | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

let serialize_bool t _ x _ =
  let open Bitstring in
  match (t, x) with
  | BoolT _, true -> of_int ~width:64 1 |> label "Bool"
  | BoolT _, false -> of_int ~width:64 0 |> label "Bool"
  | t, _ -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

let serialize_string t _ x _ =
  let open Bitstring in
  match t with
  | StringT {nchars} ->
      let unpadded_body = Bytes.of_string x in
      let body = unpadded_body |> align isize |> of_bytes in
      let len =
        match Type.AbsInt.concretize nchars with
        | Some len -> empty
        | None -> Bytes.length unpadded_body |> bytes_of_int ~width:64 |> of_bytes
      in
      concat [len |> label "String len"; body |> label "String body"]
  | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

let serialize_tuple serialize t layout ls =
  let open Bitstring in
  match t with
  | CrossTupleT (ts, {count}) | ZipTupleT (ts, {count}) ->
      let body =
        List.map2_exn ts ls ~f:(fun t l -> serialize t l) |> concat |> label "Tuple body"
      in
      let len = byte_length body in
      let len_str = of_int ~width:64 len |> label "Tuple len" in
      ( match Type.AbsCount.kind count with
      | `Count _ | `Unknown -> concat [len_str; body]
      | `Countable ->
          let ct = Layout.ntuples_exn layout in
          let ct_str = of_int ~width:64 ct |> label "Tuple count" in
          concat [ct_str; len_str; body] )
      |> label "Tuple"
  | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

let serialize_list serialize t layout ls =
  let open Bitstring in
  match t with
  | OrderedListT (t, {count}) | UnorderedListT (t, {count}) -> (
      let body = List.map ls ~f:(serialize t) |> concat |> label "List body" in
      let len = byte_length body in
      let len_str = of_int ~width:64 len |> label "List len" in
      match Type.AbsCount.kind count with
      | `Count _ | `Unknown -> concat [len_str; body]
      | `Countable ->
          let ct = Layout.ntuples_exn layout in
          let ct_str = of_int ~width:64 ct |> label "List count" in
          concat [ct_str; len_str; body] )
  | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

let serialize_table serialize t _ m _ =
  let open Bitstring in
  match t with
  | TableT (key_t, value_t, _) ->
      let keys =
        Map.keys m |> List.map ~f:(fun k -> (k, serialize key_t (Layout.of_value k)))
      in
      Logs.debug (fun m -> m "Generating hash for %d keys." (List.length keys)) ;
      let key_length =
        List.all_equal (List.map keys ~f:(fun (_, b) -> String.length (to_string b)))
      in
      let hash =
        let open Cmph in
        List.map keys ~f:(fun (_, b) -> to_string b)
        |> KeySet.create
        |> Config.create ~seed:0 ~algo:`Chd
        |> Hash.of_config
      in
      let keys =
        List.map keys ~f:(fun (k, b) ->
            let h = Cmph.Hash.hash hash (to_string b) in
            (k, b, h) )
      in
      Out_channel.with_file "hashes.txt" ~f:(fun ch ->
          List.iter keys ~f:(fun (k, v, h) ->
              Out_channel.fprintf ch "%s -> %d\n" (Bitstring.to_string v) h ) ) ;
      let hash_body = Cmph.Hash.to_packed hash |> Bytes.of_string |> align isize in
      let hash_body_b = of_bytes hash_body in
      let hash_len = byte_length hash_body_b in
      let table_size =
        List.fold_left keys ~f:(fun m (_, _, h) -> Int.max m h) ~init:0 |> fun m -> m + 1
      in
      let hash_table = Array.create ~len:table_size 0xDEADBEEF in
      let values = empty in
      let offset = isize * table_size in
      let offset, values =
        List.fold_left keys ~init:(offset, values) ~f:(fun (offset, values) (k, b, h) ->
            let v = Map.find_exn m k in
            let vb = serialize value_t v in
            hash_table.(h) <- offset ;
            let values =
              concat [values; b |> label "Table key"; vb |> label "Table value"]
            in
            let offset = offset + byte_length b + byte_length vb in
            (offset, values) )
      in
      let hash_table_b =
        Array.map hash_table ~f:(of_int ~width:64) |> Array.to_list |> concat
      in
      let body =
        concat
          [ of_int ~width:64 hash_len |> label "Cmph data len"
          ; hash_body_b |> label "Cmph data"
          ; hash_table_b |> label "Table mapping"
          ; values |> label "Table values" ]
      in
      concat [of_int ~width:64 (byte_length body) |> label "Table len"; body]
  | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

let serialize_grouping serialize t layout ls _ =
  let open Bitstring in
  match t with
  | GroupingT (kt, vt, {count; key; output}) -> (
      let body =
        List.map ls ~f:(fun (k, v) -> concat [serialize kt k; serialize vt v])
        |> concat |> label "Grouping body"
      in
      let len = byte_length body in
      let len_str = of_int ~width:64 len |> label "Grouping len" in
      match Type.AbsCount.kind count with
      | `Count _ | `Unknown -> concat [len_str; body]
      | `Countable ->
          let ct = Layout.ntuples_exn layout in
          let ct_str = of_int ~width:64 ct |> label "Grouping count" in
          concat [ct_str; len_str; body] )
  | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

let rec serialize : Type.t -> Layout.t -> Bitstring.t =
  let open Bitstring in
  fun type_ layout ->
    match layout.node with
    | Null s -> serialize_null type_ layout s
    | Int (x, s) -> serialize_int type_ layout x s
    | Bool (x, s) -> serialize_bool type_ layout x s
    | String (x, s) -> serialize_string type_ layout x s
    | CrossTuple ls | ZipTuple ls -> serialize_tuple serialize type_ layout ls
    | Table (ls, s) -> serialize_table serialize type_ layout ls s
    | Grouping (ls, s) -> serialize_grouping serialize type_ layout ls s
    | OrderedList (ls, _) | UnorderedList ls -> serialize_list serialize type_ layout ls
    | Empty -> empty

let tests =
  let open OUnit2 in
  "serialize"
  >::: [ ( "to-byte"
         >:: fun ctxt ->
         let x = 0xABCDEF01 in
         assert_equal ~ctxt x (bytes_of_int ~width:64 x |> int_of_bytes_exn) )
       ; ( "from-byte"
         >:: fun ctxt ->
         let b = Bytes.of_string "\031\012\000\000" in
         let x = 3103 in
         assert_equal ~ctxt ~printer:Caml.string_of_int x (int_of_bytes_exn b) )
       ; ( "align"
         >:: fun ctxt ->
         let b = Bytes.of_string "\001\002\003" in
         let b' = align 8 b in
         assert_equal ~ctxt ~printer:Caml.string_of_int 8 (Bytes.length b') ;
         assert_equal ~ctxt ~printer:Bytes.to_string
           (Bytes.of_string "\001\002\003\000\000\000\000\000")
           b' ) ]
