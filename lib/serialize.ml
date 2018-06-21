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
  | IntT {range= (_, max) as range; nullable= true} ->
      of_int ~width:(Type.AbsInt.bitwidth ~nullable:true range) (max + 1)
      |> label "Int (null)"
  | BoolT _ -> of_int ~width:8 2 |> label "Bool (null)"
  | StringT {nchars= min, _} ->
      let len_flag = of_int ~width:64 (min - 1) in
      concat [len_flag |> label "String len (null)"]
  | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

let serialize_int t _ x _ =
  let open Bitstring in
  match t with
  | IntT {range; nullable; field} ->
      of_int ~width:(Type.AbsInt.bitwidth ~nullable range) x |> label "Int"
  | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

let serialize_bool t _ x _ =
  let open Bitstring in
  match (t, x) with
  | BoolT _, true -> of_int ~width:8 1 |> label "Bool"
  | BoolT _, false -> of_int ~width:8 0 |> label "Bool"
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
    | Grouping (ls, s) -> serialize_grouping serialize type_ layout ls s
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
