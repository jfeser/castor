open! Core
open Collections

type piece = {str: string; len: int} [@@deriving sexp, compare]

(* type t = piece list [@@deriving sexp, compare] *)

type t = Label of string * t | Piece of piece | PList of t list

(** Serialize an integer. Little endian. Width is the number of bits to use. *)
let of_int ~byte_width x =
  if byte_width > 0 && Float.(of_int x >= 2.0 ** (of_int byte_width * 8.0)) then
    Error.create "Integer too large." (x, byte_width) [%sexp_of: int * int]
    |> Error.raise ;
  let buf = Bytes.make byte_width '\x00' in
  for i = 0 to byte_width - 1 do
    Bytes.set buf i ((x lsr (i * 8)) land 0xFF |> char_of_int)
  done ;
  Bytes.to_string buf

let of_int64 ~byte_width x =
  let buf = Bytes.make byte_width '\x00' in
  for i = 0 to byte_width - 1 do
    let shift = i * 8 in
    let mask = Int64.of_int 0xFF in
    Bytes.set buf i
      (Int64.((x lsr shift) land mask) |> Int64.to_int_exn |> Char.of_int_exn)
  done ;
  Bytes.to_string buf

let of_bytes : bytes -> t =
 fun x -> Piece {str= Bytes.to_string x; len= 8 * Bytes.length x}

let of_string : string -> t = fun str -> Piece {str; len= String.length str * 8}

let label : string -> t -> t = fun l x -> Label (l, x)

let concat xs = PList xs

let empty = PList []

let length : t -> int =
  let rec length acc = function
    | Label (_, x) -> (length [@tailcall]) acc x
    | Piece {len; _} -> acc + len
    | PList [] -> acc
    | PList (x :: xs) -> (length [@tailcall]) (length acc x) (PList xs)
  in
  length 0

let byte_length : t -> int =
 fun x ->
  let l = length x in
  (l / 8) + if l % 8 > 0 then 1 else 0

let int_length : t -> int =
 fun x ->
  let l = length x in
  (l / 64) + if l % 64 > 0 then 1 else 0

let rec flatten : t -> piece list = function
  | Label (_, x) -> flatten x
  | Piece x -> [x]
  | PList xs -> List.concat_map xs ~f:flatten

let pp : Format.formatter -> t -> unit =
 fun fmt ->
  let open Format in
  let rec pp prefix offset = function
    | Label (lbl, x) ->
        let blen = byte_length x in
        fprintf fmt "%s+ %s [%db %dB (%d bytes)]\n" prefix lbl offset (offset / 8)
          blen ;
        pp (prefix ^ "| ") offset x
    | Piece {len; _} -> offset + len
    | PList xs -> List.fold_left xs ~init:offset ~f:(pp prefix)
  in
  fun x -> pp "" 0 x |> ignore

module ByteWriter = struct
  type t =
    { output_byte: char -> unit
    ; output_bytes: bytes -> unit
    ; seek: int64 -> unit
    ; pos: unit -> int64
    ; flush: unit -> unit
    ; peek: unit -> char
    ; close: unit -> unit }

  let of_file : string -> t =
   fun fn ->
    let out_ch = Out_channel.create ~binary:true fn in
    let in_ch = In_channel.create ~binary:true fn in
    let output_byte = Out_channel.output_char out_ch in
    let output_bytes = Out_channel.output_bytes out_ch in
    let seek pos = Out_channel.seek out_ch pos ; In_channel.seek in_ch pos in
    let pos () = Out_channel.pos out_ch in
    let flush () = Out_channel.flush out_ch in
    let peek () =
      In_channel.seek in_ch (pos ()) ;
      match In_channel.input_char in_ch with Some c -> c | None -> '\x00'
    in
    let close () = In_channel.close in_ch ; Out_channel.close out_ch in
    {output_byte; output_bytes; seek; pos; flush; peek; close}

  let of_buffer : Buffer.t -> t =
   fun out ->
    let buf = ref (Bytes.create 1) in
    let pos = ref 0 in
    let len = ref 0 in
    let output_byte c =
      let buf_len = Bytes.length !buf in
      assert (!pos <= buf_len) ;
      assert (!pos <= !len) ;
      if !pos < buf_len then Bytes.set !buf !pos c
      else (
        buf := Bytes.extend !buf 0 buf_len ;
        Bytes.set !buf !pos c ) ;
      Int.incr pos ;
      if !pos > !len then len := !pos
    in
    let output_bytes b =
      for i = 0 to Bytes.length b - 1 do
        output_byte (Bytes.get b i)
      done
    in
    let seek pos' =
      let pos' = Int.of_int64_exn pos' in
      let buf_len = Bytes.length !buf in
      if pos' >= buf_len then buf := Bytes.extend !buf 0 buf_len ;
      pos := pos'
    in
    let get_pos () = Int.to_int64 !pos in
    let flush () =
      Buffer.reset out ;
      Buffer.add_bytes out (Bytes.sub !buf 0 !len)
    in
    let peek () = Bytes.get !buf !pos in
    let close () = () in
    {output_byte; output_bytes; seek; pos= get_pos; flush; peek; close}
end

module Writer = struct
  type bitstring = t

  module Id = Unique_id.Int ()

  module Pos = struct
    type t = {bit_pos: int; byte_pos: int64} [@@deriving sexp]

    let create bit_pos byte_pos =
      assert (bit_pos >= 0 && bit_pos < 8 && Int64.(byte_pos >= 0L)) ;
      {bit_pos; byte_pos}

    let ( - ) p1 p2 =
      assert (p1.bit_pos = 0 && p2.bit_pos = 0) ;
      Int64.(p1.byte_pos - p2.byte_pos)

    let to_bytes_exn {byte_pos; bit_pos} =
      if bit_pos = 0 then byte_pos else failwith "Nonzero bit offset."
  end

  type t = {mutable buf: int; mutable pos: int; writer: ByteWriter.t; id: Id.t}

  let with_bytewriter w = {writer= w; buf= 0; pos= 0; id= Id.create ()}

  let with_file fn = with_bytewriter (ByteWriter.of_file fn)

  let with_buffer buf = with_bytewriter (ByteWriter.of_buffer buf)

  let write_bit : t -> int -> unit =
   fun t x ->
    assert (t.pos < 8) ;
    assert (x = 0 || x = 1) ;
    t.buf <- t.buf lor (x lsl (8 - t.pos - 1)) ;
    t.pos <- t.pos + 1 ;
    if t.pos = 8 then (
      t.writer.output_byte (Char.of_int_exn t.buf) ;
      t.buf <- 0 ;
      t.pos <- 0 )

  let write_bit t x = Exn.reraise_uncaught "write_bit" (fun () -> write_bit t x)

  let write_char : t -> char -> unit =
   fun t x ->
    if t.pos = 0 then t.writer.output_byte x
    else
      let x = Char.to_int x in
      for i = 7 downto 0 do
        write_bit t ((x lsr i) land 1)
      done

  let write_char t x = Exn.reraise_uncaught "write_char" (fun () -> write_char t x)

  let write_bytes : t -> bytes -> unit =
   fun t x ->
    if t.pos = 0 then t.writer.output_bytes x
    else
      for i = 0 to Bytes.length x - 1 do
        write_char t (Bytes.get x i)
      done

  let write_bytes t x =
    Exn.reraise_uncaught "write_bytes" (fun () -> write_bytes t x)

  let write_string : t -> string -> unit =
   fun t x ->
    if t.pos = 0 then t.writer.output_bytes (Bytes.unsafe_of_string x)
    else
      for i = 0 to String.length x - 1 do
        write_char t x.[i]
      done

  let write_string t x =
    Exn.reraise_uncaught "write_string" (fun () -> write_string t x)

  let write : t -> bitstring -> unit =
   fun t x ->
    flatten x
    |> List.iter ~f:(fun {str; len} ->
           if len > 0 then (
             if len >= 8 then write_string t (String.sub str ~pos:0 ~len:(len / 8)) ;
             let c = str.[String.length str - 1] |> Char.to_int in
             for i = 0 to (len % 8) - 1 do
               let b = (c lsr (7 - i)) land 1 in
               write_bit t b
             done ) )

  let write t x = Exn.reraise_uncaught "write" (fun () -> write t x)

  let flush : t -> unit =
   fun t ->
    if t.pos > 0 then t.writer.output_byte (Char.of_int_exn t.buf) ;
    t.writer.flush ()

  let pos : t -> Pos.t = fun t -> Pos.create t.pos (t.writer.pos ())

  let id t = t.id

  let seek t Pos.{bit_pos; byte_pos} =
    flush t ;
    t.writer.seek byte_pos ;
    t.buf <- Char.to_int (t.writer.peek ()) ;
    t.pos <- bit_pos

  let close : t -> unit = fun t -> t.writer.close ()

  let write_file ?(buf_len = 1024) t fn =
    let buf = Bytes.create buf_len in
    In_channel.with_file fn ~f:(fun ch ->
        let rec loop () =
          let len = In_channel.input ch ~buf ~pos:0 ~len:buf_len in
          if len > 0 then (
            write_bytes t (Bytes.sub buf 0 len) ;
            loop () )
          else ()
        in
        loop () )

  let pad_to_alignment t align =
    let unaligned_pos = pos t |> Pos.to_bytes_exn |> Int64.to_int_exn in
    let aligned_pos = Int.round_up unaligned_pos ~to_multiple_of:align in
    let num_pad_bytes = aligned_pos - unaligned_pos in
    let pad_bytes = Bytes.make num_pad_bytes '\x00' in
    write_bytes t pad_bytes
end

let to_string : t -> string =
 fun s ->
  let b = Buffer.create (byte_length s) in
  let w = Writer.with_buffer b in
  Writer.write w s ; Writer.flush w ; Buffer.to_string b

open Writer

let tests =
  let open OUnit2 in
  let printer b = Buffer.to_string b |> String.escaped in
  "bitstring"
  >::: [ "length"
         >::: [ ("" >:: fun ctxt -> assert_equal ~ctxt 0 (length empty))
              ; ( ""
                >:: fun ctxt ->
                assert_equal ~ctxt 1 (length (Piece {len= 1; str= "\x80"})) ) ]
       ; "writer"
         >::: [ ( "write_bit"
                >:: fun ctxt ->
                let buf1 = Buffer.create 1 in
                Buffer.add_char buf1 '\x80' ;
                let buf2 = Buffer.create 1 in
                let w = with_buffer buf2 in
                write_bit w 1 ;
                flush w ;
                assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2 )
              ; ( "write_bit"
                >:: fun ctxt ->
                let buf1 = Buffer.create 1 in
                Buffer.add_char buf1 '\x88' ;
                let buf2 = Buffer.create 1 in
                let w = with_buffer buf2 in
                write_bit w 1 ;
                write_bit w 0 ;
                write_bit w 0 ;
                write_bit w 0 ;
                write_bit w 1 ;
                write_bit w 0 ;
                write_bit w 0 ;
                write_bit w 0 ;
                flush w ;
                assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2 )
              ; ( "write_char"
                >:: fun ctxt ->
                let buf1 = Buffer.create 1 in
                Buffer.add_string buf1 "\xbf\xc0" ;
                let buf2 = Buffer.create 1 in
                let w = with_buffer buf2 in
                write_bit w 1 ;
                write_bit w 0 ;
                write_char w '\xff' ;
                flush w ;
                assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2 )
              ; ( "write_1"
                >:: fun ctxt ->
                let buf1 = Buffer.create 1 in
                Buffer.add_string buf1 "\x88" ;
                let buf2 = Buffer.create 1 in
                let w = with_buffer buf2 in
                let bs = PList [Piece {len= 8; str= "\x88"}] in
                write w bs ;
                flush w ;
                assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2 )
              ; ( "write_2"
                >:: fun ctxt ->
                let buf1 = Buffer.create 1 in
                Buffer.add_string buf1 "\xfe" ;
                let buf2 = Buffer.create 1 in
                let w = with_buffer buf2 in
                let bs = PList [Piece {len= 7; str= "\xff"}] in
                write w bs ;
                flush w ;
                assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2 )
              ; ( "write_3"
                >:: fun ctxt ->
                let buf1 = Buffer.create 1 in
                Buffer.add_string buf1 "\xbf\xc0" ;
                let buf2 = Buffer.create 1 in
                let w = with_buffer buf2 in
                let bs =
                  PList
                    [ Piece {len= 1; str= "\x80"}
                    ; Piece {len= 3; str= "\x60"}
                    ; Piece {len= 5; str= "\xf8"}
                    ; Piece {len= 7; str= "\x80"} ]
                in
                write w bs ;
                flush w ;
                assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2 )
              ; ( "write_4"
                >:: fun ctxt ->
                let buf1 = Buffer.create 1 in
                Buffer.add_string buf1 "\xde\xad" ;
                let buf2 = Buffer.create 1 in
                let w = with_buffer buf2 in
                let bs =
                  PList
                    [ Piece {len= 8; str= "\xde"}
                    ; Piece {len= 0; str= "\x00"}
                    ; Piece {len= 8; str= "\xad"} ]
                in
                write w bs ;
                flush w ;
                assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2 ) ] ]

let%expect_test "seek1" =
  let fn = Filename.temp_file "test" "txt" in
  let writer = with_file fn in
  let pos = pos writer in
  write_string writer "testing" ;
  seek writer pos ;
  write_string writer "fish" ;
  flush writer ;
  In_channel.with_file fn ~f:In_channel.input_all |> print_endline ;
  [%expect {| fishing |}]

let%expect_test "seek2" =
  let fn = Filename.temp_file "test" "txt" in
  let writer = with_file fn in
  write_string writer "t" ;
  let pos = pos writer in
  write_string writer "esting" ;
  flush writer ;
  In_channel.with_file fn ~f:In_channel.input_all |> print_endline ;
  seek writer pos ;
  write_string writer "a" ;
  flush writer ;
  In_channel.with_file fn ~f:In_channel.input_all |> print_endline ;
  write_string writer "r" ;
  flush writer ;
  In_channel.with_file fn ~f:In_channel.input_all |> print_endline ;
  [%expect {|
    testing
    tasting
    tarting |}]

let%expect_test "write_file" =
  let fn1 = Filename.temp_file "test1" "txt" in
  let fn2 = Filename.temp_file "test2" "txt" in
  let w1 = with_file fn1 in
  let w2 = with_file fn2 in
  write_string w1 "testing" ;
  [%sexp_of: Pos.t] (pos w1) |> print_s ;
  write_string w2 "more testing" ;
  [%sexp_of: Pos.t] (pos w2) |> print_s ;
  flush w2 ;
  write_file w1 fn2 ;
  [%sexp_of: Pos.t] (pos w1) |> print_s ;
  [%expect
    {|
    ((bit_pos 0) (byte_pos 7))
    ((bit_pos 0) (byte_pos 12))
    ((bit_pos 0) (byte_pos 19)) |}]
