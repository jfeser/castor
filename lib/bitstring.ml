open Base
open Stdio
open Collections

module Format = Caml.Format

type piece = {
  str : string;
  len : int;
} [@@deriving sexp, compare]

(* type t = piece list [@@deriving sexp, compare] *)

type t =
  | Label of string * t
  | Piece of piece
  | PList of t list

(** Serialize an integer. Little endian. Width is the number of bits to use. *)
let of_int : ?null:bool -> width:int -> int -> t =
  fun ?(null=false) ~width x ->
    assert (0 <= width && width <= 64 && (not null || width <= 63));
    let nbytes = (width / 8) + 1 in
    let buf = Bytes.make nbytes '\x00' in
    for i = 0 to nbytes - 1 do
      Bytes.set buf i ((x lsr (i * 8)) land 0xFF |> Caml.char_of_int)
    done;
    Piece { str = Bytes.to_string buf; len = width }

let of_bytes : bytes -> t = fun x ->
  Piece { str = Bytes.to_string x; len = 8 * Bytes.length x }

let of_string : string -> t = fun str ->
  Piece { str; len = String.length str * 8 }

let label : string -> t -> t = fun l x -> Label (l, x)

let concat xs = PList xs
let empty = PList []
let append x y = PList [x; y]

let length : t -> int =
  let rec length acc = function
    | Label (_, x) -> (length [@tailcall]) acc x
    | Piece { len } -> acc + len
    | PList [] -> acc
    | PList (x::xs) -> (length [@tailcall]) (length acc x) (PList xs)
  in
  length 0

let byte_length : t -> int = fun x ->
  let l = length x in
  (l / 8) + (if l % 8 > 0 then 1 else 0)

let int_length : t -> int = fun x ->
  let l = length x in
  (l / 64) + (if l % 64 > 0 then 1 else 0)

let rec flatten : t -> piece list = function
  | Label (_, x) -> flatten x
  | Piece x -> [x]
  | PList xs -> List.concat_map xs ~f:flatten

let pp : Format.formatter -> t -> unit = fun fmt ->
  let open Format in
  let rec pp prefix offset =
    function
    | Label (lbl, x) ->
      let blen = byte_length x in
      fprintf fmt "%s+ %s [%db %dB (%d bytes)]\n" prefix lbl offset (offset / 8) blen;
      pp (prefix ^ "| ") offset x
    | Piece { str; len } -> offset + len
    | PList xs -> List.fold_left xs ~init:offset ~f:(pp prefix)
  in
  fun x -> pp "" 0 x |> ignore

module ByteWriter = struct
  type t = {
    output_byte : char -> unit;
    seek : int64 -> unit;
    pos : unit -> int64;
    flush : unit -> unit;
    peek : unit -> char;
    close : unit -> unit;
  }

  let of_file : string -> t = fun fn ->
    let out_ch = Out_channel.create ~binary:true fn in
    let in_ch = In_channel.create ~binary:true fn in

    let output_byte = Out_channel.output_char out_ch in
    let seek pos = Out_channel.seek out_ch pos; In_channel.seek in_ch pos in
    let pos () = Out_channel.pos out_ch in
    let flush () = Out_channel.flush out_ch in
    let peek () =
      In_channel.seek in_ch (pos ());
      Option.value_exn (In_channel.input_char in_ch)
    in
    let close () =
      In_channel.close in_ch;
      Out_channel.close out_ch
    in
    { output_byte; seek; pos; flush; peek; close }

  let of_buffer : Buffer.t -> t = fun out ->
    let buf = ref (Bytes.create 1) in
    let pos = ref 0 in
    let len = ref 0 in

    let output_byte c =
      let buf_len = Bytes.length !buf in

      assert (!pos <= buf_len);
      assert (!pos <= !len);

      if !pos < buf_len then
        Bytes.set !buf !pos c
      else begin
        buf := Bytes.extend !buf 0 buf_len;
        Bytes.set !buf !pos c;
      end;
      incr pos;
      if !pos > !len then len := !pos
    in

    let seek pos' = pos := Int.of_int64_exn pos' in
    let get_pos () = Int.to_int64 !pos in
    let flush () =
      Buffer.reset out;
      Buffer.add_bytes out (Bytes.sub !buf 0 !len)
    in
    let peek () = Bytes.get !buf !pos in
    let close () = () in

    { output_byte; seek; pos = get_pos; flush; peek; close }
end

module Writer = struct
  type bitstring = t

  module Pos = struct
    type t = (int * int64)

    let (-) = fun (i1, b1) (i2, b2) -> (i1 - i2, Int64.(b1 - b2))

    let to_bytes_exn (i, b) = if i = 0 then b else failwith "Nonzero bit offset."
  end

  type t = {
    mutable buf : int;
    mutable pos : int;
    writer : ByteWriter.t;
  }

  let with_file : string -> t = fun fn -> {
      writer = ByteWriter.of_file fn; buf = 0; pos = 0;
    }

  let with_buffer : Buffer.t -> t = fun buf -> {
      writer = ByteWriter.of_buffer buf; buf = 0; pos = 0;
    }

  let write_bit : t -> int -> unit = fun t x ->
    assert (t.pos < 8);
    assert (x = 0 || x = 1);
    t.buf <- t.buf lor (x lsl (8 - t.pos - 1));
    t.pos <- t.pos + 1;
    if t.pos = 8 then begin
      t.writer.output_byte (Char.of_int_exn t.buf);
      t.buf <- 0;
      t.pos <- 0;
    end
  let write_bit t x =
    Exn.reraise_uncaught "write_bit" (fun () -> write_bit t x)

  let write_char : t -> char -> unit = fun t x ->
    let x = Char.to_int x in
    for i = 7 downto 0 do
      write_bit t ((x lsr i) land 1)
    done
  let write_char t x =
    Exn.reraise_uncaught "write_char" (fun () -> write_char t x)

  let write_bytes : t -> bytes -> unit = fun t x ->
    for i = 0 to Bytes.length x - 1 do
      write_char t (Bytes.get x i)
    done
  let write_bytes t x =
    Exn.reraise_uncaught "write_bytes" (fun () -> write_bytes t x)

  let write_string : t -> string -> unit = fun t x ->
    for i = 0 to String.length x - 1 do
      write_char t (String.get x i)
    done
  let write_string t x =
    Exn.reraise_uncaught "write_string" (fun () -> write_string t x)

  let write : t -> bitstring -> unit = fun t x ->
    flatten x |> List.iter ~f:(fun { str; len } ->
        if len > 0 then begin
          if len >= 8 then begin
            write_string t (String.sub str 0 (len / 8))
          end;
          let c = str.[String.length str - 1] |> Char.to_int in
          for i = 0 to len % 8 - 1 do
            let b = ((c lsr (7 - i)) land 1) in
            write_bit t b
          done
        end)
  let write t x =
    Exn.reraise_uncaught "write" (fun () -> write t x)

  let flush : t -> unit = fun t ->
    if t.pos > 0 then t.writer.output_byte (Char.of_int_exn t.buf);
    t.writer.flush ()

  let pos : t -> Pos.t = fun t -> (t.pos, t.writer.pos ())
  let seek : t -> Pos.t -> unit = fun t (bit_pos, byte_pos) ->
    flush t;
    t.writer.seek byte_pos;
    t.buf <- Char.to_int (t.writer.peek ());
    t.pos <- bit_pos

  let close : t -> unit = fun t -> t.writer.close ()
end

let to_string : t -> string = fun s ->
  let b = Buffer.create (byte_length s) in
  let w = Writer.with_buffer b in
  Writer.write w s;
  Writer.flush w;
  Buffer.to_string b

let tests =
  let open OUnit2 in

  let printer b = Buffer.to_string b |> String.escaped in

  "bitstring" >::: [
    "length" >::: [
      "" >:: (fun ctxt -> assert_equal ~ctxt 0 (length empty));
      "" >:: (fun ctxt -> assert_equal ~ctxt 1 (length (Piece { len = 1; str = "\x80"})));
    ];
    "writer" >::: [
      (* "flush" >:: (fun ctxt ->
       *     let flushed = ref false in
       *     let w = Writer.create (fun c ->
       *         assert_equal ~ctxt ~printer:Char.escaped '\x80' c;
       *         flushed := true) in
       *     Writer.write_bit w 1;
       *     Writer.flush w;
       *     assert_equal ~ctxt true !flushed); *)
      "write_bit" >:: (fun ctxt ->
          let buf1 = Buffer.create 1 in
          Buffer.add_char buf1 '\x80';
          let buf2 = Buffer.create 1 in
          let w = Writer.with_buffer buf2 in
          Writer.write_bit w 1;
          Writer.flush w;
          assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2
        );
      "write_bit" >:: (fun ctxt ->
          let buf1 = Buffer.create 1 in
          Buffer.add_char buf1 '\x88';
          let buf2 = Buffer.create 1 in
          let w = Writer.with_buffer buf2 in
          Writer.write_bit w 1;
          Writer.write_bit w 0;
          Writer.write_bit w 0;
          Writer.write_bit w 0;
          Writer.write_bit w 1;
          Writer.write_bit w 0;
          Writer.write_bit w 0;
          Writer.write_bit w 0;
          Writer.flush w;
          assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2
        );
      "write_char" >:: (fun ctxt ->
          let buf1 = Buffer.create 1 in
          Buffer.add_string buf1 "\xbf\xc0";
          let buf2 = Buffer.create 1 in
          let w = Writer.with_buffer buf2 in
          Writer.write_bit w 1;
          Writer.write_bit w 0;
          Writer.write_char w '\xff';
          Writer.flush w;
          assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2
        );
      "write_1" >:: (fun ctxt ->
          let buf1 = Buffer.create 1 in
          Buffer.add_string buf1 "\x88";
          let buf2 = Buffer.create 1 in
          let w = Writer.with_buffer buf2 in
          let bs = PList [
            Piece { len = 8; str = "\x88" };
          ] in
          Writer.write w bs;
          Writer.flush w;
          assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2
        );
      "write_2" >:: (fun ctxt ->
          let buf1 = Buffer.create 1 in
          Buffer.add_string buf1 "\xfe";
          let buf2 = Buffer.create 1 in
          let w = Writer.with_buffer buf2 in
          let bs = PList [
            Piece { len = 7; str = "\xff" };
          ] in
          Writer.write w bs;
          Writer.flush w;
          assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2
        );
      "write_3" >:: (fun ctxt ->
          let buf1 = Buffer.create 1 in
          Buffer.add_string buf1 "\xbf\xc0";
          let buf2 = Buffer.create 1 in
          let w = Writer.with_buffer buf2 in
          let bs = PList [
            Piece { len = 1; str = "\x80" };
            Piece { len = 3; str = "\x60" };
            Piece { len = 5; str = "\xf8" };
            Piece { len = 7; str = "\x80" };
          ] in
          Writer.write w bs;
          Writer.flush w;
          assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2
        );
      "write_4" >:: (fun ctxt ->
          let buf1 = Buffer.create 1 in
          Buffer.add_string buf1 "\xde\xad";
          let buf2 = Buffer.create 1 in
          let w = Writer.with_buffer buf2 in
          let bs = PList [
            Piece { len = 8; str = "\xde" };
            Piece { len = 0; str = "\x00" };
            Piece { len = 8; str = "\xad" };
          ] in
          Writer.write w bs;
          Writer.flush w;
          assert_equal ~ctxt ~printer ~cmp:Buffer.equal buf1 buf2
        )

    ]
  ]
