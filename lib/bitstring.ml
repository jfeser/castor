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
let of_int : width:int -> int -> t = fun ~width x ->
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
    | Label (_, x) -> length acc x
    | Piece { len } -> acc + len
    | PList [] -> acc
    | PList (x::xs) -> length acc x + length acc (PList xs)
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
      fprintf fmt "%s+ %s [%x (%d bytes)]\n" prefix lbl offset blen;
      pp (prefix ^ "| ") offset x
    | Piece { str; len } -> offset + len
    | PList xs -> List.fold_left xs ~init:offset ~f:(pp prefix)
  in
  fun x -> pp "" 0 x |> ignore

module Writer = struct
  type bitstring = t
  type t = {
    mutable buf : int;
    mutable len : int;
    out : char -> unit;
  }

  let create : (char -> unit) -> t = fun out -> { out; buf = 0; len = 0 }

  let with_buffer : Buffer.t -> t = fun b -> create (Buffer.add_char b)

  let with_channel : Out_channel.t -> t = fun ch ->
    create (Out_channel.output_char ch)

  let write_bit : t -> int -> unit = fun t x ->
    assert (t.len < 8);
    assert (x = 0 || x = 1);
    t.buf <- t.buf lor (x lsl (8 - t.len - 1));
    t.len <- t.len + 1;
    if t.len = 8 then begin
      t.out (Char.of_int_exn t.buf);
      t.buf <- 0;
      t.len <- 0;
    end

  let write_char : t -> char -> unit = fun t x ->
    let x = Char.to_int x in
    for i = 7 downto 0 do
      write_bit t ((x lsr i) land 1)
    done

  let write_bytes : t -> bytes -> unit = fun t x ->
    for i = 0 to Bytes.length x - 1 do
      write_char t (Bytes.get x i)
    done

  let write_string : t -> string -> unit = fun t x ->
    for i = 0 to String.length x - 1 do
      write_char t (String.get x i)
    done


  let write : t -> bitstring -> unit = fun t x ->
    flatten x |> List.iter ~f:(fun { str; len } ->
        if len >= 8 then begin
          write_string t (String.sub str 0 (len / 8))
        end;
        let c = str.[String.length str - 1] |> Char.to_int in
        for i = 0 to len % 8 - 1 do
          let b = ((c lsr (7 - i)) land 1) in
          write_bit t b
        done)

  let flush : t -> unit = fun t ->
    if t.len > 0 then t.out (Char.of_int_exn t.buf)
end

let to_string : t -> string = fun s ->
  let b = Buffer.create (byte_length s) in
  let w = Writer.with_buffer b in
  Writer.write w s;
  Writer.flush w;
  Buffer.to_string b

let tests =
  let open OUnit2 in

  "bitstring" >::: [
    "length" >::: [
      "" >:: (fun ctxt -> assert_equal ~ctxt 0 (length empty));
      "" >:: (fun ctxt -> assert_equal ~ctxt 1 (length (Piece { len = 1; str = "\x80"})));
    ];
    "writer" >::: [
      "flush" >:: (fun ctxt ->
          let flushed = ref false in
          let w = Writer.create (fun c ->
              assert_equal ~ctxt ~printer:Char.escaped '\x80' c;
              flushed := true) in
          Writer.write_bit w 1;
          Writer.flush w;
          assert_equal ~ctxt true !flushed);
      "write_bit" >:: (fun ctxt ->
          let buf1 = Buffer.create 1 in
          Buffer.add_char buf1 '\x80';
          let buf2 = Buffer.create 1 in
          let w = Writer.with_buffer buf2 in
          Writer.write_bit w 1;
          Writer.flush w;
          assert_equal ~ctxt ~printer:Buffer.to_string ~cmp:Buffer.equal buf1 buf2
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
          assert_equal ~ctxt ~printer:Buffer.to_string ~cmp:Buffer.equal buf1 buf2
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
          assert_equal ~ctxt ~printer:Buffer.to_string ~cmp:Buffer.equal buf1 buf2
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
          assert_equal ~ctxt ~printer:Buffer.to_string ~cmp:Buffer.equal buf1 buf2
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
          assert_equal ~ctxt ~printer:Buffer.to_string ~cmp:Buffer.equal buf1 buf2
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
          assert_equal ~ctxt ~printer:Buffer.to_string ~cmp:Buffer.equal buf1 buf2
        )
    ]
  ]
