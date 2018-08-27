open Base
open Printf
open Stdio
open Collections

module Config = struct
  module type S = sig
    val layout_map : bool
  end
end

module No_config = struct
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
      Bytes.cat b (Bytes.make padding pad)

  open Type

  let serialize_null t _ _ =
    let open Bitstring in
    match t with
    | NullT _ -> empty
    | IntT {range= (_, max) as range; nullable= true; _} ->
        of_int ~width:(Type.AbsInt.bitwidth ~nullable:true range) (max + 1)
        |> label "Int (null)"
    | BoolT _ -> of_int ~width:8 2 |> label "Bool (null)"
    | StringT {nchars= min, _; _} ->
        let len_flag = of_int ~width:64 (min - 1) in
        concat [len_flag |> label "String len (null)"]
    | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

  let serialize_int t _ x _ =
    let open Bitstring in
    match t with
    | IntT {range; nullable; _} ->
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
    | StringT {nchars; _} ->
        let unpadded_body = Bytes.of_string x in
        let body = unpadded_body |> align isize |> of_bytes in
        let len =
          match Type.AbsInt.concretize nchars with
          | Some _ -> empty
          | None -> Bytes.length unpadded_body |> bytes_of_int ~width:64 |> of_bytes
        in
        concat [len |> label "String len"; body |> label "String body"]
    | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

  let serialize_grouping serialize t layout ls _ =
    let open Bitstring in
    match t with
    | GroupingT (kt, vt, {count; _}) -> (
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

  let serialize_scalar : Type.t -> Layout.t -> Bitstring.t =
    let open Bitstring in
    fun type_ layout ->
      match layout.node with
      | Null s -> serialize_null type_ layout s
      | Int (x, s) -> serialize_int type_ layout x s
      | Bool (x, s) -> serialize_bool type_ layout x s
      | String (x, s) -> serialize_string type_ layout x s
      | Empty -> empty
      | _ -> failwith "Unsupported"
end

module Make (Config : Config.S) (Eval : Eval.S) () = struct
  include No_config
  open Config

  module Abslayout = Abslayout_db.Make (Eval) ()

  open Abslayout

  class ['self] serialize_fold log_ch writer =
    let ctr = ref 0 in
    let labels = ref [] in
    let log_start lbl =
      let open Bitstring in
      if Config.layout_map then (
        labels := (lbl, !ctr, Writer.pos writer) :: !labels ;
        Int.incr ctr )
    in
    let log_end () =
      let open Bitstring in
      if Config.layout_map then
        match !labels with
        | (lbl, ctr, start_pos) :: ls ->
            labels := ls ;
            let prefix = String.make (List.length !labels) ' ' in
            let end_ = Writer.Pos.to_bits (Writer.pos writer) in
            let start = Writer.Pos.to_bits start_pos in
            let byte_start = Int64.(start / of_int 8) in
            let byte_len = Int64.((end_ - start) / of_int 8) in
            let aligned =
              if Int64.(start % of_int 8 = of_int 0) then "=" else "~"
            in
            let out =
              sprintf "%d %s+ %s [%Ldb %s%LdB (%Ld bytes)]\n" ctr prefix lbl start
                aligned byte_start byte_len
            in
            Out_channel.output_string log_ch out
        | [] -> Logs.warn (fun m -> m "Unexpected log_end.")
    in
    object (self: 'self)
      method visit_AList ctx type_ q elem_layout =
        let open Bitstring in
        let open Type in
        match type_ with
        | UnorderedListT (elem_t, _) ->
            (* Reserve space for list header. *)
            let header_pos = Writer.pos writer in
            log_start "List count" ;
            Writer.write_bytes writer (Bytes.make 8 '\x00') ;
            log_end () ;
            log_start "List len" ;
            Writer.write_bytes writer (Bytes.make 8 '\x00') ;
            log_end () ;
            (* Serialize list body. *)
            log_start "List body" ;
            let count = ref 0 in
            Eval.eval ctx q
            |> Seq.iter ~f:(fun t ->
                   Caml.incr count ;
                   self#visit_t (Map.merge_right ctx t) elem_t elem_layout ) ;
            let end_pos = Writer.pos writer in
            log_end () ;
            (* Serialize list header. *)
            let len =
              Writer.Pos.(end_pos - header_pos |> to_bytes_exn) |> Int64.to_int_exn
            in
            Writer.seek writer header_pos ;
            Writer.write writer (of_int ~width:64 !count) ;
            Writer.write writer (of_int ~width:64 len) ;
            Writer.seek writer end_pos
        | t ->
            Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)
      method visit_ATuple ctx type_ elem_layouts _ =
        let open Bitstring in
        let open Type in
        match type_ with
        | CrossTupleT (elem_ts, _) | ZipTupleT (elem_ts, _) ->
            (* Reserve space for header. *)
            let header_pos = Writer.pos writer in
            log_start "Tuple len" ;
            Writer.write_bytes writer (Bytes.make 8 '\x00') ;
            log_end () ;
            (* Serialize body *)
            log_start "Tuple body" ;
            List.iter2_exn ~f:(fun t l -> self#visit_t ctx t l) elem_ts elem_layouts ;
            log_end () ;
            let end_pos = Writer.pos writer in
            (* Serialize header. *)
            Writer.seek writer header_pos ;
            let len =
              Writer.Pos.(end_pos - header_pos |> to_bytes_exn) |> Int64.to_int_exn
            in
            Writer.write writer (of_int ~width:64 len) ;
            Writer.seek writer end_pos
        | t ->
            Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)
      method visit_AHashIdx ctx type_ q l _ =
        let open Bitstring in
        let open Type in
        match type_ with
        | TableT (key_t, value_t, _) ->
            let key_name = ref (Name.of_string_exn "fixme") in
            let keys =
              Eval.eval ctx q
              |> Seq.map ~f:(fun k ->
                     let name, key =
                       match Map.to_alist k with
                       | [(name, key)] -> (name, key)
                       | _ -> failwith "Unexpected key tuple shape."
                     in
                     key_name := name ;
                     key )
              |> Seq.to_list
            in
            Logs.debug (fun m -> m "Generating hash for %d keys." (List.length keys)) ;
            let keys =
              List.map keys ~f:(fun k ->
                  let serialized =
                    Db.Value.of_primvalue k |> Layout.of_value
                    |> serialize_scalar key_t
                  in
                  let hash_key =
                    match k with
                    | `String s | `Unknown s -> s
                    | _ -> Bitstring.to_string serialized
                  in
                  (k, serialized, hash_key) )
            in
            Out_channel.with_file "keys.txt" ~f:(fun ch ->
                List.iter keys ~f:(fun (_, _, x) -> Out_channel.fprintf ch "%s\n" x)
            ) ;
            let hash =
              let open Cmph in
              List.map keys ~f:(fun (_, _, x) -> x)
              |> KeySet.create
              |> Config.create ~verbose:true ~seed:0
              |> Hash.of_config
            in
            let keys =
              List.map keys ~f:(fun (k, b, x) -> (k, b, x, Cmph.Hash.hash hash x))
            in
            Out_channel.with_file "hashes.txt" ~f:(fun ch ->
                List.iter keys ~f:(fun (_, _, x, h) ->
                    Out_channel.fprintf ch "%s -> %d\n" x h ) ) ;
            let hash_body =
              Cmph.Hash.to_packed hash |> Bytes.of_string |> Serialize.(align isize)
            in
            let hash_len = Bytes.length hash_body in
            let table_size =
              List.fold_left keys ~f:(fun m (_, _, _, h) -> Int.max m h) ~init:0
              |> fun m -> m + 1
            in
            let header_pos = Writer.pos writer in
            log_start "Table len" ;
            Writer.write_bytes writer (Bytes.make 8 '\x00') ;
            log_end () ;
            log_start "Table hash len" ;
            Writer.write_bytes writer (Bytes.make 8 '\x00') ;
            log_end () ;
            log_start "Table hash" ;
            Writer.write_bytes writer (Bytes.make hash_len '\x00') ;
            log_end () ;
            log_start "Table key map" ;
            Writer.write_bytes writer (Bytes.make (8 * table_size) '\x00') ;
            log_end () ;
            let hash_table = Array.create ~len:table_size 0x0 in
            log_start "Table values" ;
            List.iter keys ~f:(fun (k, b, _, h) ->
                let ctx = Map.set ctx ~key:!key_name ~data:k in
                let value_pos = Writer.pos writer in
                Writer.write writer b ;
                self#visit_t ctx value_t l ;
                hash_table.(h)
                <- Writer.Pos.(value_pos |> to_bytes_exn |> Int64.to_int_exn) ) ;
            log_end () ;
            let end_pos = Writer.pos writer in
            Writer.seek writer header_pos ;
            let len = Writer.Pos.(end_pos - header_pos |> to_bytes_exn) in
            Writer.write writer (of_int ~width:64 (Int64.to_int_exn len)) ;
            Writer.write writer (of_int ~width:64 hash_len) ;
            Writer.write_bytes writer hash_body ;
            Array.iter hash_table ~f:(fun x ->
                Writer.write writer (of_int ~width:64 x) ) ;
            Writer.seek writer end_pos
        | t ->
            Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)
      method visit_AEmpty _ _ = ()
      method visit_AScalar ctx type_ e =
        let open Serialize in
        let l =
          Layout.of_value
            {value= eval_pred ctx e; rel= Db.Relation.dummy; field= Db.Field.dummy}
        in
        let label, bstr =
          match l.node with
          | Null s -> ("Null", serialize_null type_ l s)
          | Int (x, s) -> ("Int", serialize_int type_ l x s)
          | Bool (x, s) -> ("Bool", serialize_bool type_ l x s)
          | String (x, s) -> ("String", serialize_string type_ l x s)
          | _ -> failwith "Expected a scalar."
        in
        log_start label ;
        Bitstring.Writer.write writer bstr ;
        log_end ()
      method visit_AOrderedIdx ctx type_ key_l value_l meta =
        let open Bitstring in
        let temp_fn = Caml.Filename.temp_file "ordered-idx" "bin" in
        let temp_writer = Writer.with_file temp_fn in
        (* Need to order the key stream. Use the key stream to construct an
             ordering key. *)
        let key_schema = Meta.(find_exn key_l schema) in
        let order_key = List.map key_schema ~f:(fun n -> Name n) in
        let ordered_key_l = order_by order_key meta.order key_l in
        (* Write a dummy header. *)
        let header_pos = Writer.pos writer in
        log_start "Ordered idx len" ;
        Writer.write_bytes writer (Bytes.make 8 '\x00') ;
        log_end () ;
        log_start "Ordered idx index len" ;
        Writer.write_bytes writer (Bytes.make 8 '\x00') ;
        log_end () ;
        match type_ with
        | Type.OrderedIdxT (key_t, value_t, _) ->
            let keys = Eval.eval ctx ordered_key_l in
            Seq.iter keys ~f:(fun kctx ->
                (* Serialize key. *)
                self#visit_t kctx key_t key_l ;
                (* Serialize value ptr. *)
                Writer.write writer
                  ( Writer.pos temp_writer |> Writer.Pos.to_bytes_exn
                  |> Int64.to_int_exn |> of_int ~width:64 ) ;
                (* Serialize value. *)
                self#visit_t kctx value_t value_l )
        | _ -> failwith "Unexpected type."
      method visit_func ctx type_ rs =
        let open Type in
        match type_ with
        | FuncT (ts, _) -> List.iter2_exn ts rs ~f:(self#visit_t ctx)
        | _ ->
            Error.create "Expected a function type." type_ [%sexp_of : Type.t]
            |> Error.raise
      method visit_t ctx type_ ({node; _} as r) =
        let open Bitstring in
        let pos = Writer.pos writer |> Writer.Pos.to_bytes_exn in
        Meta.update r Meta.pos ~f:(function
          | Some (Pos pos' as p) -> if Int64.(pos = pos') then p else Many_pos
          | Some Many_pos -> Many_pos
          | None -> Pos pos ) ;
        match node with
        | AEmpty -> self#visit_AEmpty ctx type_
        | AScalar e -> self#visit_AScalar ctx type_ e
        | AList (r, a) -> self#visit_AList ctx type_ r a
        | ATuple (a, k) -> self#visit_ATuple ctx type_ a k
        | AHashIdx (r, a, t) -> self#visit_AHashIdx ctx type_ r a t
        | AOrderedIdx (r, a, t) -> self#visit_AOrderedIdx ctx type_ r a t
        | Select (_, r)
         |Filter (_, r)
         |Agg (_, _, r)
         |Dedup r
         |OrderBy {rel= r; _} ->
            self#visit_func ctx type_ [r]
        | As (_, r) -> self#visit_t ctx type_ r
        | Join {r1; r2; _} -> self#visit_func ctx type_ [r1; r2]
        | Scan _ -> Error.create "Cannot serialize." r [%sexp_of : t] |> Error.raise
    end

  let serialize ?(ctx= Map.empty (module Name)) writer t l =
    Logs.debug (fun m ->
        m "Serializing abstract layout: %s" (Sexp.to_string_hum ([%sexp_of : t] l))
    ) ;
    let open Bitstring in
    let begin_pos = Writer.pos writer in
    let fn = Caml.Filename.temp_file "buf" ".txt" in
    if Config.layout_map then
      Logs.info (fun m -> m "Outputting layout map to %s." fn) ;
    Out_channel.with_file fn ~f:(fun ch ->
        (new serialize_fold ch writer)#visit_t ctx t l ) ;
    let end_pos = Writer.pos writer in
    let len =
      Writer.Pos.(end_pos - begin_pos |> to_bytes_exn) |> Int64.to_int_exn
    in
    (l, len)
end

let tests =
  let open OUnit2 in
  let open No_config in
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
