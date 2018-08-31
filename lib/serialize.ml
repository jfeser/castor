open Base
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

  let serialize_null t =
    let open Bitstring in
    match t with
    | NullT -> empty
    | IntT {range= (_, max) as range; nullable= true; _} ->
        of_int ~width:(Type.AbsInt.bit_width ~nullable:true range) (max + 1)
        |> label "Int (null)"
    | BoolT _ -> of_int ~width:8 2 |> label "Bool (null)"
    | StringT {nchars= min, _; _} ->
        let len_flag = of_int ~width:64 (min - 1) in
        concat [len_flag |> label "String len (null)"]
    | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

  let serialize_int t x =
    let open Bitstring in
    match t with
    | IntT {range; nullable; _} ->
        of_int ~width:(Type.AbsInt.bit_width ~nullable range) x |> label "Int"
    | t -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

  let serialize_bool t x =
    let open Bitstring in
    match (t, x) with
    | BoolT _, true -> of_int ~width:8 1 |> label "Bool"
    | BoolT _, false -> of_int ~width:8 0 |> label "Bool"
    | t, _ -> Error.(create "Unexpected layout type." t [%sexp_of : Type.t] |> raise)

  let serialize_string t x =
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

  let serialize_value type_ = function
    | `Null -> serialize_null type_
    | `Int x -> serialize_int type_ x
    | `Bool x -> serialize_bool type_ x
    | `String x | `Unknown x -> serialize_string type_ x
end

include No_config

module Make (Config : Config.S) (Eval : Eval.S) = struct
  include No_config

  module Abslayout = struct
    include Abslayout
    module A = Abslayout_db.Make (Eval)
    include A
  end

  open Abslayout

  type serialize_ctx = {writer: Bitstring.Writer.t; ctx: Ctx.t}

  let log_start _ = ()

  let log_end () = ()

  let serialize_list serialize ({ctx; writer} as sctx) (elem_t, _)
      (elem_query, elem_layout) =
    let open Bitstring in
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
    Eval.eval ctx elem_query
    |> Seq.iter ~f:(fun t ->
           Caml.incr count ;
           serialize {sctx with ctx= Map.merge_right ctx t} elem_t elem_layout ) ;
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

  let serialize_tuple serialize ({writer; _} as sctx) (elem_ts, _) (elem_layouts, _)
      =
    let open Bitstring in
    (* Reserve space for header. *)
    let header_pos = Writer.pos writer in
    log_start "Tuple len" ;
    Writer.write_bytes writer (Bytes.make 8 '\x00') ;
    log_end () ;
    (* Serialize body *)
    log_start "Tuple body" ;
    List.iter2_exn ~f:(fun t l -> serialize sctx t l) elem_ts elem_layouts ;
    log_end () ;
    let end_pos = Writer.pos writer in
    (* Serialize header. *)
    Writer.seek writer header_pos ;
    let len =
      Writer.Pos.(end_pos - header_pos |> to_bytes_exn) |> Int64.to_int_exn
    in
    Writer.write writer (of_int ~width:64 len) ;
    Writer.seek writer end_pos

  type hash_key = {kctx: Ctx.t; hash_key: string; hash_val: int}

  let serialize_hashidx serialize ({ctx; writer} as sctx) (key_t, value_t, _)
      (query, value_l, meta) =
    let open Bitstring in
    let key_l =
      Option.value_exn
        ~error:(Error.create "Missing key layout." meta [%sexp_of : hash_idx])
        meta.hi_key_layout
    in
    let keys = Eval.eval ctx query |> Seq.to_list in
    Logs.debug (fun m -> m "Generating hash for %d keys." (List.length keys)) ;
    let keys =
      List.map keys ~f:(fun kctx ->
          let value =
            match key_l.node with
            | AScalar e -> [Eval.eval_pred kctx e]
            | ATuple (es, _) ->
                List.map es ~f:(function
                  | {node= AScalar e; _} -> Eval.eval_pred kctx e
                  | _ -> failwith "no nested key structures." )
            | _ -> failwith "no non-tuple key structures"
          in
          let hash_key =
            match value with
            | [`String s] | [`Unknown s] -> s
            | vs ->
                List.map vs ~f:(function
                  | `Int x -> Bitstring.of_int ~width:64 x
                  | _ -> failwith "no non-int tuple keys" )
                |> Bitstring.concat |> Bitstring.to_string
          in
          {kctx; hash_key; hash_val= -1} )
    in
    Out_channel.with_file "keys.txt" ~f:(fun ch ->
        List.iter keys ~f:(fun key -> Out_channel.fprintf ch "%s\n" key.hash_key) ) ;
    let hash =
      let open Cmph in
      List.map keys ~f:(fun key -> key.hash_key)
      |> KeySet.create
      |> Config.create ~verbose:true ~seed:0
      |> Hash.of_config
    in
    let keys =
      List.map keys ~f:(fun key ->
          {key with hash_val= Cmph.Hash.hash hash key.hash_key} )
    in
    Out_channel.with_file "hashes.txt" ~f:(fun ch ->
        List.iter keys ~f:(fun key ->
            Out_channel.fprintf ch "%s -> %d\n" key.hash_key key.hash_val ) ) ;
    let hash_body = Cmph.Hash.to_packed hash |> Bytes.of_string |> align isize in
    let hash_len = Bytes.length hash_body in
    let table_size =
      List.fold_left keys ~f:(fun m key -> Int.max m key.hash_val) ~init:0
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
    List.iter keys ~f:(fun key ->
        let ctx = Map.merge_right ctx key.kctx in
        let value_pos = Writer.pos writer in
        serialize {sctx with ctx} key_t key_l ;
        serialize {sctx with ctx} value_t value_l ;
        hash_table.(key.hash_val)
        <- Writer.Pos.(value_pos |> to_bytes_exn |> Int64.to_int_exn) ) ;
    log_end () ;
    let end_pos = Writer.pos writer in
    Writer.seek writer header_pos ;
    let len = Writer.Pos.(end_pos - header_pos |> to_bytes_exn) in
    Writer.write writer (of_int ~width:64 (Int64.to_int_exn len)) ;
    Writer.write writer (of_int ~width:64 hash_len) ;
    Writer.write_bytes writer hash_body ;
    Array.iter hash_table ~f:(fun x -> Writer.write writer (of_int ~width:64 x)) ;
    Writer.seek writer end_pos

  let serialize_orderedidx serialize ({ctx; writer} as sctx) (key_t, value_t, _)
      (query, value_l, meta) =
    let open Bitstring in
    let key_l =
      Option.value_exn
        ~error:(Error.create "Missing key layout." meta [%sexp_of : ordered_idx])
        meta.oi_key_layout
    in
    let temp_fn = Caml.Filename.temp_file "ordered-idx" "bin" in
    let temp_writer = Writer.with_file temp_fn in
    (* Need to order the key stream. Use the key stream to construct an
             ordering key. *)
    let query_schema = Meta.(find_exn query schema) in
    let order_key = List.map query_schema ~f:(fun n -> Name n) in
    let ordered_query = order_by order_key meta.order query in
    (* Write a dummy header. *)
    let len_pos = Writer.pos writer in
    log_start "Ordered idx len" ;
    Writer.write_bytes writer (Bytes.make 8 '\x00') ;
    log_end () ;
    let index_len_pos = Writer.pos writer in
    log_start "Ordered idx index len" ;
    Writer.write_bytes writer (Bytes.make 8 '\x00') ;
    log_end () ;
    Eval.eval ctx ordered_query
    |> Seq.iter ~f:(fun kctx ->
           let ksctx = {sctx with ctx= Map.merge_right ctx kctx} in
           (* Serialize key. *)
           serialize ksctx key_t key_l ;
           (* Serialize value ptr. *)
           Writer.write writer
             ( Writer.pos temp_writer |> Writer.Pos.to_bytes_exn |> Int64.to_int_exn
             |> of_int ~width:64 ) ;
           (* Serialize value. *)
           serialize {ksctx with writer= temp_writer} value_t value_l ) ;
    let index_end_pos = Writer.pos writer in
    Writer.write_file writer temp_fn ;
    let end_pos = Writer.pos writer in
    Writer.seek writer len_pos ;
    let len = Writer.Pos.(end_pos - len_pos |> to_bytes_exn) in
    let index_len = Writer.Pos.(index_end_pos - index_len_pos |> to_bytes_exn) in
    Writer.write writer (of_int64 ~width:64 len) ;
    Writer.write writer (of_int64 ~width:64 index_len) ;
    Writer.seek writer end_pos

  let serialize_scalar sctx type_ expr =
    log_start "Scalar" ;
    Eval.eval_pred sctx.ctx expr |> serialize_value type_
    |> Bitstring.Writer.write sctx.writer ;
    log_end ()

  let rec serialize ({writer; _} as sctx) type_ layout =
    let open Bitstring in
    let open Type in
    let pos = Writer.pos writer |> Writer.Pos.to_bytes_exn in
    Meta.update layout Meta.pos ~f:(function
      | Some (Pos pos' as p) -> if Int64.(pos = pos') then p else Many_pos
      | Some Many_pos -> Many_pos
      | None -> Pos pos ) ;
    match (type_, layout.node) with
    | _, AEmpty -> ()
    | t, AScalar e -> serialize_scalar sctx t e
    | ListT t, AList l -> serialize_list serialize sctx t l
    | TupleT t, ATuple l -> serialize_tuple serialize sctx t l
    | HashIdxT t, AHashIdx l -> serialize_hashidx serialize sctx t l
    | OrderedIdxT t, AOrderedIdx l -> serialize_orderedidx serialize sctx t l
    | ( FuncT ([t], _)
      , ( Select (_, r)
        | Filter (_, r)
        | Agg (_, _, r)
        | Dedup r
        | OrderBy {rel= r; _} ) )
     |t, As (_, r) ->
        serialize sctx t r
    | FuncT ([t1; t2], _), Join {r1; r2; _} ->
        serialize sctx t1 r1 ; serialize sctx t2 r2
    | t, r ->
        Error.create "Cannot serialize." (t, r) [%sexp_of : Type.t * node]
        |> Error.raise

  (* class ['self] serialize_fold log_ch writer =
   *   let ctr = ref 0 in
   *   let labels = ref [] in
   *   let log_start lbl =
   *     let open Bitstring in
   *     if Config.layout_map then (
   *       labels := (lbl, !ctr, Writer.pos writer) :: !labels ;
   *       Int.incr ctr )
   *   in
   *   let log_end () =
   *     let open Bitstring in
   *     if Config.layout_map then
   *       match !labels with
   *       | (lbl, ctr, start_pos) :: ls ->
   *           labels := ls ;
   *           let prefix = String.make (List.length !labels) ' ' in
   *           let end_ = Writer.Pos.to_bits (Writer.pos writer) in
   *           let start = Writer.Pos.to_bits start_pos in
   *           let byte_start = Int64.(start / of_int 8) in
   *           let byte_len = Int64.((end_ - start) / of_int 8) in
   *           let aligned =
   *             if Int64.(start % of_int 8 = of_int 0) then "=" else "~"
   *           in
   *           let out =
   *             sprintf "%d %s+ %s [%Ldb %s%LdB (%Ld bytes)]\n" ctr prefix lbl start
   *               aligned byte_start byte_len
   *           in
   *           Out_channel.output_string log_ch out
   *       | [] -> Logs.warn (fun m -> m "Unexpected log_end.")
   *   in
   *   object (self: 'self)
   *     method visit_AList ctx type_ q elem_layout =
   *     method visit_ATuple ctx type_ elem_layouts _ =
   *     method visit_AHashIdx ctx type_ q l _ =
   *     method visit_AEmpty _ _ = ()
   *     method visit_AScalar ctx type_ e =
   *     method visit_AOrderedIdx ctx type_ key_l value_l meta =
   *     method visit_func ctx type_ rs =
   *       let open Type in
   *       match type_ with
   *       | FuncT (ts, _) -> List.iter2_exn ts rs ~f:(self#visit_t ctx)
   *       | _ ->
   *           Error.create "Expected a function type." type_ [%sexp_of : Type.t]
   *           |> Error.raise
   *     method visit_t (writer, ctx) type_ ({node; _} as r) =
   *   end *)

  let serialize ?(ctx= Map.empty (module Name.Compare_no_type)) writer t l =
    Logs.debug (fun m ->
        m "Serializing abstract layout: %s" (Sexp.to_string_hum ([%sexp_of : t] l))
    ) ;
    let open Bitstring in
    let begin_pos = Writer.pos writer in
    serialize {writer; ctx} t l ;
    let end_pos = Writer.pos writer in
    let len =
      Writer.Pos.(end_pos - begin_pos |> to_bytes_exn) |> Int64.to_int_exn
    in
    Writer.flush writer ; (l, len)
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
