open Base
open Stdio
open Printf
open Collections
open Abslayout

module Config = struct
  module type S = sig
    val layout_map_channel : Out_channel.t option
  end
end

module type S = Serialize_intf.S

module Make (Config : Config.S) (Eval : Eval.S) = struct
  type serialize_ctx =
    { writer: Bitstring.Writer.t
    ; log_ch: Out_channel.t
    ; serialize: serialize_ctx -> Ctx.t -> Type.t -> t -> unit }

  module Log = struct
    open Bitstring.Writer

    type insert =
      {pos: int64; parent_id: Core.Uuid.Unstable.t; id: Core.Uuid.Unstable.t}
    [@@deriving sexp]

    type entry = {msg: string; pos: int64; len: int64; id: Core.Uuid.Unstable.t}
    [@@deriving sexp]

    type msg = Entry of entry | Insert of insert [@@deriving sexp]

    let write_msg sctx msg =
      Out_channel.output_string sctx.log_ch
        ([%sexp_of: msg] msg |> Sexp.to_string_mach) ;
      Out_channel.newline sctx.log_ch ;
      Out_channel.flush sctx.log_ch

    let with_msg sctx msg f =
      let start = pos sctx.writer in
      let ret = f () in
      let len = Pos.(pos sctx.writer - start) in
      write_msg sctx
        (Entry {pos= Pos.to_bytes_exn start; len; id= id sctx.writer; msg}) ;
      ret

    let render fn out_ch =
      let msgs = Sexplib.Sexp.load_sexps_conv_exn fn [%of_sexp: msg] in
      let inserts =
        List.filter_map msgs ~f:(function Entry _ -> None | Insert m -> Some m)
        |> List.map ~f:(fun (m : insert) -> (m.id, m))
        |> Map.of_alist_exn (module Core.Uuid)
      in
      let rec offset id =
        match Map.find inserts id with
        | None -> 0L
        | Some m -> Int64.(offset m.parent_id + m.pos)
      in
      List.filter_map msgs ~f:(function
        | Entry m -> Some {m with pos= Int64.(m.pos + offset m.id)}
        | Insert _ -> None )
      |> List.sort ~compare:(fun m1 m2 ->
             [%compare: int64 * int64] (m1.pos, m1.len) (m2.pos, m2.len) )
      |> List.iter ~f:(fun m ->
             Out_channel.fprintf out_ch "%Ld:%Ld %s\n" m.pos m.len m.msg ) ;
      Out_channel.flush out_ch
  end

  open Bitstring
  open Writer
  open Header

  (* let contents_query = function
   *   | AList (q, _) | AHashIdx (q, _, _) | AOrderedIdx (q, _, _) -> Some q
   *   | Select _ | Filter _ | Join _ | Agg _ | OrderBy _ | Dedup _ | Scan _ | AEmpty
   *    |AScalar _ | ATuple _ | As _ ->
   *       None *)

  let serialize_list_ sctx ctxs t l =
    let elem_t, _ = t in
    let _, elem_layout = l in
    (* Reserve space for list header. *)
    let hdr = make_header (ListT t) in
    let header_pos = pos sctx.writer in
    write_bytes sctx.writer (Bytes.make (size_exn hdr "count") '\x00') ;
    write_bytes sctx.writer (Bytes.make (size_exn hdr "len") '\x00') ;
    (* Serialize list body. *)
    let count = ref 0 in
    Log.with_msg sctx "List body" (fun () ->
        Seq.iter ctxs ~f:(fun t ->
            Caml.incr count ;
            sctx.serialize sctx t elem_t elem_layout ) ) ;
    let end_pos = pos sctx.writer in
    (* Serialize list header. *)
    let len = Pos.(end_pos - header_pos) |> Int64.to_int_exn in
    seek sctx.writer header_pos ;
    Log.with_msg sctx (sprintf "List count (=%d)" !count) (fun () ->
        write_string sctx.writer (of_int ~byte_width:(size_exn hdr "count") !count)
    ) ;
    Log.with_msg sctx (sprintf "List len (=%d)" len) (fun () ->
        write_string sctx.writer (of_int ~byte_width:(size_exn hdr "len") len) ) ;
    seek sctx.writer end_pos

  let serialize_list sctx ctx t l =
    let elem_query, _ = l in
    serialize_list_ sctx
      (Eval.eval ctx elem_query |> Seq.map ~f:(Map.merge_right ctx))
      t l

  let serialize_tuple sctx ctx t l =
    let elem_ts, _ = t in
    let elem_layouts, _ = l in
    (* Reserve space for header. *)
    let hdr = make_header (TupleT t) in
    let header_pos = pos sctx.writer in
    write_bytes sctx.writer (Bytes.make (size_exn hdr "len") '\x00') ;
    (* Serialize body *)
    Log.with_msg sctx "Tuple body" (fun () ->
        List.iter2_exn
          ~f:(fun t l -> sctx.serialize sctx ctx t l)
          elem_ts elem_layouts ) ;
    let end_pos = pos sctx.writer in
    (* Serialize header. *)
    seek sctx.writer header_pos ;
    let len = Pos.(end_pos - header_pos) |> Int64.to_int_exn in
    Log.with_msg sctx (sprintf "Tuple len (=%d)" len) (fun () ->
        write_string sctx.writer (of_int ~byte_width:(size_exn hdr "len") len) ) ;
    seek sctx.writer end_pos

  type hash_key = {kctx: Ctx.t; hash_key: string; hash_val: int}

  let serialize_hashidx_ sctx ctxs t l =
    let key_t, value_t, _ = t in
    let _, value_l, meta = l in
    let key_l =
      Option.value_exn
        ~error:(Error.create "Missing key layout." meta [%sexp_of: hash_idx])
        meta.hi_key_layout
    in
    let keys = Seq.to_list ctxs in
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
            | [`String s] -> s
            | vs ->
                List.map vs ~f:(function
                  | `Int x -> Bitstring.of_int ~byte_width:8 x
                  | _ -> failwith "no non-int tuple keys" )
                |> String.concat ~sep:""
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
    let hash_body = Cmph.Hash.to_packed hash in
    let hash_len = String.length hash_body in
    let table_size =
      List.fold_left keys ~f:(fun m key -> Int.max m key.hash_val) ~init:0
      |> fun m -> m + 1
    in
    let hdr = make_header (HashIdxT t) in
    let header_pos = pos sctx.writer in
    Log.with_msg sctx "Table len" (fun () ->
        write_bytes sctx.writer (Bytes.make (size_exn hdr "len") '\x00') ) ;
    Log.with_msg sctx "Table hash len" (fun () ->
        write_bytes sctx.writer (Bytes.make (size_exn hdr "hash_len") '\x00') ) ;
    (* Log.with_msg sctx "Table hash (align)" (fun () ->
     *     pad_to_alignment sctx.writer 8 ) ; *)
    Log.with_msg sctx "Table hash" (fun () ->
        write_bytes sctx.writer (Bytes.make hash_len '\x00') ) ;
    Log.with_msg sctx "Table map len" (fun () ->
        write_bytes sctx.writer (Bytes.make (size_exn hdr "hash_map_len") '\x00') ) ;
    Log.with_msg sctx "Table key map" (fun () ->
        write_bytes sctx.writer (Bytes.make (8 * table_size) '\x00') ) ;
    let hash_table = Array.create ~len:table_size 0x0 in
    Log.with_msg sctx "Table values" (fun () ->
        List.iter keys ~f:(fun key ->
            let ctx = key.kctx in
            let value_pos = pos sctx.writer in
            sctx.serialize sctx ctx key_t key_l ;
            sctx.serialize sctx ctx value_t value_l ;
            hash_table.(key.hash_val)
            <- Pos.(value_pos |> to_bytes_exn |> Int64.to_int_exn) ) ) ;
    let end_pos = pos sctx.writer in
    seek sctx.writer header_pos ;
    let len = Pos.(end_pos - header_pos) in
    write_string sctx.writer
      (of_int ~byte_width:(size_exn hdr "len") (Int64.to_int_exn len)) ;
    write_string sctx.writer (of_int ~byte_width:(size_exn hdr "hash_len") hash_len) ;
    write_string sctx.writer hash_body ;
    write_string sctx.writer
      (of_int ~byte_width:(size_exn hdr "hash_map_len") (8 * table_size)) ;
    Array.iteri hash_table ~f:(fun i x ->
        Log.with_msg sctx (sprintf "Map entry (%d => %d)" i x) (fun () ->
            write_string sctx.writer (of_int ~byte_width:8 x) ) ) ;
    seek sctx.writer end_pos

  let serialize_hashidx sctx ctx t l =
    let query, _, _ = l in
    serialize_hashidx_ sctx
      (Eval.eval ctx query |> Seq.map ~f:(Map.merge_right ctx))
      t l

  let serialize_orderedidx_ sctx ctxs t l =
    let _, value_l, meta = l in
    let key_t, value_t, _ = t in
    let hdr = make_header (OrderedIdxT t) in
    let key_l =
      Option.value_exn
        ~error:(Error.create "Missing key layout." meta [%sexp_of: ordered_idx])
        meta.oi_key_layout
    in
    (* Write a dummy header. *)
    let len_pos = pos sctx.writer in
    write_bytes sctx.writer (Bytes.make (size_exn hdr "len") '\x00') ;
    write_bytes sctx.writer (Bytes.make (size_exn hdr "idx_len") '\x00') ;
    let index_start_pos = pos sctx.writer in
    (* Make a first pass to get the keys and value pointers set up. *)
    ctxs
    |> Seq.iter ~f:(fun kctx ->
           (* Serialize key. *)
           Log.with_msg sctx "Ordered idx key" (fun () ->
               sctx.serialize sctx kctx key_t key_l ) ;
           (* Save space for value pointer. *)
           write_bytes sctx.writer (Bytes.make 8 '\x00') ) ;
    let index_end_pos = pos sctx.writer in
    (* Pass over again to get values in the right places. *)
    let index_pos = ref index_start_pos in
    let value_pos = ref (pos sctx.writer) in
    Seq.iter ctxs ~f:(fun kctx ->
        seek sctx.writer !index_pos ;
        (* Serialize key. *)
        Log.with_msg sctx "Ordered idx key" (fun () ->
            sctx.serialize sctx kctx key_t key_l ) ;
        (* Serialize value ptr. *)
        let ptr = !value_pos |> Pos.to_bytes_exn |> Int64.to_int_exn in
        Log.with_msg sctx (sprintf "Ordered idx value ptr (=%d)" ptr) (fun () ->
            write_string sctx.writer (ptr |> of_int ~byte_width:8) ) ;
        index_pos := pos sctx.writer ;
        seek sctx.writer !value_pos ;
        (* Serialize value. *)
        sctx.serialize sctx kctx value_t value_l ;
        value_pos := pos sctx.writer ) ;
    let end_pos = pos sctx.writer in
    seek sctx.writer len_pos ;
    let len = Pos.(end_pos - len_pos) in
    let index_len = Pos.(index_end_pos - index_start_pos) in
    Log.with_msg sctx (sprintf "Ordered idx len (=%Ld)" len) (fun () ->
        write_string sctx.writer (of_int64 ~byte_width:(size_exn hdr "len") len) ) ;
    Log.with_msg sctx (sprintf "Ordered idx index len (=%Ld)" index_len) (fun () ->
        write_string sctx.writer
          (of_int64 ~byte_width:(size_exn hdr "idx_len") index_len) ) ;
    seek sctx.writer end_pos

  let serialize_orderedidx sctx ctx t l =
    (* Need to order the key stream. Use the key stream to construct an ordering
       key. *)
    let query, _, meta = l in
    let query_schema = Meta.(find_exn query schema) in
    let order_key = List.map query_schema ~f:(fun n -> Name n) in
    let ordered_query = order_by order_key meta.order query in
    let ctxs = Eval.eval ctx ordered_query |> Seq.map ~f:(Map.merge_right ctx) in
    serialize_orderedidx_ sctx ctxs t l

  let serialize_null sctx t =
    let hdr = make_header t in
    let str =
      match t with
      | NullT -> ""
      | IntT {range= _, max; nullable= true; _} ->
          of_int ~byte_width:(size_exn hdr "value") (max + 1)
      | BoolT _ -> of_int ~byte_width:(size_exn hdr "value") 2
      | StringT {nchars= min, _; _} ->
          of_int ~byte_width:(size_exn hdr "len") (min - 1)
      | t -> Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)
    in
    Log.with_msg sctx "Null" (fun () -> write_string sctx.writer str)

  let serialize_int sctx t x =
    let hdr = make_header t in
    match t with
    | IntT _ ->
        let sval = of_int ~byte_width:(size_exn hdr "value") x in
        write_string sctx.writer sval
    | t -> Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

  let serialize_bool sctx t x =
    let hdr = make_header t in
    let str =
      match (t, x) with
      | BoolT _, true -> of_int ~byte_width:(size_exn hdr "value") 1
      | BoolT _, false -> of_int ~byte_width:(size_exn hdr "value") 0
      | t, _ ->
          Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)
    in
    write_string sctx.writer str

  let serialize_string sctx t body =
    let hdr = make_header t in
    match t with
    | StringT _ ->
        let len = String.length body in
        Log.with_msg sctx (sprintf "String length (=%d)" len) (fun () ->
            of_int ~byte_width:(size_exn hdr "nchars") len
            |> write_string sctx.writer ) ;
        Log.with_msg sctx "String body" (fun () -> write_string sctx.writer body)
    | t -> Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

  let serialize_scalar sctx ctx type_ expr =
    let value = Eval.eval_pred ctx expr in
    Log.with_msg sctx
      (sprintf "Scalar (=%s)" ([%sexp_of: Db.primvalue] value |> Sexp.to_string_hum))
      (fun () ->
        match Eval.eval_pred ctx expr with
        | `Null -> serialize_null sctx type_
        | `Int x -> serialize_int sctx type_ x
        | `Bool x -> serialize_bool sctx type_ x
        | `String x -> serialize_string sctx type_ x )

  let rec serialize sctx ctx type_ layout =
    let open Type in
    (* Update position metadata in layout. *)
    let pos = pos sctx.writer |> Pos.to_bytes_exn in
    Meta.update layout Meta.pos ~f:(function
      | Some (Pos pos' as p) -> if Int64.(pos = pos') then p else Many_pos
      | Some Many_pos -> Many_pos
      | None -> Pos pos ) ;
    (* Serialize layout. *)
    match (type_, layout.node) with
    | _, AEmpty -> ()
    | t, AScalar e -> serialize_scalar sctx ctx t e
    | ListT t, AList l -> serialize_list sctx ctx t l
    | TupleT t, ATuple l -> serialize_tuple sctx ctx t l
    | HashIdxT t, AHashIdx l -> serialize_hashidx sctx ctx t l
    | OrderedIdxT t, AOrderedIdx l -> serialize_orderedidx sctx ctx t l
    | ( FuncT ([t], _)
      , ( Select (_, r)
        | Filter (_, r)
        | Agg (_, _, r)
        | Dedup r
        | OrderBy {rel= r; _} ) )
     |t, As (_, r) ->
        serialize sctx ctx t r
    | FuncT ([t1; t2], _), Join {r1; r2; _} ->
        serialize sctx ctx t1 r1 ; serialize sctx ctx t2 r2
    | t, r ->
        Error.create "Cannot serialize." (t, r) [%sexp_of: Type.t * node]
        |> Error.raise

  let serialize ?(ctx = Map.empty (module Name.Compare_no_type)) writer t l =
    Logs.debug (fun m ->
        m "Serializing abstract layout: %s" (Sexp.to_string_hum ([%sexp_of: t] l))
    ) ;
    let begin_pos = pos writer in
    let log_tmp_file =
      if Option.is_some Config.layout_map_channel then
        Core.Filename.temp_file "serialize" "log"
      else "/dev/null"
    in
    let log_ch = Out_channel.create log_tmp_file in
    serialize {writer; log_ch; serialize} ctx t l ;
    let end_pos = pos writer in
    let len = Pos.(end_pos - begin_pos) |> Int64.to_int_exn in
    flush writer ;
    ( match Config.layout_map_channel with
    | Some ch -> Log.render log_tmp_file ch
    | None -> () ) ;
    (l, len)
end
