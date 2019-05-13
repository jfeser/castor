open! Core
open Collections
open Abslayout

module Config = struct
  module type S = sig
    val layout_map_channel : Out_channel.t option
  end
end

module type S = Serialize_intf.S

module Make (Config : Config.S) (M : Abslayout_db.S) = struct
  type serialize_ctx =
    {writer: Bitstring.Writer.t sexp_opaque; log_ch: Out_channel.t sexp_opaque}
  [@@deriving sexp]

  module Log = struct
    open Bitstring.Writer

    type msg = {msg: string; pos: int64; len: int64} [@@deriving sexp]

    type t = msg Bag.t

    let create = Bag.create

    let with_msg log writer msg f =
      let start = pos writer in
      let ret = f () in
      let len = Pos.(pos writer - start) in
      Bag.add log {pos= Pos.to_bytes_exn start; len; msg} |> ignore ;
      ret

    let render log out_ch =
      Bag.to_list log
      |> List.sort ~compare:(fun m1 m2 ->
             [%compare: int64 * int64]
               (m1.pos, Int64.(-m1.len))
               (m2.pos, Int64.(-m2.len)) )
      |> List.iter ~f:(fun m ->
             Out_channel.fprintf out_ch "%Ld:%Ld %s\n" m.pos m.len m.msg ) ;
      Out_channel.flush out_ch
  end

  open Bitstring
  open Writer
  open Header

  let string_sentinal : Type.string_ -> _ = function
    | {nchars= Bottom; _} -> 0
    | {nchars= Interval (_, max); _} -> max + 1
    | {nchars= Top; _} -> failwith "No available sentinal values."

  let int_sentinal : Type.int_ -> _ = function
    | {range= Bottom; _} -> 0
    | {range= Interval (_, max); _} -> max + 1
    | {range= Top; _} -> failwith "No available sentinal values."

  let date_sentinal : Type.date -> _ = function
    | {range= Bottom; _} -> 0
    | {range= Interval (_, max); _} -> max + 1
    | {range= Top; _} -> failwith "No available sentinal values."

  let fixed_sentinal : Type.fixed -> _ = function
    | {value= {range= Bottom; _}; _} -> 0
    | {value= {range= Interval (_, max); _}; _} -> max + 1
    | {value= {range= Top; _}; _} -> failwith "No available sentinal values."

  let bool_sentinal = 2

  let null_sentinal = function
    | Type.IntT x -> int_sentinal x
    | BoolT _ -> bool_sentinal
    | StringT x -> string_sentinal x
    | DateT x -> date_sentinal x
    | FixedT x -> fixed_sentinal x
    | t -> Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

  class serialize_fold writer =
    object (self : 'self)
      inherit [_, _] M.unsafe_material_fold as super

      val log_msgs = Log.create ()

      method pos = Writer.pos writer

      method build_AEmpty () _ = ()

      method build_AList sctx meta (_, elem_layout) gen =
        let t = Meta.Direct.(find_exn meta Meta.type_) in
        (* Reserve space for list header. *)
        let hdr = make_header t in
        let header_pos = pos writer in
        write_bytes writer (Bytes.make (size_exn hdr "count") '\x00') ;
        write_bytes writer (Bytes.make (size_exn hdr "len") '\x00') ;
        (* Serialize list body. *)
        let count = ref 0 in
        self#log "List body" (fun () ->
            Gen.iter gen ~f:(fun (_, vctx) ->
                count := !count + 1 ;
                self#visit_t sctx vctx elem_layout ) ) ;
        let end_pos = pos writer in
        (* Serialize list header. *)
        let len = Pos.(end_pos - header_pos) |> Int64.to_int_exn in
        seek writer header_pos ;
        self#log (sprintf "List count (=%d)" !count) (fun () ->
            write_string writer (of_int ~byte_width:(size_exn hdr "count") !count)
        ) ;
        self#log (sprintf "List len (=%d)" len) (fun () ->
            write_string writer (of_int ~byte_width:(size_exn hdr "len") len) ) ;
        seek writer end_pos

      method build_Tuple sctx t ls ctxs =
        (* Reserve space for header. *)
        let hdr = make_header t in
        let header_pos = pos writer in
        write_bytes writer (Bytes.make (size_exn hdr "len") '\x00') ;
        (* Serialize body *)
        self#log "Tuple body" (fun () ->
            List.iter2_exn ctxs ls ~f:(self#visit_t sctx) ) ;
        let end_pos = pos writer in
        (* Serialize header. *)
        seek writer header_pos ;
        let len = Pos.(end_pos - header_pos) |> Int64.to_int_exn in
        self#log (sprintf "Tuple len (=%d)" len) (fun () ->
            write_string writer (of_int ~byte_width:(size_exn hdr "len") len) ) ;
        seek writer end_pos

      method build_ATuple sctx meta (elem_layouts, _) ctxs =
        let t = Meta.Direct.find_exn meta Meta.type_ in
        self#build_Tuple sctx t elem_layouts ctxs

      method build_Select sctx _ (_, r) ectx = self#visit_t sctx ectx r

      method build_Filter sctx _ (_, r) ectx = self#visit_t sctx ectx r

      method build_DepJoin sctx _ {d_lhs; d_rhs; _} (ctx1, ctx2) =
        let lhs_t = Meta.(find_exn d_lhs type_) in
        let rhs_t = Meta.(find_exn d_lhs type_) in
        let tuple_t = Type.(TupleT ([lhs_t; rhs_t], {count= AbsInt.top})) in
        self#build_Tuple sctx tuple_t [d_lhs; d_rhs] [ctx1; ctx2]

      method build_AHashIdx sctx meta (_, value_l, hmeta) keys gen =
        let type_ = Meta.Direct.find_exn meta Meta.type_ in
        let key_l =
          Option.value_exn
            ~error:(Error.create "Missing key layout." hmeta [%sexp_of: hash_idx])
            hmeta.hi_key_layout
        in
        let serialize_key = function
          | [Value.String s] -> s
          | vs ->
              List.map vs ~f:(function
                | Int x -> Bitstring.of_int ~byte_width:8 x
                | Date x -> Date.to_int x |> Bitstring.of_int ~byte_width:8
                | Bool true -> Bitstring.of_int 1 ~byte_width:1
                | Bool false -> Bitstring.of_int 1 ~byte_width:1
                | String s -> s
                | v ->
                    Error.(
                      create "Unexpected key value." v [%sexp_of: Value.t] |> raise) )
              |> String.concat ~sep:"|"
        in
        Logs.debug (fun m -> m "Generating hash.") ;
        let hash, hash_body, hash_len =
          match Type.hash_kind_exn type_ with
          | `Direct ->
              let hash = Hashtbl.create (module String) in
              Gen.iter keys ~f:(fun k ->
                  let key = serialize_key k in
                  let data =
                    match k with
                    | [Value.Int x] -> x
                    | [Date x] -> Date.to_int x
                    | _ -> failwith "Unexpected key."
                  in
                  Hashtbl.set hash ~key ~data ) ;
              (hash, "", 0)
          | `Cmph ->
              let keys = Gen.map keys ~f:serialize_key |> Gen.to_list in
              let hash = Hashtbl.create (module String) in
              let hash_body =
                if List.length keys = 0 then ""
                else
                  (* Create a CMPH hash from the keyset. *)
                  let cmph_hash =
                    let open Cmph in
                    let keyset = KeySet.create keys in
                    List.find_map_exn [Config.default_chd; `Bdz; `Bmz; `Chm; `Fch]
                      ~f:(fun algo ->
                        try
                          Some
                            ( Config.create ~verbose:true ~seed:0 ~algo keyset
                            |> Hash.of_config )
                        with Error _ as err ->
                          Logs.warn (fun m ->
                              m "Creating CMPH hash failed: %a" Sexp.pp_hum
                                ([%sexp_of: exn] err) ) ;
                          None )
                  in
                  (* Populate hash table with CMPH hash values. *)
                  List.iter keys ~f:(fun k ->
                      Hashtbl.set hash ~key:k ~data:(Cmph.Hash.hash cmph_hash k) ) ;
                  Cmph.Hash.to_packed cmph_hash
              in
              let hash_len = String.length hash_body in
              (hash, hash_body, hash_len)
        in
        (* Write the hashes to a file. *)
        Out_channel.(
          with_file "hashes.txt" ~f:(fun ch ->
              fprintf ch "%s"
                (Sexp.to_string_hum ([%sexp_of: int Hashtbl.M(String).t] hash)) )) ;
        let table_size =
          Hashtbl.fold hash ~f:(fun ~key:_ ~data:v m -> Int.max m v) ~init:0
          |> fun m -> m + 1
        in
        let hdr = make_header type_ in
        let header_pos = pos writer in
        self#log "Table len" (fun () ->
            write_bytes writer (Bytes.make (size_exn hdr "len") '\x00') ) ;
        self#log "Table hash len" (fun () ->
            write_bytes writer (Bytes.make (size_exn hdr "hash_len") '\x00') ) ;
        self#log "Table hash" (fun () ->
            write_bytes writer (Bytes.make hash_len '\x00') ) ;
        self#log "Table map len" (fun () ->
            write_bytes writer (Bytes.make (size_exn hdr "hash_map_len") '\x00') ) ;
        self#log "Table key map" (fun () ->
            write_bytes writer (Bytes.make (8 * table_size) '\x00') ) ;
        let hash_table = Array.create ~len:table_size 0x0 in
        self#log "Table values" (fun () ->
            Gen.iter gen ~f:(fun (key, vctx) ->
                let value_pos = pos writer in
                let skey = serialize_key key in
                let hash_val =
                  match Hashtbl.find hash skey with
                  | Some v -> v
                  | None ->
                      Error.(
                        create "BUG: Missing key." skey [%sexp_of: string] |> raise)
                in
                hash_table.(hash_val)
                <- Pos.(value_pos |> to_bytes_exn |> Int64.to_int_exn) ;
                self#visit_t sctx (M.to_ctx key) key_l ;
                self#visit_t sctx vctx value_l ) ) ;
        let end_pos = pos writer in
        seek writer header_pos ;
        let len = Pos.(end_pos - header_pos) in
        write_string writer
          (of_int ~byte_width:(size_exn hdr "len") (Int64.to_int_exn len)) ;
        write_string writer (of_int ~byte_width:(size_exn hdr "hash_len") hash_len) ;
        write_string writer hash_body ;
        write_string writer
          (of_int ~byte_width:(size_exn hdr "hash_map_len") (8 * table_size)) ;
        Array.iteri hash_table ~f:(fun i x ->
            self#log (sprintf "Map entry (%d => %d)" i x) (fun () ->
                write_string writer (of_int ~byte_width:8 x) ) ) ;
        seek writer end_pos

      method build_AOrderedIdx sctx meta (_, value_l, ometa) keys gen =
        let t = Meta.Direct.(find_exn meta Meta.type_) in
        let _, vt, m =
          match t with OrderedIdxT (kt, vt, m) -> (kt, vt, m) | _ -> assert false
        in
        let hdr = make_header t in
        let key_l =
          Option.value_exn
            ~error:
              (Error.create "Missing key layout." ometa [%sexp_of: ordered_idx])
            ometa.oi_key_layout
        in
        (* Write a dummy header. *)
        let len_pos = pos writer in
        write_bytes writer (Bytes.make (size_exn hdr "len") '\x00') ;
        write_bytes writer (Bytes.make (size_exn hdr "idx_len") '\x00') ;
        (* Burn the keys*)
        Gen.iter keys ~f:(fun _ -> ()) ;
        let vbuf = Buffer.create 1024 in
        let valf = new serialize_fold (Writer.with_buffer vbuf) in
        let keys = ref [] in
        Gen.iter gen ~f:(fun (key, vctx) ->
            (* Serialize key. *)
            let kbuf = Buffer.create 8 in
            let kwriter = Writer.with_buffer kbuf in
            let keyf = new serialize_fold kwriter in
            keyf#log "Ordered idx key" (fun () ->
                keyf#visit_t () (M.to_ctx key) key_l ) ;
            Writer.flush kwriter ;
            keys := !keys @ [(kbuf, keyf, Writer.Pos.to_bytes_exn valf#pos)] ;
            (* Serialize value. *)
            valf#visit_t sctx vctx value_l ) ;
        let index_start = self#pos in
        let ptr_size = Type.oi_ptr_size vt m in
        List.iter !keys ~f:(fun (kbuf, keyf, vptr) ->
            self#log "Ordered idx key" (fun () ->
                self#log_insert keyf ;
                write_string writer (Buffer.contents kbuf) ) ;
            self#log (sprintf "Ordered idx ptr (=%Ld)" vptr) (fun () ->
                write_string writer (of_int64 ~byte_width:ptr_size vptr) ) ) ;
        let index_end = self#pos in
        self#log "Ordered idx body" (fun () ->
            self#log_insert valf ;
            write_string writer (Buffer.contents vbuf) ) ;
        let end_pos = self#pos in
        let len = Writer.Pos.(end_pos - len_pos) in
        let index_len = Writer.Pos.(index_end - index_start) in
        Writer.seek writer len_pos ;
        self#log (sprintf "Ordered idx len (=%Ld)" len) (fun () ->
            write_string writer (of_int64 ~byte_width:(size_exn hdr "len") len) ) ;
        self#log (sprintf "Ordered idx index len (=%Ld)" index_len) (fun () ->
            write_string writer
              (of_int64 ~byte_width:(size_exn hdr "idx_len") index_len) ) ;
        seek writer end_pos

      method serialize_null _ t =
        let hdr = make_header t in
        let str =
          match t with
          | NullT -> ""
          | _ -> null_sentinal t |> of_int ~byte_width:(size_exn hdr "value")
        in
        self#log "Null" (fun () -> write_string writer str)

      method serialize_int _ t x =
        let hdr = make_header t in
        let sval = of_int ~byte_width:(size_exn hdr "value") x in
        write_string writer sval

      method serialize_fixed _ t x =
        match t with
        | Type.FixedT {value= {scale; _}; _} ->
            let hdr = make_header t in
            let sval =
              of_int ~byte_width:(size_exn hdr "value")
                Fixed_point.((convert x scale).value)
            in
            write_string writer sval
        | _ ->
            Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

      method serialize_bool _ t x =
        let hdr = make_header t in
        let str =
          match (t, x) with
          | BoolT _, true -> of_int ~byte_width:(size_exn hdr "value") 1
          | BoolT _, false -> of_int ~byte_width:(size_exn hdr "value") 0
          | t, _ ->
              Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)
        in
        write_string writer str

      method serialize_string _ t body =
        let hdr = make_header t in
        match t with
        | StringT _ ->
            let len = String.length body in
            self#log (sprintf "String length (=%d)" len) (fun () ->
                of_int ~byte_width:(size_exn hdr "nchars") len
                |> write_string writer ) ;
            self#log "String body" (fun () -> write_string writer body)
        | t ->
            Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

      method build_AScalar sctx meta _ value =
        let type_ = Meta.Direct.(find_exn meta Meta.type_) in
        self#log
          (sprintf "Scalar (=%s)"
             ([%sexp_of: Value.t] (Lazy.force value) |> Sexp.to_string_hum))
          (fun () ->
            match Lazy.force value with
            | Null -> self#serialize_null sctx type_
            | Date x -> self#serialize_int sctx type_ (Date.to_int x)
            | Int x -> self#serialize_int sctx type_ x
            | Fixed x -> self#serialize_fixed sctx type_ x
            | Bool x -> self#serialize_bool sctx type_ x
            | String x -> self#serialize_string sctx type_ x )

      method! visit_t sctx ectx layout =
        (* Update position metadata in layout. *)
        let pos = pos writer |> Pos.to_bytes_exn in
        Meta.update layout Meta.pos ~f:(function
          | Some (Pos pos' as p) -> if Int64.(pos = pos') then p else Many_pos
          | Some Many_pos -> Many_pos
          | None -> Pos pos ) ;
        super#visit_t sctx ectx layout

      val mutable should_log = true

      method log msg f =
        if should_log then Log.with_msg log_msgs writer msg f else f ()

      method log_msgs = log_msgs

      method log_insert (child : serialize_fold) =
        let pos = Writer.Pos.to_bytes_exn self#pos in
        Bag.until_empty child#log_msgs (fun m ->
            Bag.add_unit log_msgs {m with pos= Int64.(m.Log.pos + pos)} )

      method log_render ch = Log.render log_msgs ch

      method skip_t sctx ectx layout =
        should_log <- false ;
        super#visit_t sctx ectx layout ;
        should_log <- true
    end

  let serialize writer l =
    Logs.info (fun m -> m "Serializing abstract layout.") ;
    let begin_pos = pos writer in
    (* Serialize the main layout. *)
    let serializer = new serialize_fold writer in
    serializer#run () l ;
    (* Serialize subquery layouts. *)
    let subquery_visitor =
      object
        inherit Abslayout0.runtime_subquery_visitor

        method visit_Subquery r = serializer#run () r
      end
    in
    subquery_visitor#visit_t () l ;
    let end_pos = pos writer in
    let len = Pos.(end_pos - begin_pos) |> Int64.to_int_exn in
    flush writer ;
    ( match Config.layout_map_channel with
    | Some ch -> serializer#log_render ch
    | None -> () ) ;
    (l, len)
end
