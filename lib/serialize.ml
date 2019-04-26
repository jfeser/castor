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

module Make (Config : Config.S) (M : Abslayout_db.S) = struct
  type serialize_ctx =
    {writer: Bitstring.Writer.t sexp_opaque; log_ch: Out_channel.t sexp_opaque}
  [@@deriving sexp]

  module Log = struct
    open Bitstring.Writer

    type insert = {pos: int64; parent_id: Id.t; id: Id.t} [@@deriving sexp]

    type entry = {msg: string; pos: int64; len: int64; id: Id.t} [@@deriving sexp]

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
        |> Map.of_alist_exn (module Id)
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

  let serialize_null sctx t =
    let hdr = make_header t in
    let str =
      match t with
      | NullT -> ""
      | _ -> null_sentinal t |> of_int ~byte_width:(size_exn hdr "value")
    in
    Log.with_msg sctx "Null" (fun () -> write_string sctx.writer str)

  let serialize_int sctx t x =
    let hdr = make_header t in
    let sval = of_int ~byte_width:(size_exn hdr "value") x in
    write_string sctx.writer sval

  let serialize_fixed sctx t x =
    match t with
    | Type.FixedT {value= {scale; _}; _} ->
        let hdr = make_header t in
        let sval =
          of_int ~byte_width:(size_exn hdr "value")
            Fixed_point.((convert x scale).value)
        in
        write_string sctx.writer sval
    | _ -> Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

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

  class serialize_fold =
    object (self)
      inherit [_, _] M.unsafe_material_fold as super

      method build_AEmpty _ _ = ()

      method build_AList sctx meta (_, elem_layout) gen =
        let t = Meta.Direct.(find_exn meta Meta.type_) in
        (* Reserve space for list header. *)
        let hdr = make_header t in
        let header_pos = pos sctx.writer in
        write_bytes sctx.writer (Bytes.make (size_exn hdr "count") '\x00') ;
        write_bytes sctx.writer (Bytes.make (size_exn hdr "len") '\x00') ;
        (* Serialize list body. *)
        let count = ref 0 in
        Log.with_msg sctx "List body" (fun () ->
            Gen.iter gen ~f:(fun (_, vctx) ->
                count := !count + 1 ;
                self#visit_t sctx vctx elem_layout ) ) ;
        let end_pos = pos sctx.writer in
        (* Serialize list header. *)
        let len = Pos.(end_pos - header_pos) |> Int64.to_int_exn in
        seek sctx.writer header_pos ;
        Log.with_msg sctx (sprintf "List count (=%d)" !count) (fun () ->
            write_string sctx.writer
              (of_int ~byte_width:(size_exn hdr "count") !count) ) ;
        Log.with_msg sctx (sprintf "List len (=%d)" len) (fun () ->
            write_string sctx.writer (of_int ~byte_width:(size_exn hdr "len") len)
        ) ;
        seek sctx.writer end_pos

      method build_ATuple sctx meta (elem_layouts, _) ctxs =
        let t = Meta.Direct.find_exn meta Meta.type_ in
        (* Reserve space for header. *)
        let hdr = make_header t in
        let header_pos = pos sctx.writer in
        write_bytes sctx.writer (Bytes.make (size_exn hdr "len") '\x00') ;
        (* Serialize body *)
        Log.with_msg sctx "Tuple body" (fun () ->
            List.iter2_exn ctxs elem_layouts ~f:(self#visit_t sctx) ) ;
        let end_pos = pos sctx.writer in
        (* Serialize header. *)
        seek sctx.writer header_pos ;
        let len = Pos.(end_pos - header_pos) |> Int64.to_int_exn in
        Log.with_msg sctx (sprintf "Tuple len (=%d)" len) (fun () ->
            write_string sctx.writer (of_int ~byte_width:(size_exn hdr "len") len)
        ) ;
        seek sctx.writer end_pos

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
        let header_pos = pos sctx.writer in
        Log.with_msg sctx "Table len" (fun () ->
            write_bytes sctx.writer (Bytes.make (size_exn hdr "len") '\x00') ) ;
        Log.with_msg sctx "Table hash len" (fun () ->
            write_bytes sctx.writer (Bytes.make (size_exn hdr "hash_len") '\x00') ) ;
        Log.with_msg sctx "Table hash" (fun () ->
            write_bytes sctx.writer (Bytes.make hash_len '\x00') ) ;
        Log.with_msg sctx "Table map len" (fun () ->
            write_bytes sctx.writer
              (Bytes.make (size_exn hdr "hash_map_len") '\x00') ) ;
        Log.with_msg sctx "Table key map" (fun () ->
            write_bytes sctx.writer (Bytes.make (8 * table_size) '\x00') ) ;
        let hash_table = Array.create ~len:table_size 0x0 in
        Log.with_msg sctx "Table values" (fun () ->
            Gen.iter gen ~f:(fun (key, vctx) ->
                let value_pos = pos sctx.writer in
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
        let end_pos = pos sctx.writer in
        seek sctx.writer header_pos ;
        let len = Pos.(end_pos - header_pos) in
        write_string sctx.writer
          (of_int ~byte_width:(size_exn hdr "len") (Int64.to_int_exn len)) ;
        write_string sctx.writer
          (of_int ~byte_width:(size_exn hdr "hash_len") hash_len) ;
        write_string sctx.writer hash_body ;
        write_string sctx.writer
          (of_int ~byte_width:(size_exn hdr "hash_map_len") (8 * table_size)) ;
        Array.iteri hash_table ~f:(fun i x ->
            Log.with_msg sctx (sprintf "Map entry (%d => %d)" i x) (fun () ->
                write_string sctx.writer (of_int ~byte_width:8 x) ) ) ;
        seek sctx.writer end_pos

      method build_AOrderedIdx sctx meta (_, value_l, ometa) keys gen =
        let t = Meta.Direct.(find_exn meta Meta.type_) in
        let hdr = make_header t in
        let key_l =
          Option.value_exn
            ~error:
              (Error.create "Missing key layout." ometa [%sexp_of: ordered_idx])
            ometa.oi_key_layout
        in
        (* Write a dummy header. *)
        let len_pos = pos sctx.writer in
        write_bytes sctx.writer (Bytes.make (size_exn hdr "len") '\x00') ;
        write_bytes sctx.writer (Bytes.make (size_exn hdr "idx_len") '\x00') ;
        let index_start_pos = pos sctx.writer in
        (* Make a first pass to get the keys and value pointers set up. *)
        Gen.iter keys ~f:(fun key ->
            (* Serialize key. *)
            self#visit_t sctx (M.to_ctx key) key_l ;
            (* Save space for value pointer. *)
            write_bytes sctx.writer (Bytes.make 8 '\x00') ) ;
        let index_end_pos = pos sctx.writer in
        (* Pass over again to get values in the right places. *)
        let index_pos = ref index_start_pos in
        let value_pos = ref (pos sctx.writer) in
        Gen.iter gen ~f:(fun (key, vctx) ->
            seek sctx.writer !index_pos ;
            (* Serialize key. *)
            Log.with_msg sctx "Ordered idx key" (fun () ->
                self#visit_t sctx (M.to_ctx key) key_l ) ;
            (* Serialize value ptr. *)
            let ptr = !value_pos |> Pos.to_bytes_exn |> Int64.to_int_exn in
            Log.with_msg sctx (sprintf "Ordered idx value ptr (=%d)" ptr) (fun () ->
                write_string sctx.writer (ptr |> of_int ~byte_width:8) ) ;
            index_pos := pos sctx.writer ;
            seek sctx.writer !value_pos ;
            (* Serialize value. *)
            self#visit_t sctx vctx value_l ;
            value_pos := pos sctx.writer ) ;
        let end_pos = pos sctx.writer in
        seek sctx.writer len_pos ;
        let len = Pos.(end_pos - len_pos) in
        let index_len = Pos.(index_end_pos - index_start_pos) in
        Log.with_msg sctx (sprintf "Ordered idx len (=%Ld)" len) (fun () ->
            write_string sctx.writer (of_int64 ~byte_width:(size_exn hdr "len") len)
        ) ;
        Log.with_msg sctx (sprintf "Ordered idx index len (=%Ld)" index_len)
          (fun () ->
            write_string sctx.writer
              (of_int64 ~byte_width:(size_exn hdr "idx_len") index_len) ) ;
        seek sctx.writer end_pos

      method build_AScalar sctx meta _ value =
        let type_ = Meta.Direct.(find_exn meta Meta.type_) in
        Log.with_msg sctx
          (sprintf "Scalar (=%s)"
             ([%sexp_of: Value.t] (Lazy.force value) |> Sexp.to_string_hum))
          (fun () ->
            match Lazy.force value with
            | Null -> serialize_null sctx type_
            | Date x -> serialize_int sctx type_ (Date.to_int x)
            | Int x -> serialize_int sctx type_ x
            | Fixed x -> serialize_fixed sctx type_ x
            | Bool x -> serialize_bool sctx type_ x
            | String x -> serialize_string sctx type_ x )

      method build_Select sctx _ (_, r) ectx = self#visit_t sctx ectx r

      method build_Filter sctx _ (_, r) ectx = self#visit_t sctx ectx r

      method build_DepJoin sctx _ {d_lhs; d_rhs; _} (ctx1, ctx2) =
        self#visit_t sctx ctx1 d_lhs ; self#visit_t sctx ctx2 d_rhs

      method! visit_t sctx ectx layout =
        (* Update position metadata in layout. *)
        let pos = pos sctx.writer |> Pos.to_bytes_exn in
        Meta.update layout Meta.pos ~f:(function
          | Some (Pos pos' as p) -> if Int64.(pos = pos') then p else Many_pos
          | Some Many_pos -> Many_pos
          | None -> Pos pos ) ;
        super#visit_t sctx ectx layout
    end

  let serialize writer l t =
    Logs.info (fun m -> m "Serializing abstract layout.") ;
    let begin_pos = pos writer in
    let log_tmp_file =
      if Option.is_some Config.layout_map_channel then
        Core.Filename.temp_file "serialize" "log"
      else "/dev/null"
    in
    let log_ch = Out_channel.create log_tmp_file in
    (* Serialize the main layout. *)
    let sctx = {writer; log_ch} in
    let serializer = new serialize_fold in
    M.annotate_type l t ;
    serializer#run sctx l ;
    (* Serialize subquery layouts. *)
    let subquery_visitor =
      object
        inherit Abslayout0.runtime_subquery_visitor

        method visit_Subquery r =
          M.annotate_type r (M.to_type r) ;
          serializer#run sctx r
      end
    in
    subquery_visitor#visit_t () l ;
    let end_pos = pos writer in
    let len = Pos.(end_pos - begin_pos) |> Int64.to_int_exn in
    flush writer ;
    ( match Config.layout_map_channel with
    | Some ch -> Log.render log_tmp_file ch
    | None -> () ) ;
    (l, len)
end
