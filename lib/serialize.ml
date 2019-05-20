open! Core
open! Lwt
open Collections
module A = Abslayout

module Config = struct
  module type S = sig
    val layout_file : string option
  end
end

module type S = Serialize_intf.S

module Make (Config : Config.S) (M : Abslayout_db.S) = struct
  open Config

  let debug = Option.is_some layout_file

  open Header

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

  let serialize_key = function
    | [Value.String s] -> s
    | vs ->
        List.map vs ~f:(function
          | Int x -> of_int ~byte_width:8 x
          | Date x -> Date.to_int x |> of_int ~byte_width:8
          | Bool true -> of_int 1 ~byte_width:1
          | Bool false -> of_int 1 ~byte_width:1
          | String s -> s
          | v ->
              Error.(create "Unexpected key value." v [%sexp_of: Value.t] |> raise) )
        |> String.concat ~sep:"|"

  let make_direct_hash keys =
    let hash =
      Seq.map keys ~f:(fun (k, p) ->
          let h =
            match k with
            | [Value.Int x] -> x
            | [Date x] -> Date.to_int x
            | _ -> failwith "Unexpected key."
          in
          (h, p) )
    in
    (hash, "")

  let make_cmph_hash keys =
    if Seq.length keys = 0 then (Seq.empty, "")
    else
      let keys = Seq.map keys ~f:(fun (k, p) -> (serialize_key k, p)) in
      (* Create a CMPH hash from the keyset. *)
      let cmph_hash =
        let open Cmph in
        let keyset =
          Seq.map keys ~f:(fun (k, _) -> k) |> Seq.to_list |> KeySet.create
        in
        List.find_map_exn [Config.default_chd; `Bdz; `Bmz; `Chm; `Fch]
          ~f:(fun algo ->
            try
              Some
                (Config.create ~verbose:true ~seed:0 ~algo keyset |> Hash.of_config)
            with Error _ as err ->
              Logs.warn (fun m ->
                  m "Creating CMPH hash failed: %a" Sexp.pp_hum
                    ([%sexp_of: exn] err) ) ;
              None )
      in
      (* Populate hash table with CMPH hash values. *)
      ( Seq.map keys ~f:(fun (k, p) -> (Cmph.Hash.hash cmph_hash k, p))
      , Cmph.Hash.to_packed cmph_hash )

  let make_hash type_ keys =
    let hash, body =
      match Type.hash_kind_exn type_ with
      | `Direct -> make_direct_hash keys
      | `Cmph -> make_cmph_hash keys
    in
    let max_key = Seq.fold hash ~init:0 ~f:(fun m (h, _) -> Int.max h m) in
    let hash_array = Array.create ~len:(max_key + 1) 0 in
    Seq.iter hash ~f:(fun (h, p) -> hash_array.(h) <- p) ;
    (hash_array, body)

  class serializer ?(size = 1024) () =
    object (self : 'self)
      val writer = Faraday.create size

      val mutable pos = 0

      method writer = writer

      method pos = pos

      method write_string s =
        pos <- pos + String.length s ;
        Faraday.write_string self#writer s

      method schedule_bigstring s =
        pos <- pos + Bigstringaf.length s ;
        Faraday.schedule_bigstring self#writer s

      method write_into (s : 'self) =
        if s = self then failwith "Cannot write into self." ;
        s#schedule_bigstring (Faraday.serialize_to_bigstring self#writer)

      method write_into_channel ch =
        Out_channel.output_string ch
          (Bigstringaf.to_string (Faraday.serialize_to_bigstring self#writer))
    end

  type msg = {msg: string; pos: int; len: int}

  class logged_serializer ?size () =
    object (self : 'self)
      inherit serializer ?size () as super

      val msgs = Bag.create ()

      method msgs = msgs

      method log : 'a. string -> f:(unit -> 'a) -> 'a =
        fun msg ~f -> self#logf (fun m -> m "%s" msg) ~f

      method logf
          : 'a 'b.    ((('a, unit, string) format -> 'a) -> string)
            -> f:(unit -> 'b) -> 'b =
        fun msgf ~f ->
          if debug then (
            let start = self#pos in
            let ret = f () in
            Bag.add_unit msgs
              {pos= start; len= self#pos - start; msg= msgf Format.sprintf} ;
            ret )
          else f ()

      method! write_into (s : 'self) =
        let pos = s#pos in
        super#write_into s ;
        Bag.until_empty msgs (fun m -> Bag.add_unit s#msgs {m with pos= m.pos + pos})

      method render ch =
        Bag.to_list msgs
        |> List.sort ~compare:(fun m1 m2 ->
               [%compare: int * int] (m1.pos, -m1.len) (m2.pos, -m2.len) )
        |> List.iter ~f:(fun m ->
               Out_channel.fprintf ch "%d:%d %s\n" m.pos m.len m.msg ) ;
        Out_channel.flush ch
    end

  let gen_iter_s ~f g =
    Gen.fold g ~init:(return ()) ~f:(fun p x ->
        let%lwt () = p in
        f x )

  class serialize_fold =
    object (self : 'self)
      inherit [_, _] M.unsafe_material_fold as super

      method build_AEmpty _ _ = return ()

      method build_AList (s : logged_serializer) meta (_, elem_layout) gen =
        let t = Meta.Direct.(find_exn meta Meta.type_) in
        (* Serialize list body. *)
        let child = new logged_serializer () in
        let count = ref 0 in
        let%lwt () =
          child#log "List body" ~f:(fun () ->
              gen_iter_s gen ~f:(fun (_, vctx) ->
                  count := !count + 1 ;
                  self#visit_t child vctx elem_layout ) )
        in
        (* Serialize list header. *)
        let hdr = make_header t in
        let len =
          let body_len = child#pos in
          size_exn hdr "count" + size_exn hdr "len" + body_len
        in
        s#logf
          (fun m -> m "List count (=%d)" !count)
          ~f:(fun () ->
            s#write_string (of_int ~byte_width:(size_exn hdr "count") !count) ) ;
        s#logf
          (fun m -> m "List len (=%d)" len)
          ~f:(fun () -> s#write_string (of_int ~byte_width:(size_exn hdr "len") len)) ;
        return (child#write_into s)

      method build_Tuple (s : logged_serializer) t ls ctxs =
        (* Serialize body *)
        let child = new logged_serializer () in
        let%lwt () =
          child#log "Tuple body" ~f:(fun () ->
              List.zip_exn ctxs ls
              |> Lwt_list.iter_s (fun (c, l) -> self#visit_t child c l) )
        in
        (* Serialize header. *)
        let hdr = make_header t in
        let len =
          let body_len = child#pos in
          size_exn hdr "len" + body_len
        in
        s#logf
          (fun m -> m "Tuple len (=%d)" len)
          ~f:(fun () -> s#write_string (of_int ~byte_width:(size_exn hdr "len") len)) ;
        return (child#write_into s)

      method build_ATuple s meta (elem_layouts, _) ctxs =
        let t = Meta.Direct.find_exn meta Meta.type_ in
        self#build_Tuple s t elem_layouts ctxs

      method build_Select s _ (_, r) ectx = self#visit_t s ectx r

      method build_Filter s _ (_, r) ectx = self#visit_t s ectx r

      method build_DepJoin s _ {d_lhs; d_rhs; _} (ctx1, ctx2) =
        let lhs_t = Meta.(find_exn d_lhs type_) in
        let rhs_t = Meta.(find_exn d_lhs type_) in
        let tuple_t = Type.(TupleT ([lhs_t; rhs_t], {count= AbsInt.top})) in
        self#build_Tuple s tuple_t [d_lhs; d_rhs] [ctx1; ctx2]

      method build_AHashIdx s meta h gen =
        let type_ = Meta.Direct.find_exn meta Meta.type_ in
        let key_l =
          Option.value_exn
            ~error:(Error.create "Missing key layout." h [%sexp_of: A.hash_idx])
            h.hi_key_layout
        in
        (* Collect keys and write values to a child buffer. *)
        let keys = Queue.create () in
        let valf = new logged_serializer () in
        let%lwt () =
          gen_iter_s gen ~f:(fun (key, vctx) ->
              let vptr = valf#pos in
              Queue.enqueue keys (key, vptr) ;
              let%lwt () = self#visit_t valf (M.to_ctx key) key_l in
              self#visit_t valf vctx h.hi_values )
        in
        Logs.debug (fun m -> m "Generating hash.") ;
        let hash, hash_body =
          make_hash type_ (Queue.to_array keys |> Array.to_sequence)
        in
        Logs.debug (fun m -> m "Generating hash finished.") ;
        let hdr = make_header type_ in
        let hash_len = String.length hash_body in
        let hash_map_len = Array.length hash * 8 in
        let value_len = valf#pos in
        s#log "Table len" ~f:(fun () ->
            let flen = size_exn hdr "len" in
            let len =
              flen + size_exn hdr "hash_len" + hash_len
              + size_exn hdr "hash_map_len" + hash_map_len + value_len
            in
            s#write_string (of_int ~byte_width:flen len) ) ;
        s#log "Table hash len" ~f:(fun () ->
            s#write_string (of_int ~byte_width:(size_exn hdr "hash_len") hash_len)
        ) ;
        s#log "Table hash" ~f:(fun () -> s#write_string hash_body) ;
        s#log "Table map len" ~f:(fun () ->
            s#write_string
              (of_int ~byte_width:(size_exn hdr "hash_map_len") hash_map_len) ) ;
        s#log "Table key map" ~f:(fun () ->
            Array.iteri hash ~f:(fun h p ->
                s#logf
                  (fun m -> m "Map entry (%d => %d)" h p)
                  ~f:(fun () -> s#write_string (of_int ~byte_width:8 p)) ) ) ;
        return (s#log "Table values" ~f:(fun () -> valf#write_into s))

      method build_AOrderedIdx s meta (_, value_l, ometa) gen =
        let t = Meta.Direct.(find_exn meta Meta.type_) in
        let _, vt, m =
          match t with OrderedIdxT (kt, vt, m) -> (kt, vt, m) | _ -> assert false
        in
        let key_l =
          Option.value_exn
            ~error:
              (Error.create "Missing key layout." ometa [%sexp_of: A.ordered_idx])
            ometa.oi_key_layout
        in
        let vals = new logged_serializer () in
        let keys = ref [] in
        let%lwt () =
          gen_iter_s gen ~f:(fun (key, vctx) ->
              (* Serialize key. *)
              let keyf = new logged_serializer () in
              let%lwt () =
                keyf#log "Ordered idx key" ~f:(fun () ->
                    self#visit_t keyf (M.to_ctx key) key_l )
              in
              keys := !keys @ [(keyf, vals#pos)] ;
              (* Serialize value. *)
              self#visit_t vals vctx value_l )
        in
        let ptr_size = Type.oi_ptr_size vt m in
        (* Serialize keys and value pointers. *)
        let idxs = new logged_serializer () in
        List.iter !keys ~f:(fun (keyf, vptr) ->
            idxs#log "Ordered idx key" ~f:(fun () -> keyf#write_into idxs) ;
            idxs#logf
              (fun m -> m "Ordered idx ptr (=%d)" vptr)
              ~f:(fun () -> idxs#write_string (of_int ~byte_width:ptr_size vptr)) ) ;
        let hdr = make_header t in
        let idx_len = idxs#pos in
        let val_len = vals#pos in
        let len =
          Header.size_exn hdr "len"
          + Header.size_exn hdr "idx_len"
          + idx_len + val_len
        in
        (* Write header. *)
        s#logf
          (fun m -> m "Ordered idx len (=%d)" len)
          ~f:(fun () -> s#write_string (of_int ~byte_width:(size_exn hdr "len") len)) ;
        s#logf
          (fun m -> m "Ordered idx index len (=%d)" idx_len)
          ~f:(fun () ->
            s#write_string (of_int ~byte_width:(size_exn hdr "idx_len") idx_len) ) ;
        s#log "Ordered idx map" ~f:(fun () -> idxs#write_into s) ;
        return (s#log "Ordered idx body" ~f:(fun () -> vals#write_into s))

      method serialize_null (s : logged_serializer) t =
        let hdr = make_header t in
        let str =
          match t with
          | NullT -> ""
          | _ -> null_sentinal t |> of_int ~byte_width:(size_exn hdr "value")
        in
        s#log "Null" ~f:(fun () -> s#write_string str)

      method serialize_int s t x =
        let hdr = make_header t in
        let sval = of_int ~byte_width:(size_exn hdr "value") x in
        s#write_string sval

      method serialize_fixed s t x =
        match t with
        | Type.FixedT {value= {scale; _}; _} ->
            let hdr = make_header t in
            let sval =
              of_int ~byte_width:(size_exn hdr "value")
                Fixed_point.((convert x scale).value)
            in
            s#write_string sval
        | _ ->
            Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

      method serialize_bool s t x =
        let hdr = make_header t in
        let str =
          match (t, x) with
          | BoolT _, true -> of_int ~byte_width:(size_exn hdr "value") 1
          | BoolT _, false -> of_int ~byte_width:(size_exn hdr "value") 0
          | t, _ ->
              Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)
        in
        s#write_string str

      method serialize_string (s : logged_serializer) t body =
        let hdr = make_header t in
        match t with
        | StringT _ ->
            let len = String.length body in
            s#logf
              (fun m -> m "String length (=%d)" len)
              ~f:(fun () ->
                of_int ~byte_width:(size_exn hdr "nchars") len |> s#write_string ) ;
            s#log "String body" ~f:(fun () -> s#write_string body)
        | t ->
            Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

      method build_AScalar (s : logged_serializer) meta _ value =
        let type_ = Meta.Direct.(find_exn meta Meta.type_) in
        s#logf
          (fun m ->
            m "Scalar (=%s)"
              ([%sexp_of: Value.t] (Lazy.force value) |> Sexp.to_string_hum) )
          ~f:(fun () ->
            match Lazy.force value with
            | Null -> self#serialize_null s type_
            | Date x -> self#serialize_int s type_ (Date.to_int x)
            | Int x -> self#serialize_int s type_ x
            | Fixed x -> self#serialize_fixed s type_ x
            | Bool x -> self#serialize_bool s type_ x
            | String x -> self#serialize_string s type_ x )
        |> return

      method! visit_t s ectx layout =
        (* Update position metadata in layout. *)
        let pos = s#pos in
        Meta.update layout Meta.pos ~f:(function
          | Some (Pos pos' as p) -> if pos = pos' then p else Many_pos
          | Some Many_pos -> Many_pos
          | None -> Pos pos ) ;
        super#visit_t s ectx layout
    end

  let serialize fd l =
    let open Lwt_main in
    Logs.info (fun m -> m "Serializing abstract layout.") ;
    let serializer = new logged_serializer () in
    let write =
      let writev = Faraday_lwt_unix.writev_of_fd (Lwt_unix.of_unix_file_descr fd) in
      Faraday_lwt.serialize serializer#writer ~yield:(fun _ -> yield ()) ~writev
    in
    let serialize =
      (* Serialize the main layout. *)
      let s = ref ((new serialize_fold)#run serializer l) in
      (* Serialize subquery layouts. *)
      let subquery_visitor =
        object
          inherit Abslayout0.runtime_subquery_visitor

          method visit_Subquery r =
            s := Infix.(!s >>= fun () -> (new serialize_fold)#run serializer r)
        end
      in
      subquery_visitor#visit_t () l ;
      let%lwt () = !s in
      return (Faraday.close serializer#writer)
    in
    run (join [write; serialize]) ;
    Option.iter layout_file ~f:(fun f ->
        Out_channel.with_file f ~f:serializer#render ) ;
    let len = serializer#pos in
    (l, len)
end
