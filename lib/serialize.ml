open! Core
open Collections
open Abslayout

module Config = struct
  module type S = sig
    val layout_file : string option
  end
end

module type S = Serialize_intf.S

module Make (Config : Config.S) (M : Abslayout_db.S) = struct
  open Config

  let debug = Option.is_some layout_file

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

  class serializer =
    object (self : 'self)
      val buf = Buffer.create 1024

      val mutable writer = Writer.with_buffer (Buffer.create 1)

      initializer writer <- Writer.with_buffer buf

      method writer = writer

      method pos = Writer.pos writer

      method flush = Writer.flush writer

      method write_string = Writer.write_string writer

      method write_bytes = Writer.write_bytes writer

      method write_into (s : 'self) =
        if s = self then failwith "Cannot write into self." ;
        self#flush ;
        s#write_string (Buffer.to_string buf)

      method write_into_writer w =
        self#flush ;
        Writer.write_string w (Buffer.to_string buf)
    end

  type msg = {msg: string; pos: int64; len: int64}

  class logged_serializer =
    object (self : 'self)
      inherit serializer as super

      val msgs = Bag.create ()

      method msgs = msgs

      method log msg ~f : unit = self#logf (fun m -> m "%s" msg) ~f

      method logf
          : 'a.    ((('a, unit, string) format -> 'a) -> string) -> f:(unit -> unit)
            -> unit =
        fun msgf ~f ->
          if debug then (
            let start = self#pos in
            let ret = f () in
            Bag.add_unit msgs
              { pos= Pos.to_bytes_exn start
              ; len= Pos.(self#pos - start)
              ; msg= msgf Format.sprintf } ;
            ret )
          else f ()

      method! write_into (s : 'self) =
        let pos = Pos.to_bytes_exn s#pos in
        super#write_into s ;
        Bag.until_empty msgs (fun m ->
            Bag.add_unit s#msgs {m with pos= Int64.(m.pos + pos)} )

      method render ch =
        Bag.to_list msgs
        |> List.sort ~compare:(fun m1 m2 ->
               [%compare: int64 * int64]
                 (m1.pos, Int64.(-m1.len))
                 (m2.pos, Int64.(-m2.len)) )
        |> List.iter ~f:(fun m ->
               Out_channel.fprintf ch "%Ld:%Ld %s\n" m.pos m.len m.msg ) ;
        Out_channel.flush ch
    end

  class serialize_fold =
    object (self : 'self)
      inherit [_, _] M.unsafe_material_fold as super

      method build_AEmpty _ _ = ()

      method build_AList (s : logged_serializer) meta (_, elem_layout) gen =
        let t = Meta.Direct.(find_exn meta Meta.type_) in
        (* Serialize list body. *)
        let child = new logged_serializer in
        let count = ref 0 in
        child#log "List body" ~f:(fun () ->
            Gen.iter gen ~f:(fun (_, vctx) ->
                count := !count + 1 ;
                self#visit_t child vctx elem_layout ) ) ;
        (* Serialize list header. *)
        let hdr = make_header t in
        let len =
          let body_len = Int64.to_int_exn (Pos.to_bytes_exn child#pos) in
          size_exn hdr "count" + size_exn hdr "len" + body_len
        in
        s#logf
          (fun m -> m "List count (=%d)" !count)
          ~f:(fun () ->
            s#write_string (of_int ~byte_width:(size_exn hdr "count") !count) ) ;
        s#logf
          (fun m -> m "List len (=%d)" len)
          ~f:(fun () -> s#write_string (of_int ~byte_width:(size_exn hdr "len") len)) ;
        child#write_into s

      method build_Tuple (s : logged_serializer) t ls ctxs =
        (* Serialize body *)
        let child = new logged_serializer in
        child#log "Tuple body" ~f:(fun () ->
            List.iter2_exn ctxs ls ~f:(self#visit_t child) ) ;
        (* Serialize header. *)
        let hdr = make_header t in
        let len =
          let body_len = Int64.to_int_exn (Pos.to_bytes_exn child#pos) in
          size_exn hdr "len" + body_len
        in
        s#logf
          (fun m -> m "Tuple len (=%d)" len)
          ~f:(fun () -> s#write_string (of_int ~byte_width:(size_exn hdr "len") len)) ;
        child#write_into s

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
            ~error:(Error.create "Missing key layout." h [%sexp_of: hash_idx])
            h.hi_key_layout
        in
        (* Collect keys and write values to a child buffer. *)
        let keys = Queue.create () in
        let valf = new logged_serializer in
        Gen.iter gen ~f:(fun (key, vctx) ->
            let vptr = valf#pos |> Pos.to_bytes_exn |> Int64.to_int_exn in
            Queue.enqueue keys (key, vptr) ;
            self#visit_t valf (M.to_ctx key) key_l ;
            self#visit_t valf vctx h.hi_values ) ;
        Logs.debug (fun m -> m "Generating hash.") ;
        let hash, hash_body =
          make_hash type_ (Queue.to_array keys |> Array.to_sequence)
        in
        Logs.debug (fun m -> m "Generating hash finished.") ;
        let hdr = make_header type_ in
        let hash_len = String.length hash_body in
        let hash_map_len = Array.length hash * 8 in
        let value_len = valf#pos |> Pos.to_bytes_exn |> Int64.to_int_exn in
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
        s#log "Table values" ~f:(fun () -> valf#write_into s)

      method build_AOrderedIdx s meta (_, value_l, ometa) gen =
        let t = Meta.Direct.(find_exn meta Meta.type_) in
        let _, vt, m =
          match t with OrderedIdxT (kt, vt, m) -> (kt, vt, m) | _ -> assert false
        in
        let key_l =
          Option.value_exn
            ~error:
              (Error.create "Missing key layout." ometa [%sexp_of: ordered_idx])
            ometa.oi_key_layout
        in
        let vals = new logged_serializer in
        let keys = ref [] in
        Gen.iter gen ~f:(fun (key, vctx) ->
            (* Serialize key. *)
            let keyf = new logged_serializer in
            keyf#log "Ordered idx key" ~f:(fun () ->
                self#visit_t keyf (M.to_ctx key) key_l ) ;
            keys := !keys @ [(keyf, Pos.to_bytes_exn vals#pos)] ;
            (* Serialize value. *)
            self#visit_t vals vctx value_l ) ;
        let ptr_size = Type.oi_ptr_size vt m in
        (* Serialize keys and value pointers. *)
        let idxs = new logged_serializer in
        List.iter !keys ~f:(fun (keyf, vptr) ->
            idxs#log "Ordered idx key" ~f:(fun () -> keyf#write_into idxs) ;
            idxs#logf
              (fun m -> m "Ordered idx ptr (=%Ld)" vptr)
              ~f:(fun () -> idxs#write_string (of_int64 ~byte_width:ptr_size vptr))
        ) ;
        let hdr = make_header t in
        let idx_len = idxs#pos |> Pos.to_bytes_exn |> Int64.to_int_exn in
        let val_len = vals#pos |> Pos.to_bytes_exn |> Int64.to_int_exn in
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
        s#log "Ordered idx body" ~f:(fun () -> vals#write_into s)

      method serialize_null s t =
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

      method! visit_t s ectx layout =
        (* Update position metadata in layout. *)
        let pos = s#pos |> Pos.to_bytes_exn in
        Meta.update layout Meta.pos ~f:(function
          | Some (Pos pos' as p) -> if Int64.(pos = pos') then p else Many_pos
          | Some Many_pos -> Many_pos
          | None -> Pos pos ) ;
        super#visit_t s ectx layout
    end

  let serialize writer l =
    Logs.info (fun m -> m "Serializing abstract layout.") ;
    let serializer = new logged_serializer in
    let begin_pos = serializer#pos in
    (* Serialize the main layout. *)
    (new serialize_fold)#run serializer l ;
    (* Serialize subquery layouts. *)
    let subquery_visitor =
      object
        inherit Abslayout0.runtime_subquery_visitor

        method visit_Subquery r = (new serialize_fold)#run serializer r
      end
    in
    subquery_visitor#visit_t () l ;
    serializer#write_into_writer writer ;
    Option.iter layout_file ~f:(fun f ->
        Out_channel.with_file f ~f:serializer#render ) ;
    let len = Pos.(serializer#pos - begin_pos) |> Int64.to_int_exn in
    (l, len)
end
