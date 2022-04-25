open Core
open Ast
open Collections
open Abslayout_fold
module V = Visitors
open Header

type 'a meta =
  < type_ : Type.t
  ; pos : int option
  ; fold_stream : Abslayout_fold.Data.t
  ; meta : 'a >

let int_of_string bs =
  if String.length bs > 4 then failwith "Too many bytes.";
  let ret = ref 0 in
  for i = 0 to String.length bs - 1 do
    ret := !ret lor (int_of_char bs.[i] lsl (i * 8))
  done;
  !ret

(** Serialize an integer. Little endian. Width is the number of bits to use. *)
let of_int ~byte_width x =
  if
    (* Note that integers are stored signed. *)
    byte_width > 0
    && Float.(of_int x >= 2.0 ** ((of_int byte_width * 8.0) - 1.0))
  then
    Error.create "Integer too large." (x, byte_width) [%sexp_of: int * int]
    |> Error.raise;
  let buf = Bytes.make byte_width '\x00' in
  for i = 0 to byte_width - 1 do
    Bytes.set buf i ((x lsr (i * 8)) land 0xFF |> char_of_int)
  done;
  Bytes.to_string buf

let string_sentinal : Type.string_ -> _ = function
  | { nchars = Bottom; _ } -> 0
  | { nchars = Interval (_, max); _ } -> max + 1
  | { nchars = Top; _ } -> failwith "No available sentinal values."

let int_sentinal : Type.int_ -> _ = function
  | { range = Bottom; _ } -> 0
  | { range = Interval (_, max); _ } -> max + 1
  | { range = Top; _ } -> failwith "No available sentinal values."

let date_sentinal : Type.date -> _ = function
  | { range = Bottom; _ } -> 0
  | { range = Interval (_, max); _ } -> max + 1
  | { range = Top; _ } -> failwith "No available sentinal values."

let fixed_sentinal : Type.fixed -> _ = function
  | { value = { range = Bottom; _ }; _ } -> 0
  | { value = { range = Interval (_, max); _ }; _ } -> max + 1
  | { value = { range = Top; _ }; _ } ->
      failwith "No available sentinal values."

let bool_sentinal = 2

let null_sentinal = function
  | Type.IntT x -> int_sentinal x
  | BoolT _ -> bool_sentinal
  | StringT x -> string_sentinal x
  | DateT x -> date_sentinal x
  | FixedT x -> fixed_sentinal x
  | t -> Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

let make_direct_hash keys =
  let hash =
    Seq.map keys ~f:(fun (k, p) ->
        let h =
          match k with
          | [ Value.Int x ] -> x
          | [ Date x ] -> Date.to_int x
          | _ -> failwith "Unexpected key."
        in
        (h, p))
  in
  (hash, "")

let make_ms_hash keys =
  let nkeys = Seq.length keys in
  if nkeys = 0 then (Seq.empty, "")
  else
    let keys =
      Seq.map keys ~f:(fun (k, p) ->
          let h =
            match k with
            | [ Value.Int x ] -> x
            | [ Date x ] -> Date.to_int x
            | _ -> failwith "Unexpected key."
          in
          (h, p))
    in
    Log.debug (fun m -> m "Generating hash for %d keys." nkeys);
    let hash =
      Seq.map keys ~f:(fun (k, _) -> k)
      |> Seq.to_list
      |> Genhash.search_hash ~max_time:Time.Span.minute
    in
    Log.debug (fun m ->
        m "Generated hash with %d bins for %d keys." (Int.pow 2 hash.log_bins)
          nkeys);
    ( Seq.map keys ~f:(fun (k, p) -> (Genhash.hash hash k, p)),
      of_int ~byte_width:8 hash.Genhash.a
      ^ of_int ~byte_width:8 hash.b
      ^ of_int ~byte_width:8 hash.log_bins )

let make_hash type_ keys =
  let nkeys = Seq.length keys in
  Log.debug (fun m -> m "Generating hash for %d keys." nkeys);
  let hash, body =
    match Type.hash_kind_exn type_ with
    | `Direct -> make_direct_hash keys
    | `Universal -> make_ms_hash keys
  in
  let max_key = Seq.fold hash ~init:0 ~f:(fun m (h, _) -> Int.max h m) in
  let hash_array = Array.create ~len:(max_key + 1) 0 in
  Seq.iter hash ~f:(fun (h, p) -> hash_array.(h) <- p);
  (hash_array, body)

class _buffer_serializer ?(size = 1024) () =
  object (self : 'self)
    val buf = Buffer.create size
    method buf = buf
    method pos = Buffer.length buf
    method write_string s = Buffer.add_string buf s

    method write_into (s : 'self) =
      if phys_equal s self then failwith "Cannot write into self.";
      Buffer.add_buffer s#buf self#buf

    method write_into_channel ch = Out_channel.output_buffer ch self#buf
  end

class bigbuffer_serializer ?(size = 8) () =
  object (self : 'self)
    val buf = Bigbuffer.create size
    method buf = buf
    method pos = Bigbuffer.length buf
    method write_string s = Bigbuffer.add_string buf s

    method write_into (s : 'self) =
      if phys_equal s self then failwith "Cannot write into self.";
      Bigbuffer.add_buffer s#buf self#buf

    method write_into_channel ch =
      Bigstring_unix.really_output ch (Bigbuffer.big_contents self#buf)
  end

type msg = { msg : string; pos : int; len : int }

class logged_serializer ?(debug = false) ?size () =
  object (self : 'self)
    inherit bigbuffer_serializer ?size () as super
    val mutable msgs = []
    method msgs = msgs
    method set_msgs m = msgs <- m

    method log : 'a. string -> f:(unit -> 'a) -> 'a =
      fun msg ~f -> self#logf (fun m -> m "%s" msg) ~f

    method logf
        : 'a 'b.
          ((('a, unit, string) format -> 'a) -> string) -> f:(unit -> 'b) -> 'b
        =
      fun msgf ~f ->
        if debug then (
          let start = self#pos in
          let ret = f () in
          let m =
            { pos = start; len = self#pos - start; msg = msgf Format.sprintf }
          in
          msgs <- m :: msgs;
          ret)
        else f ()

    method! write_into (s : 'self) =
      let pos = s#pos in
      super#write_into s;
      if debug then
        s#set_msgs
          (s#msgs @ List.map msgs ~f:(fun m -> { m with pos = m.pos + pos }))

    method render ch =
      if debug then (
        msgs
        |> List.sort ~compare:(fun m1 m2 ->
               [%compare: int * int] (m1.pos, -m1.len) (m2.pos, -m2.len))
        |> List.iter ~f:(fun m ->
               Out_channel.fprintf ch "%d:%d %s\n" m.pos m.len m.msg);
        Out_channel.flush ch)
  end

let size_exn hdr name = Or_error.ok_exn (size hdr name)

let serialize_field hdr name value =
  of_int ~byte_width:(size_exn hdr name) value

class ['self] serialize_fold ?debug () =
  object (self : 'self)
    inherit [_] abslayout_fold
    method type_ meta = meta#type_
    method serializer = new logged_serializer ?debug ()
    method empty _ = self#serializer

    method list meta _ =
      let t = self#type_ meta in
      let init = (self#serializer, 0) in
      let fold acc (_, vp) =
        let body, count = acc and v = vp in
        v#write_into body;
        (body, count + 1)
      in
      let extract acc =
        let body, count = acc in
        (* Serialize header. *)
        let hdr = make_header t in
        let len =
          let body_len = body#pos in
          size_exn hdr "count" + size_exn hdr "len" + body_len
        in
        let main = self#serializer in
        main#logf
          (fun m -> m "List count (=%d)" count)
          ~f:(fun () -> main#write_string (serialize_field hdr "count" count));
        main#logf
          (fun m -> m "List len (=%d)" len)
          ~f:(fun () -> main#write_string (serialize_field hdr "len" len));
        main#log "List body" ~f:(fun () -> body#write_into main);
        main
      in
      Fold.(Fold { init; fold; extract })

    method tuple' t =
      let init = self#serializer in
      let fold acc vp =
        let body = acc and v = vp in
        v#write_into body;
        body
      in
      let extract acc =
        let body = acc in
        (* Serialize header. *)
        let main = self#serializer in
        let hdr = make_header t in
        let len =
          let body_len = body#pos in
          size_exn hdr "len" + body_len
        in
        main#logf
          (fun m -> m "Tuple len (=%d)" len)
          ~f:(fun () -> main#write_string (serialize_field hdr "len" len));

        (* Serialize body *)
        main#log "Tuple body" ~f:(fun () -> body#write_into main);
        main
      in
      Fold.(Fold { init; fold; extract })

    method tuple meta _ =
      let t = self#type_ meta in
      self#tuple' t

    method join' meta lhs rhs =
      let t = self#type_ meta in
      let lhs_t, rhs_t =
        match t with
        | FuncT ([ lhs_t; rhs_t ], _) -> (lhs_t, rhs_t)
        | _ -> assert false
      in
      Fold.run
        (self#tuple' (TupleT ([ lhs_t; rhs_t ], { kind = `Cross })))
        [ lhs; rhs ]

    method join meta _ lhs rhs = self#join' meta lhs rhs
    method depjoin meta _ lhs rhs = self#join' meta lhs rhs

    method hash_idx meta _ =
      let type_ = self#type_ meta in
      let kt, vt, m =
        match type_ with
        | HashIdxT (kt, vt, m) -> (kt, vt, m)
        | _ -> assert false
      in
      (* Collect keys and write values to a child buffer. *)
      let init = (Queue.create (), self#serializer) in
      let fold acc (key, kp, vp) =
        let kq, vs = acc and k = kp and v = vp in
        let vptr = vs#pos in
        Queue.enqueue kq (key, vptr);
        k#write_into vs;
        v#write_into vs;
        (kq, vs)
      in
      let extract acc =
        let kq, vs = acc in
        let hash, hash_body =
          make_hash type_ (Queue.to_array kq |> Array.to_sequence)
        in
        let hdr = make_header type_ in
        let hash_len = String.length hash_body in
        let ptr_size = Type.hi_ptr_size kt vt m in
        let hash_map_len = Array.length hash * ptr_size in
        let value_len = vs#pos in
        let len =
          size_exn hdr "len" + size_exn hdr "hash_len" + hash_len
          + size_exn hdr "hash_map_len"
          + hash_map_len + value_len
        in
        let main = self#serializer in
        main#logf
          (fun m -> m "Table len (=%d)" len)
          ~f:(fun () -> main#write_string (serialize_field hdr "len" len));
        main#logf
          (fun m -> m "Table hash len (=%d)" hash_len)
          ~f:(fun () ->
            main#write_string (serialize_field hdr "hash_len" hash_len));
        main#log "Table hash" ~f:(fun () -> main#write_string hash_body);
        main#logf
          (fun m -> m "Table map len (=%d)" hash_map_len)
          ~f:(fun () ->
            main#write_string (serialize_field hdr "hash_map_len" hash_map_len));
        main#log "Table key map" ~f:(fun () ->
            Array.iteri hash ~f:(fun h p ->
                main#logf
                  (fun m -> m "Map entry (%d => %d)" h p)
                  ~f:(fun () ->
                    main#write_string (of_int ~byte_width:ptr_size p))));
        main#log "Table values" ~f:(fun () -> vs#write_into main);
        main
      in
      Fold.(Fold { init; fold; extract })

    method ordered_idx meta _ =
      let type_ = self#type_ meta in
      let key_type, value_type, m =
        match type_ with
        | OrderedIdxT (kt, vt, m) -> (kt, vt, m)
        | _ -> assert false
      in

      let keys_queue = Queue.create ()
      and values_serial = self#serializer
      and key_serial = self#serializer in
      let fold () (ks, kp, vp) =
        let ks =
          match key_type with
          | Type.TupleT (ts, _) ->
              List.map2_exn ks ts ~f:(fun v t ->
                  self#serialize_scalar key_serial t v;
                  let int_key =
                    int_of_string @@ Bigbuffer.contents key_serial#buf
                  in
                  Bigbuffer.clear key_serial#buf;
                  int_key)
              |> Array.of_list |> Option.some
          | _ -> None
        in
        let k = kp and v = vp in
        Queue.enqueue keys_queue (k, ks, values_serial#pos);
        v#write_into values_serial
      in
      let extract () =
        (* Serialize keys and value pointers. *)
        let ptr_size = Type.oi_ptr_size value_type m in
        let idxs = self#serializer in
        let keys = Queue.to_list keys_queue in
        let keys =
          match key_type with
          | Type.TupleT _ ->
              let compare (_, k1, _) (_, k2, _) =
                Zorder.compare (Option.value_exn k1) (Option.value_exn k2)
              in
              List.sort keys ~compare
          | _ -> keys
        in
        List.iter keys ~f:(fun (keyf, _, vptr) ->
            idxs#log "Ordered idx key" ~f:(fun () -> keyf#write_into idxs);
            idxs#logf
              (fun m -> m "Ordered idx ptr (=%d)" vptr)
              ~f:(fun () ->
                idxs#write_string @@ of_int ~byte_width:ptr_size vptr));

        (* Write header. *)
        let hdr = make_header type_
        and idx_len = idxs#pos
        and val_len = values_serial#pos in
        let len =
          size_exn hdr "len" + size_exn hdr "idx_len" + idx_len + val_len
        in
        let main = self#serializer in
        main#logf
          (fun m -> m "Ordered idx len (=%d)" len)
          ~f:(fun () -> main#write_string (serialize_field hdr "len" len));
        main#logf
          (fun m -> m "Ordered idx index len (=%d)" idx_len)
          ~f:(fun () ->
            main#write_string (serialize_field hdr "idx_len" idx_len));
        main#log "Ordered idx map" ~f:(fun () -> idxs#write_into main);
        main#log "Ordered idx body" ~f:(fun () -> values_serial#write_into main);
        main
      in
      Fold.(Fold { init = (); fold; extract })

    method serialize_null (s : logged_serializer) t =
      let hdr = make_header t in
      match t with
      | NullT -> s#log "Null" ~f:(fun () -> s#write_string "")
      | StringT _ -> self#serialize_null_string s t
      | _ ->
          s#log "Null" ~f:(fun () ->
              s#write_string (serialize_field hdr "value" (null_sentinal t)))

    method serialize_int s t x =
      let hdr = make_header t in
      let sval = serialize_field hdr "value" x in
      s#write_string sval

    method serialize_fixed s t x =
      match t with
      | Type.FixedT { value = { scale; _ }; _ } ->
          let hdr = make_header t in
          let sval =
            serialize_field hdr "value" Fixed_point.((convert x scale).value)
          in
          s#write_string sval
      | _ ->
          Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

    method serialize_bool s t x =
      let hdr = make_header t in
      let str =
        let value =
          match (t, x) with
          | BoolT _, true -> 1
          | BoolT _, false -> 0
          | t, _ ->
              Error.(
                create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)
        in
        serialize_field hdr "value" value
      in
      s#write_string str

    method serialize_null_string (s : logged_serializer) t =
      let hdr = make_header t in
      match t with
      | StringT t ->
          s#logf
            (fun m -> m "Null string")
            ~f:(fun () ->
              serialize_field hdr "nchars" (string_sentinal t) |> s#write_string)
      | t ->
          Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

    method serialize_string (s : logged_serializer) t body =
      let hdr = make_header t in
      match t with
      | StringT _ ->
          let len = String.length body in
          s#logf
            (fun m -> m "String length (=%d)" len)
            ~f:(fun () -> serialize_field hdr "nchars" len |> s#write_string);
          s#log "String body" ~f:(fun () -> s#write_string body)
      | t ->
          Error.(create "Unexpected layout type." t [%sexp_of: Type.t] |> raise)

    method private serialize_scalar s type_ value =
      match value with
      | Value.Null -> self#serialize_null s type_
      | Date x -> self#serialize_int s type_ (Date.to_int x)
      | Int x -> self#serialize_int s type_ x
      | Fixed x -> self#serialize_fixed s type_ x
      | Bool x -> self#serialize_bool s type_ x
      | String x -> self#serialize_string s type_ x

    method scalar meta _ value =
      let type_ = self#type_ meta in
      let main = self#serializer in
      main#logf
        (fun m ->
          m "Scalar (=%s)" ([%sexp_of: Value.t] value |> Sexp.to_string_hum))
        ~f:(fun () -> self#serialize_scalar main type_ value);
      main
  end

let set_pos (r : _ annot) (pos : int) =
  {
    r with
    meta =
      object
        method fold_stream = r.meta#fold_stream
        method type_ = r.meta#type_

        method pos =
          match r.meta#pos with
          | Some pos' -> if pos = pos' then Some pos else None
          | None -> Some pos

        method meta = r.meta#meta
      end;
  }

let serialize ?layout_file fn l =
  let l =
    V.map_meta
      (fun t ->
        object
          method fold_stream = t#fold_stream
          method type_ = t#type_
          method pos = None
          method meta = t
        end)
      l
  in
  let debug = Option.is_some layout_file in
  Log.info (fun m -> m "Serializing abstract layout.");
  let serializer = new logged_serializer ~debug () in

  (* Serialize the main layout. *)
  let l = set_pos l serializer#pos in
  let w = (new serialize_fold ~debug ())#run l.meta#fold_stream l in
  w#write_into serializer;

  (* Serialize subquery layouts. *)
  let subquery_visitor =
    object
      inherit [_] V.runtime_subquery_map

      method visit_Subquery r =
        let r = set_pos r serializer#pos in
        let w = (new serialize_fold ~debug ())#run r.meta#fold_stream r in
        w#write_into serializer;
        r
    end
  in
  let l = subquery_visitor#visit_t () l in

  let len = serializer#pos in
  Out_channel.with_file fn ~f:serializer#write_into_channel;
  Option.iter layout_file ~f:(fun fn ->
      let layout_fn, layout_ch = Filename.open_temp_file "layout" "txt" in
      serializer#render layout_ch;
      (Unix.create_process ~prog:"mv" ~args:[ layout_fn; fn ]).pid
      |> Unix.waitpid |> ignore;
      Out_channel.close layout_ch);
  (l, len)
