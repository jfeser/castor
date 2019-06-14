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
    if
      (* Note that integers are stored signed. *)
      byte_width > 0 && Float.(of_int x >= 2.0 ** ((of_int byte_width * 8.0) - 1.0))
    then
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
              Log.warn (fun m ->
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
      val buf = Buffer.create size

      method buf = buf

      method pos = Buffer.length buf

      method write_string s = Buffer.add_string buf s

      method write_into (s : 'self) =
        if s = self then failwith "Cannot write into self." ;
        Buffer.add_buffer s#buf self#buf

      method write_into_channel ch = Out_channel.output_buffer ch self#buf
    end

  type msg = {msg: string; pos: int; len: int}

  class logged_serializer ?size () =
    object (self : 'self)
      inherit serializer ?size () as super

      val mutable msgs = []

      method msgs = msgs

      method set_msgs m = msgs <- m

      method log : 'a. string -> f:(unit -> 'a) -> 'a =
        fun msg ~f -> self#logf (fun m -> m "%s" msg) ~f

      method logf
          : 'a 'b.    ((('a, unit, string) format -> 'a) -> string)
            -> f:(unit -> 'b) -> 'b =
        fun msgf ~f ->
          if debug then (
            let start = self#pos in
            let ret = f () in
            let m = {pos= start; len= self#pos - start; msg= msgf Format.sprintf} in
            msgs <- m :: msgs ;
            ret )
          else f ()

      method! write_into (s : 'self) =
        let pos = s#pos in
        super#write_into s ;
        s#set_msgs (s#msgs @ List.map msgs ~f:(fun m -> {m with pos= m.pos + pos}))

      method render ch =
        msgs
        |> List.sort ~compare:(fun m1 m2 ->
               [%compare: int * int] (m1.pos, -m1.len) (m2.pos, -m2.len) )
        |> List.iter ~f:(fun m ->
               Out_channel.fprintf ch "%d:%d %s\n" m.pos m.len m.msg ) ;
        Out_channel.flush ch
    end

  class serialize_fold =
    object (self : 'self)
      inherit [_] M.abslayout_fold

      method empty _ = return (new logged_serializer ())

      method list meta _ =
        let t = Meta.Direct.find_exn meta Meta.type_ in
        let init = return (new logged_serializer (), 0) in
        let fold acc (_, vp) =
          let%lwt body, count = acc and v = vp in
          v#write_into body ;
          return (body, count + 1)
        in
        let extract acc =
          let%lwt body, count = acc in
          (* Serialize header. *)
          let hdr = make_header t in
          let len =
            let body_len = body#pos in
            size_exn hdr "count" + size_exn hdr "len" + body_len
          in
          let main = new logged_serializer () in
          main#logf
            (fun m -> m "List count (=%d)" count)
            ~f:(fun () ->
              main#write_string (of_int ~byte_width:(size_exn hdr "count") count) ) ;
          main#logf
            (fun m -> m "List len (=%d)" len)
            ~f:(fun () ->
              main#write_string (of_int ~byte_width:(size_exn hdr "len") len) ) ;
          main#log "List body" ~f:(fun () -> body#write_into main) ;
          return main
        in
        M.Fold.(Fold {init; fold; extract})

      method tuple' t =
        let init = return (new logged_serializer ()) in
        let fold acc vp =
          let%lwt body = acc and v = vp in
          v#write_into body ; return body
        in
        let extract acc =
          let%lwt body = acc in
          (* Serialize header. *)
          let main = new logged_serializer () in
          let hdr = make_header t in
          let len =
            let body_len = body#pos in
            size_exn hdr "len" + body_len
          in
          main#logf
            (fun m -> m "Tuple len (=%d)" len)
            ~f:(fun () ->
              main#write_string (of_int ~byte_width:(size_exn hdr "len") len) ) ;
          (* Serialize body *)
          main#log "Tuple body" ~f:(fun () -> body#write_into main) ;
          return main
        in
        M.Fold.(Fold {init; fold; extract})

      method tuple meta _ =
        let t = Meta.Direct.find_exn meta Meta.type_ in
        self#tuple' t

      method join' meta lhs rhs =
        let t = Meta.Direct.find_exn meta Meta.type_ in
        let lhs_t, rhs_t =
          match t with
          | FuncT ([lhs_t; rhs_t], _) -> (lhs_t, rhs_t)
          | _ -> assert false
        in
        M.Fold.run (self#tuple' (TupleT ([lhs_t; rhs_t], {kind= `Cross}))) [lhs; rhs]

      method join meta _ lhs rhs = self#join' meta lhs rhs

      method depjoin meta _ lhs rhs = self#join' meta lhs rhs

      method hash_idx meta _ =
        let type_ = Meta.Direct.find_exn meta Meta.type_ in
        let kt, vt, m =
          match type_ with HashIdxT (kt, vt, m) -> (kt, vt, m) | _ -> assert false
        in
        (* Collect keys and write values to a child buffer. *)
        let init = return (Queue.create (), new logged_serializer ()) in
        let fold acc (key, kp, vp) =
          let%lwt kq, vs = acc and k = kp and v = vp in
          let vptr = vs#pos in
          Queue.enqueue kq (key, vptr) ;
          k#write_into vs ;
          v#write_into vs ;
          return (kq, vs)
        in
        let extract acc =
          let%lwt kq, vs = acc in
          Log.debug (fun m -> m "Generating hash.") ;
          let hash, hash_body =
            make_hash type_ (Queue.to_array kq |> Array.to_sequence)
          in
          Log.debug (fun m -> m "Generating hash finished.") ;
          let hdr = make_header type_ in
          let hash_len = String.length hash_body in
          let ptr_size = Type.hi_ptr_size kt vt m in
          let hash_map_len = Array.length hash * ptr_size in
          let value_len = vs#pos in
          let len =
            size_exn hdr "len" + size_exn hdr "hash_len" + hash_len
            + size_exn hdr "hash_map_len" + hash_map_len + value_len
          in
          let main = new logged_serializer () in
          main#logf
            (fun m -> m "Table len (=%d)" len)
            ~f:(fun () ->
              main#write_string (of_int ~byte_width:(size_exn hdr "len") len) ) ;
          main#logf
            (fun m -> m "Table hash len (=%d)" hash_len)
            ~f:(fun () ->
              main#write_string
                (of_int ~byte_width:(size_exn hdr "hash_len") hash_len) ) ;
          main#log "Table hash" ~f:(fun () -> main#write_string hash_body) ;
          main#logf
            (fun m -> m "Table map len (=%d)" hash_map_len)
            ~f:(fun () ->
              main#write_string
                (of_int ~byte_width:(size_exn hdr "hash_map_len") hash_map_len) ) ;
          main#log "Table key map" ~f:(fun () ->
              Array.iteri hash ~f:(fun h p ->
                  main#logf
                    (fun m -> m "Map entry (%d => %d)" h p)
                    ~f:(fun () -> main#write_string (of_int ~byte_width:ptr_size p))
              ) ) ;
          main#log "Table values" ~f:(fun () -> vs#write_into main) ;
          return main
        in
        M.Fold.(Fold {init; fold; extract})

      method ordered_idx meta _ =
        let t = Meta.Direct.(find_exn meta Meta.type_) in
        let _, vt, m =
          match t with OrderedIdxT (kt, vt, m) -> (kt, vt, m) | _ -> assert false
        in
        let init = return ([], new logged_serializer ()) in
        let fold acc (_, kp, vp) =
          let%lwt keys, vals = acc and k = kp and v = vp in
          let keys = keys @ [(k, vals#pos)] in
          v#write_into vals ;
          return (keys, vals)
        in
        let extract acc =
          let%lwt keys, vals = acc in
          (* Serialize keys and value pointers. *)
          let ptr_size = Type.oi_ptr_size vt m in
          let idxs = new logged_serializer () in
          List.iter keys ~f:(fun (keyf, vptr) ->
              idxs#log "Ordered idx key" ~f:(fun () -> keyf#write_into idxs) ;
              idxs#logf
                (fun m -> m "Ordered idx ptr (=%d)" vptr)
                ~f:(fun () -> idxs#write_string (of_int ~byte_width:ptr_size vptr))
          ) ;
          let hdr = make_header t in
          let idx_len = idxs#pos in
          let val_len = vals#pos in
          let len =
            Header.size_exn hdr "len"
            + Header.size_exn hdr "idx_len"
            + idx_len + val_len
          in
          (* Write header. *)
          let main = new logged_serializer () in
          main#logf
            (fun m -> m "Ordered idx len (=%d)" len)
            ~f:(fun () ->
              main#write_string (of_int ~byte_width:(size_exn hdr "len") len) ) ;
          main#logf
            (fun m -> m "Ordered idx index len (=%d)" idx_len)
            ~f:(fun () ->
              main#write_string
                (of_int ~byte_width:(size_exn hdr "idx_len") idx_len) ) ;
          main#log "Ordered idx map" ~f:(fun () -> idxs#write_into main) ;
          main#log "Ordered idx body" ~f:(fun () -> vals#write_into main) ;
          return main
        in
        M.Fold.(Fold {init; fold; extract})

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

      method scalar meta _ value =
        let type_ = Meta.Direct.(find_exn meta Meta.type_) in
        let main = new logged_serializer () in
        main#logf
          (fun m ->
            m "Scalar (=%s)" ([%sexp_of: Value.t] value |> Sexp.to_string_hum) )
          ~f:(fun () ->
            match value with
            | Null -> self#serialize_null main type_
            | Date x -> self#serialize_int main type_ (Date.to_int x)
            | Int x -> self#serialize_int main type_ x
            | Fixed x -> self#serialize_fixed main type_ x
            | Bool x -> self#serialize_bool main type_ x
            | String x -> self#serialize_string main type_ x ) ;
        return main
    end

  let set_pos r pos =
    Meta.update r Meta.pos ~f:(function
      | Some (Pos pos' as p) -> if pos = pos' then p else Many_pos
      | Some Many_pos -> Many_pos
      | None -> Pos pos )

  let serialize ch l =
    Log.info (fun m -> m "Serializing abstract layout.") ;
    let serializer = new logged_serializer () in
    let serialize =
      (* Serialize the main layout. *)
      set_pos l serializer#pos ;
      let s =
        ref
          (let%lwt w = (new serialize_fold)#run l in
           return (w#write_into serializer))
      in
      (* Serialize subquery layouts. *)
      let subquery_visitor =
        object
          inherit Abslayout0.runtime_subquery_visitor

          method visit_Subquery r =
            set_pos r serializer#pos ;
            s :=
              let%lwt () = !s in
              let%lwt w = (new serialize_fold)#run r in
              return (w#write_into serializer)
        end
      in
      subquery_visitor#visit_t () l ;
      let%lwt () = !s in
      return ()
    in
    Lwt_main.run serialize ;
    serializer#write_into_channel ch ;
    Option.iter layout_file ~f:(fun f ->
        Out_channel.with_file f ~f:serializer#render ) ;
    let len = serializer#pos in
    (l, len)
end
