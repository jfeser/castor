open Core
open Base
open Collections
open Implang
module A = Abslayout

type ir_module =
  {iters: func list; funcs: func list; params: Name.Compare.t list; buffer_len: int}
[@@deriving compare, sexp]

type callback = Builder.t -> expr list -> unit

exception IRGenError of Error.t [@@deriving sexp]

module Config = struct
  module type S = sig
    val debug : bool

    val code_only : bool
  end
end

module type S = sig
  val irgen : params:Name.t list -> data_fn:string -> Abslayout.t -> ir_module

  val pp : Formatter.t -> ir_module -> unit
end

module Make
    (Config : Config.S)
    (Abslayout_db : Abslayout_db.S)
    (Serialize : Serialize.S)
    () =
struct
  let fresh = Fresh.create ()

  let iters = ref []

  let add_iter i = iters := i :: !iters

  let debug_print msg v b =
    let open Builder in
    if Config.debug then build_print (Tuple [String msg; v]) b

  (** Generate an expression for the length of a value at `start` with type
     `type_`. Compensates for restrictions in the Header api. *)
  let rec len start type_ =
    let hdr = Header.make_header type_ in
    match type_ with
    | StringT _ ->
        Infix.(
          int (Header.size_exn hdr "nchars") + Header.make_access hdr "nchars" start)
    | FuncT ([t], _) -> len start t
    | _ -> Header.make_access hdr "len" start

  (** Add layout start positions to contexts that don't contain them.
       Sometimes the start is passed in by the parent layout and sometimes it is
       fixed and known by the layout. *)
  let add_layout_start ctx r =
    let ctx =
      let start_name = Name.create ~type_:Type.PrimType.int_t "start" in
      if Config.code_only then
        Map.update ctx start_name ~f:(function
          | Some x -> x
          | None -> Ctx.(Global Infix.(int 0)) )
      else
        match Meta.(find r pos) with
        | Some (Pos start) ->
            (* We don't want to bind over a start parameter that's already being
               passed in. *)
            Map.update ctx start_name ~f:(function
              | Some x -> x
              | None -> Ctx.(Global Infix.(int (Int64.to_int_exn start))) )
        | Some Many_pos | None -> ctx
    in
    ctx

  let types_of_schema s = List.map s ~f:Name.type_exn

  let type_of_schema s = Type.PrimType.TupleT (types_of_schema s)

  let type_of_layout l = Meta.(find_exn l schema) |> type_of_schema

  let types_of_layout l = Meta.(find_exn l schema) |> types_of_schema

  let list_of_tuple t b =
    match Builder.type_of t b with
    | TupleT ts -> List.mapi ts ~f:(fun i _ -> Infix.(index t i))
    | _ -> failwith "Expected a tuple type."

  let build_bin_search build_key n low_target high_target callback b =
    let open Builder in
    let low = build_defn "low" Infix.(int 0) b in
    let high = build_defn "high" n b in
    build_loop
      Infix.(low < high)
      (fun b ->
        let mid = build_defn "mid" Infix.((low + high) / int 2) b in
        let key = build_key mid b in
        build_if
          ~cond:(build_lt key (Tuple [low_target]) b)
          ~then_:(fun b -> build_assign Infix.(mid + int 1) low b)
          ~else_:(fun b -> build_assign mid high b)
          b )
      b ;
    build_if
      ~cond:Infix.(low < n)
      ~then_:(fun b ->
        let key = build_defn "key" (build_key low b) b in
        build_loop
          Infix.(build_lt key (Tuple [high_target]) b && low < n)
          (fun b ->
            callback key low b ;
            build_assign Infix.(low + int 1) low b ;
            build_assign (build_key low b) key b )
          b )
      ~else_:(fun _ -> ())
      b

  let collect_aggs p =
    let visitor =
      object (self : 'a)
        inherit [_] Abslayout0.mapreduce

        inherit [_] Util.list_monoid

        method private visit_Agg kind p =
          let n = kind ^ Fresh.name fresh "%d" in
          (A.Name (Name.create n), [(n, p)])

        method! visit_Sum () p = self#visit_Agg "sum" (A.Sum p)

        method! visit_Count () = self#visit_Agg "count" A.Count

        method! visit_Min () p = self#visit_Agg "min" (A.Min p)

        method! visit_Max () p = self#visit_Agg "max" (A.Max p)

        method! visit_Avg () p = self#visit_Agg "avg" (A.Avg p)
      end
    in
    visitor#visit_pred () p

  let rec scan ctx b r t (cb : callback) =
    match r.Abslayout.node with
    | As (_, r) -> scan ctx b r t cb
    | _ -> scan' ctx b r t cb

  and scan' ctx b r t (cb : callback) =
    let ctx = add_layout_start ctx r in
    match (r.node, t) with
    | Abslayout.AScalar r', Type.IntT t' -> scan_int ctx b r' t' cb
    | AScalar r', FixedT t' -> scan_fixed ctx b r' t' cb
    | AScalar r', BoolT t' -> scan_bool ctx b r' t' cb
    | AScalar r', StringT t' -> scan_string ctx b r' t' cb
    | _, EmptyT -> scan_empty ctx b () () cb
    | AScalar r', NullT -> scan_null ctx b r' () cb
    | ATuple r', TupleT t' -> scan_tuple ctx b r' t' cb
    | AList r', ListT t' -> scan_list ctx b r' t' cb
    | AHashIdx r', HashIdxT t' -> scan_hash_idx ctx b r' t' cb
    | AOrderedIdx r', OrderedIdxT t' -> scan_ordered_idx ctx b r' t' cb
    | Filter r', FuncT t' -> scan_filter ctx b r' t' cb
    | Select r', FuncT t' -> scan_select ctx b r' t' cb
    | (Join _ | GroupBy _ | OrderBy _ | Dedup _ | Scan _ | As _), _ ->
        Error.create "Unsupported at runtime." r [%sexp_of: A.t] |> Error.raise
    | _ ->
        Error.create "Mismatched type." (r, t) [%sexp_of: A.t * Type.t]
        |> Error.raise

  and type_of_pred ctx p b =
    let open Builder in
    let b' = new_scope b in
    let ret = gen_pred ctx p b' in
    type_of ret b'

  and gen_pred ctx pred b =
    let open Builder in
    let rec gen_pred p b =
      match p with
      | A.Null -> Null
      | A.Int x -> Int x
      | A.String x -> String x
      | A.Fixed x -> Fixed x
      | Date x -> Int (Date.to_int x)
      | Unop (op, p) -> (
          let x = gen_pred p b in
          match op with
          | A.Not -> Infix.(not x)
          | A.Year -> Infix.(int 365 * x)
          | A.Month -> Infix.(int 30 * x)
          | A.Day -> x
          | A.Strlen -> Unop {op= StrLen; arg= x}
          | A.ExtractY -> Unop {op= ExtractY; arg= x}
          | A.ExtractM -> Unop {op= ExtractM; arg= x}
          | A.ExtractD -> Unop {op= ExtractD; arg= x} )
      | A.Bool x -> Bool x
      | A.As_pred (x, _) -> gen_pred x b
      | Name n -> (
        match Ctx.find ctx n b with
        | Some e -> e
        | None ->
            Error.create "Unbound variable." (n, ctx) [%sexp_of: Name.t * Ctx.t]
            |> Error.raise )
      | A.Binop (op, arg1, arg2) -> (
          let e1 = gen_pred arg1 b in
          let e2 = gen_pred arg2 b in
          match op with
          | A.Eq -> build_eq e1 e2 b
          | A.Lt -> build_lt e1 e2 b
          | A.Le -> build_le e1 e2 b
          | A.Gt -> build_gt e1 e2 b
          | A.Ge -> build_ge e1 e2 b
          | A.And -> Infix.(e1 && e2)
          | A.Or -> Infix.(e1 || e2)
          | A.Add -> build_add e1 e2 b
          | A.Sub -> build_sub e1 e2 b
          | A.Mul -> build_mul e1 e2 b
          | A.Div -> build_div e1 e2 b
          | A.Mod -> Infix.(e1 % e2)
          | A.Strpos -> Binop {op= StrPos; arg1= e1; arg2= e2} )
      | (A.Count | A.Min _ | A.Max _ | A.Sum _ | A.Avg _) as p ->
          Error.create "Not a scalar predicate." p [%sexp_of: A.pred] |> Error.raise
      | A.If (p1, p2, p3) ->
          let ret_var =
            build_var "ret"
              (Type.PrimType.unify (type_of_pred ctx p2 b) (type_of_pred ctx p3 b))
              b
          in
          build_if ~cond:(gen_pred p1 b)
            ~then_:(fun b -> build_assign (gen_pred p2 b) ret_var b)
            ~else_:(fun b -> build_assign (gen_pred p3 b) ret_var b)
            b ;
          ret_var
          (* Ternary (gen_pred p1 b, gen_pred p2 b, gen_pred p3 b) *)
      | A.First r ->
          (* Don't use the passed in start value. Subquery layouts are not stored
           inline. *)
          let ctx = Map.remove ctx (Name.create "start") in
          let t = Meta.(find_exn r type_) in
          let ret_var = build_var "first" (List.hd_exn (types_of_layout r)) b in
          scan ctx b r t (fun b tup -> build_assign (List.hd_exn tup) ret_var b) ;
          ret_var
      | A.Exists r ->
          let ctx = Map.remove ctx (Name.create "start") in
          let t = Meta.(find_exn r type_) in
          let ret_var = build_defn "exists" (Bool false) b in
          scan ctx b r t (fun b _ -> build_assign (Bool true) ret_var b) ;
          ret_var
      | A.Substring (e1, e2, e3) ->
          Substr (gen_pred e1 b, gen_pred e2 b, gen_pred e3 b)
    in
    gen_pred pred b

  and scan_empty _ _ _ _ _ = ()

  and scan_null _ b _ _ (cb : callback) = cb b [Null]

  and scan_int ctx b _ (Type.({nullable; range= (_, h) as range; _}) : Type.int_)
      (cb : callback) =
    let open Builder in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let ival = Slice (start, Type.AbsInt.byte_width ~nullable range) in
    let _nval =
      if nullable then
        let null_val = h + 1 in
        Infix.(build_eq ival (int null_val) b)
      else Bool false
    in
    debug_print "int" ival b ;
    cb b [ival]

  and scan_fixed ctx b _ Type.({nullable; value= {range= (_, h) as range; scale}})
      (cb : callback) =
    let open Builder in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let ival = Slice (start, Type.AbsInt.byte_width ~nullable range) in
    let sval = Infix.(int scale) in
    let xval = build_div (int2fl ival) (int2fl sval) b in
    let _nval =
      if nullable then
        let null_val = h + 1 in
        Infix.(build_eq ival (int null_val) b)
      else Bool true
    in
    debug_print "fixed" xval b ;
    cb b [xval]

  and scan_bool ctx b _ _ (cb : callback) =
    let start = Ctx.find_exn ctx (Name.create "start") b in
    cb b [Unop {op= LoadBool; arg= start}]

  and scan_string ctx b _ (Type.({nchars= l, _; nullable; _}) as t) (cb : callback)
      =
    let open Builder in
    let hdr = Header.make_header (StringT t) in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let value_ptr = Header.make_position hdr "value" start in
    let nchars = Header.make_access hdr "nchars" start in
    let xval = Binop {op= LoadStr; arg1= value_ptr; arg2= nchars} in
    let _nval =
      if nullable then
        let null_val = l - 1 in
        Infix.(build_eq nchars (int null_val) b)
      else Bool true
    in
    debug_print "string" xval b ;
    cb b [xval]

  and scan_crosstuple ctx b (child_layouts, _) ((child_types, _) as t)
      (cb : callback) =
    let open Builder in
    let rec make_loops ctx fields clayouts ctypes cstarts b =
      match (clayouts, ctypes, cstarts) with
      | [], [], [] -> cb b fields
      | clayout :: clayouts, ctype :: ctypes, cstart :: cstarts ->
          let ctx = Ctx.bind ctx "start" Type.PrimType.int_t cstart in
          scan ctx b clayout ctype (fun b tup ->
              let next_ctx =
                let tuple_ctx = Ctx.of_schema Meta.(find_exn clayout schema) tup in
                Ctx.bind_ctx ctx tuple_ctx
              in
              make_loops next_ctx (fields @ tup) clayouts ctypes cstarts b )
      | _ -> failwith ""
    in
    let hdr = Header.make_header (TupleT t) in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let child_starts =
      let _, ret =
        List.fold_left child_types
          ~init:(Header.make_position hdr "value" start, [])
          ~f:(fun (cstart, ret) ctype ->
            let cstart = build_defn "cstart" cstart b in
            let next_cstart = Infix.(cstart + len cstart ctype) in
            (next_cstart, cstart :: ret) )
      in
      List.rev ret
    in
    debug_print "scanning crosstuple" (Int 0) b ;
    make_loops ctx [] child_layouts child_types child_starts b

  and scan_ziptuple ctx b r t cb =
    let open Builder in
    let child_layouts, _ = r in
    let child_types, meta = t in
    let hdr = Header.make_header (TupleT t) in
    let pstart = Ctx.find_exn ctx (Name.create "start") b in
    let cstart = build_defn "cstart" pstart b in
    let ctx = Ctx.bind ctx "start" Type.PrimType.int_t cstart in
    let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
    (* Build iterator initializers using the computed start positions. *)
    build_assign (Header.make_position hdr "value" pstart) cstart b ;
    let callee_funcs =
      List.zip_exn child_layouts child_types
      |> List.map ~f:(fun (callee_layout, callee_type) ->
             (* Construct a pull based iterator for each column in the zip tuple.
             This loop also initializes the iterator and computes the next start
             position. *)
             let b' =
               create ~ctx:callee_ctx ~name:(Fresh.name fresh "zt_%d")
                 ~ret:(type_of_layout callee_layout)
                 ~fresh
             in
             scan callee_ctx b' callee_layout callee_type (fun b tup ->
                 build_yield (Tuple tup) b ) ;
             let iter = build_func b' in
             add_iter iter ;
             build_iter iter callee_args b ;
             build_assign Infix.(cstart + len cstart callee_type) cstart b ;
             iter )
    in
    let child_tuples =
      List.map callee_funcs ~f:(fun f -> build_var "tup" f.ret_type b)
    in
    let build_body b =
      List.iter2_exn callee_funcs child_tuples ~f:(fun f t -> build_step t f b) ;
      let tup =
        List.map2_exn child_types child_tuples ~f:(fun in_t child_tup ->
            List.init (Type.width in_t) ~f:(fun i -> Infix.(index child_tup i)) )
        |> List.concat
        |> fun l -> Tuple l
      in
      cb b (list_of_tuple tup b)
    in
    match Type.AbsCount.kind meta.count with
    | `Count x -> build_count_loop Infix.(int x) build_body b
    | `Countable | `Unknown ->
        build_body b ;
        let not_done =
          List.fold_left callee_funcs ~init:(Bool true) ~f:(fun acc f ->
              Infix.(acc && not (Done f.name)) )
        in
        build_loop not_done build_body b

  and scan_tuple ctx b ((_, kind) as r) t (cb : callback) =
    match kind with
    | Abslayout.Cross -> scan_crosstuple ctx b r t cb
    | Zip -> scan_ziptuple ctx b r t cb
    | Concat -> scan_concattuple ctx b r t cb

  and scan_concattuple ctx b r t cb =
    let open Builder in
    let child_layouts, _ = r in
    let child_types, _ = t in
    let hdr = Header.make_header (TupleT t) in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let cstart = build_defn "cstart" (Header.make_position hdr "value" start) b in
    let ctx = Ctx.bind ctx "start" Type.PrimType.int_t cstart in
    List.iter2_exn child_layouts child_types ~f:(fun r' t' ->
        scan ctx b r' t' cb ;
        build_assign (build_add (Int 1) (len cstart t') b) cstart b )

  and scan_list ctx b (_, child_layout) ((child_type, _) as t) (cb : callback) =
    let open Builder in
    let hdr = Header.make_header (ListT t) in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let cstart = build_defn "cstart" (Header.make_position hdr "value" start) b in
    let callee_ctx = Ctx.bind ctx "start" Type.PrimType.int_t cstart in
    debug_print "scanning list" (Int 0) b ;
    build_count_loop
      (Header.make_access hdr "count" start)
      (fun b ->
        let clen = len cstart child_type in
        scan callee_ctx b child_layout child_type cb ;
        build_assign Infix.(cstart + clen) cstart b )
      b

  and scan_hash_idx ctx b r t cb =
    let open Builder in
    let _, value_layout, Abslayout.({lookup; hi_key_layout= m_key_layout}) = r in
    let key_layout = Option.value_exn m_key_layout in
    let key_type, value_type, _ = t in
    let hdr = Header.make_header (HashIdxT t) in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let kstart = build_var ~persistent:false "kstart" Type.PrimType.int_t b in
    let vstart = build_var ~persistent:false "vstart" Type.PrimType.int_t b in
    let key_tuple =
      build_var ~persistent:false "key" (type_of_layout key_layout) b
    in
    let key_ctx = Ctx.bind ctx "start" Type.PrimType.int_t kstart in
    let value_ctx =
      let key_schema = Meta.(find_exn key_layout schema) in
      let ctx =
        Ctx.bind_ctx ctx (Ctx.of_schema key_schema (list_of_tuple key_tuple b))
      in
      Ctx.bind ctx "start" Type.PrimType.int_t vstart
    in
    let hash_data_start = Header.make_position hdr "hash_data" start in
    let mapping_start = Header.make_position hdr "hash_map" start in
    let mapping_len = Header.make_access hdr "hash_map_len" start in
    let lookup_expr = List.map lookup ~f:(fun p -> gen_pred ctx p b) in
    (* Compute the index in the mapping table for this key. *)
    let hash_key =
      match lookup_expr with
      | [] -> failwith "empty hash key"
      | [x] -> build_hash hash_data_start x b
      | xs -> build_hash hash_data_start (Tuple xs) b
    in
    debug_print "hashing key" (Tuple lookup_expr) b ;
    let hash_key = Infix.(hash_key * int 8) in
    (* Get a pointer to the value. *)
    let value_ptr = Infix.(Slice (mapping_start + hash_key, 8)) in
    debug_print "value pointer is" value_ptr b ;
    (* If the pointer is null, then the key is not present. *)
    build_if
      ~cond:
        Infix.(
          hash_key < int 0
          || hash_key >= mapping_len
          || build_eq value_ptr (int 0x0) b)
      ~then_:(fun b -> debug_print "no values found" value_ptr b)
      ~else_:(fun b ->
        debug_print "found values" value_ptr b ;
        build_assign value_ptr kstart b ;
        scan key_ctx b key_layout key_type (fun b tup ->
            build_assign (Tuple tup) key_tuple b ) ;
        build_assign Infix.(value_ptr + len value_ptr key_type) vstart b ;
        build_if
          ~cond:(build_eq key_tuple (Tuple lookup_expr) b)
          ~then_:(fun b ->
            scan value_ctx b value_layout value_type (fun b value_tup ->
                cb b (list_of_tuple key_tuple b @ value_tup) ) )
          ~else_:(fun _ -> ())
          b )
      b

  and scan_ordered_idx ctx b r t cb =
    let open Builder in
    let ( _
        , value_layout
        , Abslayout.({oi_key_layout= m_key_layout; lookup_low; lookup_high; _}) ) =
      r
    in
    let key_type, value_type, _ = t in
    let key_layout = Option.value_exn m_key_layout in
    let hdr = Header.make_header (OrderedIdxT t) in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let kstart = build_var "kstart" Type.PrimType.int_t b in
    let vstart = build_var "vstart" Type.PrimType.int_t b in
    let key_tuple = build_var "key" (type_of_layout key_layout) b in
    let key_ctx = Ctx.bind ctx "start" Type.PrimType.int_t kstart in
    let value_ctx =
      let key_schema = Meta.(find_exn key_layout schema) in
      let ctx =
        Ctx.bind_ctx ctx (Ctx.of_schema key_schema (list_of_tuple key_tuple b))
      in
      Ctx.bind ctx "start" Type.PrimType.int_t vstart
    in
    let index_len = Header.make_access hdr "idx_len" start in
    let index_start = Header.make_position hdr "idx" start in
    let key_len = len index_start key_type in
    let ptr_len = Infix.(int 8) in
    let kp_len = Infix.(key_len + ptr_len) in
    let key_index i b =
      build_assign Infix.(index_start + (i * kp_len)) kstart b ;
      let key = build_var ~persistent:false "key" (type_of_layout key_layout) b in
      scan key_ctx b key_layout key_type (fun b tup ->
          build_assign (Tuple tup) key b ) ;
      key
    in
    let n = Infix.(index_len / kp_len) in
    build_bin_search key_index n (gen_pred ctx lookup_low b)
      (gen_pred ctx lookup_high b)
      (fun key idx b ->
        build_assign
          Infix.(Slice (index_start + (idx * kp_len) + key_len, 8))
          vstart b ;
        build_assign key key_tuple b ;
        scan value_ctx b value_layout value_type (fun b value_tup ->
            cb b (list_of_tuple key_tuple b @ value_tup) ) )
      b

  and scan_filter ctx b r t cb =
    let open Builder in
    let pred, child_layout = r in
    let child_type =
      match t with [ct], _ -> ct | _ -> failwith "Unexpected type."
    in
    scan ctx b child_layout child_type (fun b tup ->
        let ctx =
          let child_schema = Meta.(find_exn child_layout schema) in
          Ctx.bind_ctx ctx (Ctx.of_schema child_schema tup)
        in
        let cond = gen_pred ctx pred b in
        build_if ~cond
          ~then_:(fun b ->
            debug_print "filter selected" (Tuple tup) b ;
            cb b tup )
          ~else_:(fun b -> debug_print "filter rejected" (Tuple tup) b)
          b )

  and agg_init ctx p b =
    let open Builder in
    match A.pred_remove_as p with
    | A.Count ->
        `Count
          (build_defn ~persistent:false "count" (const_int Type.PrimType.int_t 0) b)
    | A.Sum f ->
        let t = type_of_pred ctx f b in
        `Sum (f, build_defn ~persistent:false "sum" (const_int t 0) b)
    | A.Min f ->
        let t = type_of_pred ctx f b in
        `Min (f, build_defn ~persistent:false "min" (const_int t Int.max_value) b)
    | A.Max f ->
        let t = type_of_pred ctx f b in
        `Max (f, build_defn ~persistent:false "max" (const_int t Int.min_value) b)
    | A.Avg f ->
        let t = type_of_pred ctx f b in
        `Avg
          ( f
          , build_defn ~persistent:false "avg_num" (const_int t 0) b
          , build_defn ~persistent:false "avg_dem"
              (const_int Type.PrimType.int_t 0)
              b )
    | p -> `Passthru p

  and agg_step ctx b acc =
    let open Builder in
    let one = const_int Type.PrimType.int_t 1 in
    match acc with
    | `Count x -> build_assign (build_add x one b) x b
    | `Sum (f, x) -> build_assign (build_add x (gen_pred ctx f b) b) x b
    | `Min (f, x) ->
        let v = gen_pred ctx f b in
        build_assign (Ternary (build_lt v x b, v, x)) x b
    | `Max (f, x) ->
        let v = gen_pred ctx f b in
        build_assign (Ternary (build_lt v x b, x, v)) x b
    | `Avg (f, n, d) ->
        let v = gen_pred ctx f b in
        build_assign (build_add n v b) n b ;
        build_assign (build_add d one b) d b
    | `Passthru _ -> ()

  and agg_extract ctx b =
    let open Builder in
    function
    | `Count x | `Sum (_, x) | `Min (_, x) | `Max (_, x) -> x
    | `Avg (_, n, d) -> build_div n d b
    | `Passthru p ->
        [%sexp_of: A.pred * [`Agg | `Scalar]] (p, A.pred_kind p) |> print_s ;
        gen_pred ctx p b

  and scan_select ctx b r t cb =
    let open Builder in
    let args, child_layout = r in
    let child_type =
      match t with [ct], _ -> ct | _ -> failwith "Unexpected type."
    in
    match A.select_kind args with
    | `Scalar ->
        scan ctx b child_layout child_type (fun b tup ->
            let ctx =
              let child_schema = Meta.(find_exn child_layout schema) in
              Ctx.bind_ctx ctx (Ctx.of_schema child_schema tup)
            in
            cb b (List.map args ~f:(fun p -> gen_pred ctx p b)) )
    | `Agg ->
        (* Extract all the aggregates from the arguments. *)
        let scalar_preds, agg_preds = List.map ~f:collect_aggs args |> List.unzip in
        let agg_preds = List.concat agg_preds in
        let last_tup =
          build_var ~persistent:false "tup" (type_of_layout child_layout) b
        in
        let found_tup = build_defn ~persistent:false "found_tup" (Bool false) b in
        let pred_ctx =
          let child_schema = Meta.(find_exn child_layout schema) in
          Ctx.bind_ctx ctx (Ctx.of_schema child_schema (list_of_tuple last_tup b))
        in
        (* Holds the state for each aggregate. *)
        let agg_temps =
          List.map agg_preds ~f:(fun (n, p) -> (n, agg_init pred_ctx p b))
        in
        (* Compute the aggregates. *)
        scan ctx b child_layout child_type (fun b tup ->
            build_assign (Tuple tup) last_tup b ;
            List.iter agg_temps ~f:(fun (_, p) -> agg_step pred_ctx b p) ;
            build_assign (Bool true) found_tup b ) ;
        (* Extract and return aggregates. *)
        build_if ~cond:found_tup
          ~then_:(fun b ->
            let agg_temps =
              List.map agg_temps ~f:(fun (n, p) -> (n, agg_extract ctx b p))
            in
            let output_ctx =
              List.fold_left agg_temps ~init:pred_ctx ~f:(fun ctx (n, v) ->
                  Ctx.bind ctx n (type_of v b) v )
            in
            let output =
              List.map ~f:(fun p -> gen_pred output_ctx p b) scalar_preds
            in
            debug_print "select produced" (Tuple output) b ;
            cb b output )
          ~else_:(fun _ -> ())
          b

  let printer ctx r t =
    let open Builder in
    let b = create ~ctx ~name:"printer" ~ret:VoidT ~fresh in
    scan ctx b r t (fun b tup -> build_print (Tuple tup) b) ;
    build_func b

  let counter ctx r t =
    let open Builder in
    let open Infix in
    let b = create ~name:"counter" ~ctx ~ret:(IntT {nullable= false}) ~fresh in
    let c = build_defn "c" Infix.(int 0) b in
    scan ctx b r t (fun b _ -> build_assign (c + int 1) c b) ;
    build_return c b ;
    build_func b

  let irgen ~params ~data_fn r =
    let ctx =
      List.map params ~f:(fun n -> (n, Ctx.Global (Var n.Name.name)))
      |> Map.of_alist_exn (module Name.Compare_no_type)
    in
    let type_ = Abslayout_db.to_type r in
    let writer = Bitstring.Writer.with_file data_fn in
    let r, len =
      if Config.code_only then (r, 0) else Serialize.serialize writer r
    in
    Bitstring.Writer.flush writer ;
    Bitstring.Writer.close writer ;
    { iters= !iters
    ; funcs= [printer ctx r type_; counter ctx r type_]
    ; params
    ; buffer_len= len }

  let pp fmt {funcs; iters; _} =
    let open Caml.Format in
    pp_open_vbox fmt 0 ;
    List.iter (iters @ funcs) ~f:(pp_func fmt) ;
    pp_close_box fmt () ;
    pp_print_flush fmt ()
end
