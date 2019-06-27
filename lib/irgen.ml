open! Core
open Collections
open Implang
module A = Abslayout

type ir_module =
  {iters: func list; funcs: func list; params: Name.t list; buffer_len: int}
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
              | None -> Ctx.(Global Infix.(int start)) )
        | Some Many_pos | None -> ctx
    in
    ctx

  let types_of_schema s = List.map s ~f:Name.type_exn

  let type_of_schema s = Type.PrimType.TupleT (types_of_schema s)

  let type_of_layout l = A.schema_exn l |> type_of_schema

  let types_of_layout l = A.schema_exn l |> types_of_schema

  let list_of_tuple t b =
    match Builder.type_of t b with
    | TupleT ts -> List.mapi ts ~f:(fun i _ -> Infix.(index t i))
    | _ -> failwith "Expected a tuple type."

  (** True if the key is on the right side of the bound, false otherwise. *)
  let cmp ~build_open ~build_closed ~reducer bounds key b =
    List.filter_mapi bounds ~f:(fun i bnd ->
        let key_val = Index (key, i) in
        Option.map bnd ~f:(function
          | e, `Open -> build_open key_val e b
          | e, `Closed -> build_closed key_val e b ) )
    |> List.reduce ~f:reducer
    |> Option.value ~default:(Bool true)

  (** Build a binary search algorithm over indices in [0, n).

      - build_key builds a key given an index
  *)
  let build_bin_search build_key n bounds callback b =
    let open Builder in
    let lower_bounds, upper_bounds = List.unzip bounds in
    (* True if the key is above the lower bound, false otherwise. *)
    let is_above_lower =
      cmp ~build_open:build_gt ~build_closed:build_ge ~reducer:Infix.( || )
        lower_bounds
    in
    (* True if the key is below the upper bound, false otherwise. *)
    let is_below_upper =
      cmp ~build_open:build_lt ~build_closed:build_le ~reducer:Infix.( || )
        upper_bounds
    in
    (* True if the key is above the lower bound, false otherwise. *)
    let is_above_lower_tight =
      cmp ~build_open:build_gt ~build_closed:build_ge ~reducer:Infix.( && )
        lower_bounds
    in
    (* True if the key is below the upper bound, false otherwise. *)
    let is_below_upper_tight =
      cmp ~build_open:build_lt ~build_closed:build_le ~reducer:Infix.( && )
        upper_bounds
    in
    (* Binary search for the lower bound *)
    let low = build_defn "low" Infix.(int 0) b in
    let high = build_defn "high" n b in
    let print ~key idx =
      debug_print "bin_search"
        (Tuple
           [ String "lo"
           ; low
           ; String "hi"
           ; high
           ; String "idx"
           ; idx
           ; String "key"
           ; key ])
    in
    build_loop
      Infix.(low < high)
      (fun b ->
        let mid = build_defn "mid" Infix.((low + high) / int 2) b in
        let key = build_key mid b in
        build_if ~cond:(is_above_lower key b)
          ~then_:(fun b -> build_assign mid high b)
          ~else_:(fun b -> build_assign Infix.(mid + int 1) low b)
          b )
      b ;
    (* Iterate through the keys until one is found that is above the upper bound. *)
    let idx = build_defn "idx" low b in
    build_if
      ~cond:Infix.(idx < n)
      ~then_:(fun b ->
        let key = build_defn "key" (build_key idx b) b in
        build_loop
          Infix.(is_below_upper key b && idx < n)
          (fun b ->
            build_if
              ~cond:Infix.(is_below_upper_tight key b && is_above_lower_tight key b)
              ~then_:(fun b -> print ~key idx b ; callback key idx b)
              ~else_:(fun _ -> ())
              b ;
            build_assign Infix.(idx + int 1) idx b ;
            build_assign (build_key idx b) key b )
          b )
      ~else_:(fun _ -> ())
      b

  let rec scan ctx b r t (cb : callback) =
    match r.Abslayout.node with
    | As (_, r) -> scan ctx b r t cb
    | _ -> scan' ctx b r t cb

  and scan' ctx b r t (cb : callback) =
    let ctx = add_layout_start ctx r in
    match (r.node, t) with
    | Abslayout.AScalar r', Type.IntT t' -> scan_int ctx b r' t' cb
    | Abslayout.AScalar r', Type.DateT t' -> scan_date ctx b r' t' cb
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
    | DepJoin r', FuncT t' -> scan_depjoin ctx b r' t' cb
    | (Join _ | GroupBy _ | OrderBy _ | Dedup _ | Relation _ | As _), _ ->
        Error.create "Unsupported at runtime." r [%sexp_of: A.t] |> Error.raise
    | _, _ ->
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
      | A.Null _ -> Null
      | A.Int x -> Int x
      | A.String x -> String x
      | A.Fixed x -> Fixed x
      | Date x -> Date x
      | Unop (op, p) -> (
          let x = gen_pred p b in
          match op with
          | A.Not -> Infix.(not x)
          | A.Year | A.Month | A.Day ->
              Error.create "Found interval in unexpected position."
                (A.Unop (op, p))
                [%sexp_of: A.pred]
              |> Error.raise
          | A.Strlen -> Unop {op= `StrLen; arg= x}
          | A.ExtractY -> Unop {op= `ExtractY; arg= x}
          | A.ExtractM -> Unop {op= `ExtractM; arg= x}
          | A.ExtractD -> Unop {op= `ExtractD; arg= x} )
      | A.Bool x -> Bool x
      | A.As_pred (x, _) -> gen_pred x b
      | Name n -> (
        match Ctx.find ctx n b with
        | Some e -> e
        | None ->
            Error.create "Unbound variable." (n, ctx) [%sexp_of: Name.t * Ctx.t]
            |> Error.raise )
      (* Special cases for date intervals. *)
      | A.Binop (A.Add, arg1, Unop (Year, arg2)) ->
          Binop {op= `AddY; arg1= gen_pred arg1 b; arg2= gen_pred arg2 b}
      | A.Binop (A.Add, arg1, Unop (Month, arg2)) ->
          Binop {op= `AddM; arg1= gen_pred arg1 b; arg2= gen_pred arg2 b}
      | A.Binop (A.Add, arg1, Unop (Day, arg2)) ->
          Binop {op= `AddD; arg1= gen_pred arg1 b; arg2= gen_pred arg2 b}
      | A.Binop (A.Sub, arg1, Unop (Year, arg2)) ->
          let e2 = gen_pred (A.Binop (A.Sub, A.Int 0, arg2)) b in
          Binop {op= `AddY; arg1= gen_pred arg1 b; arg2= e2}
      | A.Binop (A.Sub, arg1, Unop (Month, arg2)) ->
          let e2 = gen_pred (A.Binop (A.Sub, A.Int 0, arg2)) b in
          Binop {op= `AddM; arg1= gen_pred arg1 b; arg2= e2}
      | A.Binop (A.Sub, arg1, Unop (Day, arg2)) ->
          let e2 = gen_pred (A.Binop (A.Sub, A.Int 0, arg2)) b in
          Binop {op= `AddD; arg1= gen_pred arg1 b; arg2= e2}
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
          | A.Strpos -> Binop {op= `StrPos; arg1= e1; arg2= e2} )
      | (A.Count | A.Min _ | A.Max _ | A.Sum _ | A.Avg _ | A.Row_number) as p ->
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

  and scan_int ctx b _ (Type.({nullable; range; _} as t) : Type.int_)
      (cb : callback) =
    let open Builder in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let ival = Slice (start, Type.AbsInt.byte_width ~nullable range) in
    let _nval =
      if nullable then
        let null_val = Serialize.int_sentinal t in
        Infix.(build_eq ival (int null_val) b)
      else Bool false
    in
    debug_print "int" ival b ;
    cb b [ival]

  and scan_date ctx b _ (Type.({nullable; range; _} as t) : Type.date)
      (cb : callback) =
    let open Builder in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let ival = Slice (start, Type.AbsInt.byte_width ~nullable range) in
    let _nval =
      if nullable then
        let null_val = Serialize.date_sentinal t in
        Infix.(build_eq ival (int null_val) b)
      else Bool false
    in
    let dval = Unop {op= `Int2Date; arg= ival} in
    debug_print "date" dval b ;
    cb b [dval]

  and scan_fixed ctx b _ Type.({nullable; value= {range; scale}} as t)
      (cb : callback) =
    let open Builder in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let ival = Slice (start, Type.AbsInt.byte_width ~nullable range) in
    let sval = Infix.(int scale) in
    let xval = build_div (int2fl ival) (int2fl sval) b in
    let _nval =
      if nullable then
        let null_val = Serialize.fixed_sentinal t in
        Infix.(build_eq ival (int null_val) b)
      else Bool true
    in
    debug_print "fixed" xval b ;
    cb b [xval]

  and scan_bool ctx b _ _ (cb : callback) =
    let start = Ctx.find_exn ctx (Name.create "start") b in
    cb b [Unop {op= `LoadBool; arg= start}]

  and scan_string ctx b _ (Type.({nullable; _} as st) as t) (cb : callback) =
    let open Builder in
    let hdr = Header.make_header (StringT t) in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let value_ptr = Header.make_position hdr "value" start in
    let nchars = Header.make_access hdr "nchars" start in
    let xval = Binop {op= `LoadStr; arg1= value_ptr; arg2= nchars} in
    let _nval =
      if nullable then
        let null_val = Serialize.string_sentinal st in
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
              make_loops ctx (fields @ tup) clayouts ctypes cstarts b )
      | _ -> failwith ""
    in
    let hdr = Header.make_header (TupleT t) in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let child_starts =
      let _, ret =
        List.fold_left child_types
          ~init:(Header.make_position hdr "value" start, RevList.empty)
          ~f:(fun (cstart, ret) ctype ->
            let cstart = build_defn "cstart" cstart b in
            let next_cstart = Infix.(cstart + len cstart ctype) in
            (next_cstart, RevList.(ret ++ cstart)) )
      in
      RevList.to_list ret
    in
    debug_print "scanning crosstuple" (Int 0) b ;
    make_loops ctx [] child_layouts child_types child_starts b

  and scan_ziptuple ctx b r t cb =
    let open Builder in
    let child_layouts, _ = r in
    let child_types, _ = t in
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
               create ~ctx:callee_ctx
                 ~name:(Fresh.name Global.fresh "zt_%d")
                 ~ret:(type_of_layout callee_layout)
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
    match Type.count (TupleT t) |> Type.AbsInt.to_int with
    | Some x -> build_count_loop Infix.(int x) build_body b
    | None ->
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
    List.iter2_exn child_layouts child_types ~f:(fun child_layout child_type ->
        let clen = len cstart child_type in
        scan ctx b child_layout child_type cb ;
        build_assign Infix.(cstart + clen) cstart b )

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
    let key_layout = A.h_key_layout r in
    let key_type, value_type, m = t in
    let hdr = Header.make_header (HashIdxT t) in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let kstart = build_var ~persistent:false "kstart" Type.PrimType.int_t b in
    let vstart = build_var ~persistent:false "vstart" Type.PrimType.int_t b in
    let key_tuple =
      build_var ~persistent:false "key" (type_of_layout key_layout) b
    in
    let key_ctx = Ctx.bind ctx "start" Type.PrimType.int_t kstart in
    let value_ctx =
      let key_schema = A.schema_exn key_layout |> Schema.scoped r.A.hi_scope in
      let ctx =
        Ctx.bind_ctx ctx (Ctx.of_schema key_schema (list_of_tuple key_tuple b))
      in
      Ctx.bind ctx "start" Type.PrimType.int_t vstart
    in
    let hash_data_start = Header.make_position hdr "hash_data" start in
    let mapping_start = Header.make_position hdr "hash_map" start in
    let mapping_len = Header.make_access hdr "hash_map_len" start in
    let hash_ptr_len = Type.hi_ptr_size key_type value_type m in
    let lookup_expr = List.map r.hi_lookup ~f:(fun p -> gen_pred ctx p b) in
    debug_print "scanning hash idx" start b ;
    (* Compute the index in the mapping table for this key. *)
    let hash_key =
      match (Type.(hash_kind_exn (HashIdxT t)), lookup_expr) with
      | _, [] -> failwith "empty hash key"
      | `Direct, [x] -> build_to_int x b
      | `Direct, _ -> failwith "Unexpected direct hash."
      | `Cmph, [x] -> build_hash hash_data_start x b
      | `Cmph, xs -> build_hash hash_data_start (Tuple xs) b
    in
    debug_print "hashing key" (Tuple lookup_expr) b ;
    let hash_key = Infix.(hash_key * int hash_ptr_len) in
    (* Get a pointer to the value. *)
    let value_ptr =
      Infix.(
        Slice (mapping_start + hash_key, hash_ptr_len)
        + Header.make_position hdr "data" start)
    in
    debug_print "value ptr is" value_ptr b ;
    (* If the pointer is null, then the key is not present. *)
    build_if
      ~cond:
        Infix.(
          hash_key < int 0 || hash_key >= mapping_len
          (* || build_eq value_ptr (int 0x0) b *))
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
            scan value_ctx b r.hi_values value_type (fun b value_tup ->
                cb b (list_of_tuple key_tuple b @ value_tup) ) )
          ~else_:(fun _ -> ())
          b )
      b

  and scan_ordered_idx ctx b r t cb =
    let open Builder in
    let _, value_layout, m = r in
    let key_type, value_type, mt = t in
    let key_layout =
      Option.value_exn ~message:"No ordered idx key layout." m.oi_key_layout
    in
    let hdr = Header.make_header (OrderedIdxT t) in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let kstart = build_var "kstart" Type.PrimType.int_t b in
    let vstart = build_var "vstart" Type.PrimType.int_t b in
    let key_tuple = build_var "key" (type_of_layout key_layout) b in
    let key_ctx = Ctx.bind ctx "start" Type.PrimType.int_t kstart in
    let value_ctx =
      let key_schema = A.schema_exn key_layout in
      let ctx =
        Ctx.bind_ctx ctx (Ctx.of_schema key_schema (list_of_tuple key_tuple b))
      in
      Ctx.bind ctx "start" Type.PrimType.int_t vstart
    in
    let index_len = Header.make_access hdr "idx_len" start in
    let index_start = Header.make_position hdr "idx" start in
    let key_len = len index_start key_type in
    let ptr_size = Type.oi_ptr_size value_type mt in
    let ptr_len = Infix.(int ptr_size) in
    let kp_len = Infix.(key_len + ptr_len) in
    let key_start i = Infix.(index_start + (i * kp_len)) in
    let ptr_start i = Infix.(key_start i + key_len) in
    let read_ptr i = Slice (ptr_start i, ptr_size) in
    let key_index i b =
      build_assign (key_start i) kstart b ;
      let key = build_var ~persistent:false "key" (type_of_layout key_layout) b in
      scan key_ctx b key_layout key_type (fun b tup ->
          build_assign (Tuple tup) key b ) ;
      key
    in
    let n = Infix.(index_len / kp_len) in
    debug_print "scanning ordered idx" start b ;
    let bounds =
      let mk_bound = Option.map ~f:(fun (p, bnd) -> (gen_pred ctx p b, bnd)) in
      List.map m.oi_lookup ~f:(fun (lb, ub) -> (mk_bound lb, mk_bound ub))
    in
    build_bin_search key_index n bounds
      (fun key idx b ->
        build_assign Infix.(read_ptr idx + index_len + index_start) vstart b ;
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
          let child_schema = A.schema_exn child_layout in
          Ctx.bind_ctx ctx (Ctx.of_schema child_schema tup)
        in
        let cond = gen_pred ctx pred b in
        build_if ~cond
          ~then_:(fun b ->
            debug_print
              (Format.asprintf "filter %a selected" Pred.pp pred)
              (Tuple tup) b ;
            cb b tup )
          ~else_:(fun b ->
            debug_print
              (Format.asprintf "filter %a rejected" Pred.pp pred)
              (Tuple tup) b )
          b )

  and agg_init ctx p b =
    let open Builder in
    match Pred.remove_as p with
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
    | `Passthru p -> gen_pred ctx p b

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
              let child_schema = A.schema_exn child_layout in
              Ctx.bind_ctx ctx (Ctx.of_schema child_schema tup)
            in
            cb b (List.map args ~f:(fun p -> gen_pred ctx p b)) )
    | `Agg ->
        (* Extract all the aggregates from the arguments. *)
        let scalar_preds, agg_preds =
          List.map ~f:Pred.collect_aggs args |> List.unzip
        in
        let agg_preds = List.concat agg_preds in
        let last_tup =
          build_var ~persistent:false "tup" (type_of_layout child_layout) b
        in
        let found_tup = build_defn ~persistent:false "found_tup" (Bool false) b in
        let pred_ctx =
          let child_schema = A.schema_exn child_layout in
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

  and scan_depjoin ctx b {d_lhs; d_alias; d_rhs} (child_types, _) (cb : callback) =
    let lhs_t, rhs_t =
      match child_types with
      | [lhs_t; rhs_t] -> (lhs_t, rhs_t)
      | _ -> failwith "Unexpected type."
    in
    let hdr = Header.make_header (TupleT ([lhs_t; rhs_t], {kind= `Cross})) in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let lhs_start = Header.make_position hdr "value" start in
    let lhs_ctx = Ctx.bind ctx "start" Type.PrimType.int_t lhs_start in
    let rhs_ctx =
      let rhs_start = Infix.(lhs_start + len lhs_start lhs_t) in
      debug_print "lhs_start" lhs_start b ;
      debug_print "lhs_len" (len lhs_start lhs_t) b ;
      debug_print "rhs_start" rhs_start b ;
      Ctx.bind ctx "start" Type.PrimType.int_t rhs_start
    in
    debug_print "scanning depjoin" (Int 0) b ;
    scan lhs_ctx b d_lhs lhs_t (fun b tup ->
        let rhs_ctx =
          let lhs_ctx =
            Ctx.of_schema (A.schema_exn d_lhs |> Schema.scoped d_alias) tup
          in
          Ctx.bind_ctx rhs_ctx lhs_ctx
        in
        scan rhs_ctx b d_rhs rhs_t cb )

  let printer ctx r t =
    let open Builder in
    let b = create ~ctx ~name:"printer" ~ret:VoidT in
    scan ctx b r t (fun b tup -> build_print (Tuple tup) b) ;
    build_func b

  let consumer ctx r t =
    let open Builder in
    let b = create ~name:"consumer" ~ctx ~ret:VoidT in
    scan ctx b r t (fun b tup -> build_consume (Tuple tup) b) ;
    build_func b

  let irgen ~params ~data_fn r =
    let ctx =
      List.map params ~f:(fun n -> (n, Ctx.Global (Var (Name.name n))))
      |> Ctx.of_alist_exn
    in
    let type_ = Meta.(find_exn r type_) in
    let r, len =
      if Config.code_only then (r, 0) else Serialize.serialize data_fn r
    in
    { iters= !iters
    ; funcs= [printer ctx r type_; consumer ctx r type_]
    ; params
    ; buffer_len= len }

  let pp fmt {funcs; iters; _} =
    let open Caml.Format in
    pp_open_vbox fmt 0 ;
    List.iter (iters @ funcs) ~f:(pp_func fmt) ;
    pp_close_box fmt () ;
    pp_print_flush fmt ()
end
