open Core
open Base
open Collections
open Implang
module A = Abslayout

type ir_module =
  {iters: func list; funcs: func list; params: Name.Compare.t list; buffer_len: int}
[@@deriving compare, sexp]

type scan_args =
  { ctx: Ctx.t
  ; name: string
  ; scan: Ctx.t -> Abslayout.t -> Type.t -> func
  ; layout: Abslayout.t
  ; type_: Type.t }

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

module Make (Config : Config.S) (Eval : Eval.S) (Serialize : Serialize.S) () =
struct
  module Abslayout_db = Abslayout_db.Make (Eval)

  let fresh = Fresh.create ()

  let funcs = ref []

  let add_func (f : func) = funcs := f :: !funcs

  let debug_print msg v b =
    let open Builder in
    if Config.debug then build_print (Tuple [String msg; v]) b

  let gen_pred ~ctx ~scan pred b =
    let open Builder in
    let rec gen_pred = function
      | A.Null -> Null
      | A.Int x -> Int x
      | A.String x -> String x
      | A.Fixed x -> Fixed x
      | Date x -> Int (Date.to_int x)
      | Unop (op, p) -> (
          let x = gen_pred p in
          match op with
          | A.Not -> Infix.(not x)
          | A.Year -> Infix.(int 365 * x)
          | A.Month -> Infix.(int 30 * x)
          | A.Day -> x )
      | A.Bool x -> Bool x
      | A.As_pred (x, _) -> gen_pred x
      | Name n -> (
        match Ctx.find ctx n b with
        | Some e -> e
        | None ->
            Error.create "Unbound variable." (n, ctx) [%sexp_of: Name.t * Ctx.t]
            |> Error.raise )
      | A.Binop (op, arg1, arg2) -> (
          let e1 = gen_pred arg1 in
          let e2 = gen_pred arg2 in
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
          | A.Mod -> Infix.(e1 % e2) )
      | (A.Count | A.Min _ | A.Max _ | A.Sum _ | A.Avg _) as p ->
          Error.create "Not a scalar predicate." p [%sexp_of: A.pred] |> Error.raise
      | A.If (p1, p2, p3) -> Ternary (gen_pred p1, gen_pred p2, gen_pred p3)
      | A.First callee_layout ->
          (* Don't use the passed in start value. Subquery layouts are not stored
           inline. *)
          let ctx = Map.remove ctx (Name.create "start") in
          let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
          let callee_type = Meta.(find_exn callee_layout type_) in
          let callee = scan callee_ctx callee_layout callee_type in
          build_iter callee callee_args b ;
          let tup = build_var "tup" callee.ret_type b in
          build_step tup callee b ;
          Infix.(index tup 0)
      | A.Exists callee_layout ->
          let ctx = Map.remove ctx (Name.create "start") in
          let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
          let callee_type = Meta.(find_exn callee_layout type_) in
          let callee = scan callee_ctx callee_layout callee_type in
          build_iter callee callee_args b ;
          let tup = build_var "tup" callee.ret_type b in
          build_step tup callee b ;
          Infix.(not (Done callee.name))
    in
    gen_pred pred

  (** Generate an expression for the length of a value at `start` with type
     `type_`. Compensates for restrictions in the Header api. *)
  let rec len start type_ =
    let hdr = Header.make_header type_ in
    match type_ with
    | StringT _ ->
        Infix.(
          int (Header.size_exn hdr "nchars") + Header.make_access hdr "nchars" start)
    | FuncT ([t], _) -> len start t
    | _ ->
        Logs.debug (fun m -> m "len for %a" Sexp.pp_hum ([%sexp_of: Type.t] type_)) ;
        Header.make_access hdr "len" start

  let scan_empty {ctx; name; _} =
    let open Builder in
    let b = create ~ctx ~name ~ret:(Type.PrimType.TupleT []) ~fresh in
    build_func b

  let scan_null {ctx; name; _} =
    let open Builder in
    let b = create ~ctx ~name ~ret:Type.PrimType.NullT ~fresh in
    build_yield Null b ; build_func b

  let scan_int args =
    match args with
    | {ctx; name; type_= IntT {nullable; range= (_, h) as range}; _} ->
        let open Builder in
        let b =
          let ret_type = Type.PrimType.TupleT [IntT {nullable}] in
          create ~ctx ~name ~ret:ret_type ~fresh
        in
        let start = Ctx.find_exn ctx (Name.create "start") b in
        let ival = Slice (start, Type.AbsInt.byte_width ~nullable range) in
        let _nval =
          if nullable then
            let null_val = h + 1 in
            Infix.(build_eq ival (int null_val) b)
          else Bool false
        in
        debug_print "int" ival b ;
        build_yield (Tuple [ival]) b ;
        build_func b
    | _ -> failwith "Unexpected args."

  let scan_fixed args =
    match args with
    | {ctx; name; type_= FixedT {nullable; range= (_, h) as range; scale}; _} ->
        let open Builder in
        let b =
          let ret_type = Type.PrimType.TupleT [FixedT {nullable}] in
          create ~ctx ~name ~ret:ret_type ~fresh
        in
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
        build_yield (Tuple [xval]) b ;
        build_func b
    | _ -> failwith "Unexpected args."

  let scan_bool args =
    match args with
    | {ctx; name; type_= BoolT meta; _} ->
        let open Builder in
        let b =
          create ~ctx ~name ~ret:(TupleT [BoolT {nullable= meta.nullable}]) ~fresh
        in
        let start = Ctx.find_exn ctx (Name.create "start") b in
        let ival = Slice (start, 1) in
        let _nval =
          if meta.nullable then
            let null_val = 2 in
            Infix.(build_eq ival (int null_val) b)
          else Bool true
        in
        debug_print "bool" ival b ;
        build_yield (Tuple [ival]) b ;
        build_func b
    | _ -> failwith "Unexpected args."

  let scan_string args =
    match args with
    | {ctx; name; type_= StringT {nchars= l, _; nullable; _} as t; _} ->
        let open Builder in
        let hdr = Header.make_header t in
        let b = create ~ctx ~name ~ret:(TupleT [StringT {nullable}]) ~fresh in
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
        build_yield (Tuple [xval]) b ;
        build_func b
    | _ -> failwith "Unexpected args."

  [@@@warning "-8"]

  let scan_crosstuple
      A.({ ctx
         ; scan
         ; layout= {node= ATuple (child_layouts, Cross); _} as r
         ; type_= TupleT (child_types, _) as t
         ; name }) =
    let open Builder in
    let rec make_loops ctx tuples clayouts ctypes cstarts b =
      match (clayouts, ctypes, cstarts) with
      | [], [], [] ->
          let tup = build_concat (List.rev tuples) b in
          build_yield tup b
      | clayout :: clayouts, ctype :: ctypes, cstart :: cstarts ->
          let ctx = Ctx.bind ctx "start" Type.PrimType.int_t cstart in
          let child_ctx, child_args = Ctx.make_callee_context ctx b in
          let child_iter = scan child_ctx clayout ctype in
          build_foreach ~persistent:true ~count:(Type.count ctype) child_iter
            child_args
            (fun tup b ->
              let next_ctx =
                let tuple_ctx = Ctx.of_schema Meta.(find_exn clayout schema) tup in
                Ctx.bind_ctx ctx tuple_ctx
              in
              make_loops next_ctx (tup :: tuples) clayouts ctypes cstarts b )
            b
    in
    let b =
      let ret_type =
        let schema = Meta.(find_exn r schema) in
        Type.PrimType.TupleT (List.map schema ~f:Name.type_exn)
      in
      create ~ctx ~name ~ret:ret_type ~fresh
    in
    let hdr = Header.make_header t in
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
    make_loops ctx [] child_layouts child_types child_starts b ;
    build_func b

  let scan_ziptuple
      A.({ ctx
         ; name
         ; scan
         ; layout= {node= ATuple (child_layouts, Zip); _} as r
         ; type_= TupleT (child_types, {count}) as t; _ }) =
    let open Builder in
    let b =
      let ret_type =
        let schema = Meta.(find_exn r schema) in
        Type.PrimType.TupleT (List.map schema ~f:Name.type_exn)
      in
      create ~ctx ~name ~ret:ret_type ~fresh
    in
    let hdr = Header.make_header t in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let ctx = Ctx.bind ctx "start" Type.PrimType.int_t start in
    let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
    (* Build iterator initializers using the computed start positions. *)
    build_assign (Header.make_position hdr "value" start) start b ;
    let callee_funcs =
      List.map2_exn child_layouts child_types ~f:(fun callee_layout callee_type ->
          let callee = scan callee_ctx callee_layout callee_type in
          build_iter callee callee_args b ;
          build_assign Infix.(start + len start callee_type) start b ;
          callee )
    in
    let child_tuples =
      List.map callee_funcs ~f:(fun f -> build_var "t" f.ret_type b)
    in
    let build_body b =
      List.iter2_exn callee_funcs child_tuples ~f:(fun f t -> build_step t f b) ;
      let tup =
        List.map2_exn child_types child_tuples ~f:(fun in_t child_tup ->
            List.init (Type.width in_t) ~f:(fun i -> Infix.(index child_tup i)) )
        |> List.concat
        |> fun l -> Tuple l
      in
      build_yield tup b
    in
    ( match Type.AbsCount.kind count with
    | `Count x -> build_count_loop Infix.(int x) build_body b
    | `Countable | `Unknown ->
        build_body b ;
        let not_done =
          List.fold_left callee_funcs ~init:(Bool true) ~f:(fun acc f ->
              Infix.(acc && not (Done f.name)) )
        in
        build_loop not_done build_body b ) ;
    build_func b

  let scan_tuple (A.({layout= {node= ATuple (_, kind); _}; _}) as args) =
    match kind with Cross -> scan_crosstuple args | Zip -> scan_ziptuple args

  let scan_unordered_list
      A.({ ctx
         ; name
         ; scan
         ; layout= {node= AList (_, child_layout); _} as r
         ; type_= ListT (child_type, _) as t; _ }) =
    let open Builder in
    let b =
      let ret_type =
        let schema = Meta.(find_exn r schema) in
        Type.PrimType.TupleT (List.map schema ~f:Name.type_exn)
      in
      create ~ctx ~name ~ret:ret_type ~fresh
    in
    let hdr = Header.make_header t in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let cstart = build_defn "cstart" (Header.make_position hdr "value" start) b in
    let callee_ctx, callee_args =
      let ctx = Ctx.bind ctx "start" Type.PrimType.int_t cstart in
      Ctx.make_callee_context ctx b
    in
    let func = scan callee_ctx child_layout child_type in
    let pcount = build_defn "pcount" (Header.make_access hdr "count" start) b in
    build_loop
      Infix.(pcount > int 0)
      (fun b ->
        let clen = len cstart child_type in
        build_foreach ~persistent:false ~count:(Type.count child_type) func
          callee_args build_yield b ;
        build_assign Infix.(cstart + clen) cstart b ;
        build_assign Infix.(pcount - int 1) pcount b )
      b ;
    build_func b

  let scan_hash_idx
      A.({ ctx
         ; name
         ; scan
         ; layout=
             { node=
                 AHashIdx (_, value_layout, {lookup; hi_key_layout= Some key_layout}); _
             } as r
         ; type_= HashIdxT (key_type, value_type, _) as t; _ }) =
    let open Builder in
    let b =
      let ret_type =
        let schema = Meta.(find_exn r schema) in
        Type.PrimType.TupleT (List.map schema ~f:Name.type_exn)
      in
      create ~ctx ~name ~ret:ret_type ~fresh
    in
    let hdr = Header.make_header t in
    let start = Ctx.find_exn ctx (Name.create "start") b in
    let kstart = build_var ~persistent:false "kstart" Type.PrimType.int_t b in
    let key_callee_ctx, key_callee_args =
      let ctx = Ctx.bind ctx "start" Type.PrimType.int_t kstart in
      Ctx.make_callee_context ctx b
    in
    let key_iter = scan key_callee_ctx key_layout key_type in
    let key_tuple = build_var ~persistent:false "key" key_iter.ret_type b in
    let ctx =
      let key_schema = Meta.(find_exn key_layout schema) in
      Ctx.bind_ctx ctx (Ctx.of_schema key_schema key_tuple)
    in
    let vstart = build_var ~persistent:false "vstart" Type.PrimType.int_t b in
    let value_callee_ctx, value_callee_args =
      let ctx = Ctx.bind ctx "start" Type.PrimType.int_t vstart in
      Ctx.make_callee_context ctx b
    in
    let value_iter = scan value_callee_ctx value_layout value_type in
    let hash_data_start = Header.make_position hdr "hash_data" start in
    let mapping_start = Header.make_position hdr "hash_map" start in
    let mapping_len = Header.make_access hdr "hash_map_len" start in
    let lookup_expr = List.map lookup ~f:(fun p -> gen_pred ~scan ~ctx p b) in
    (* Compute the index in the mapping table for this key. *)
    let hash_key =
      match lookup_expr with
      | [] -> failwith "empty hash key"
      | [x] -> build_hash hash_data_start x b
      | xs -> build_hash hash_data_start (Tuple xs) b
    in
    let hash_key = Infix.(hash_key * int 8) in
    (* Get a pointer to the value. *)
    let value_ptr = Infix.(Slice (mapping_start + hash_key, 8)) in
    (* If the pointer is null, then the key is not present. *)
    build_if
      ~cond:
        Infix.(
          hash_key < int 0
          || hash_key >= mapping_len
          || build_eq value_ptr (int 0x0) b)
      ~then_:(fun _ -> ())
      ~else_:(fun b ->
        build_assign value_ptr kstart b ;
        build_iter key_iter key_callee_args b ;
        build_step key_tuple key_iter b ;
        build_assign Infix.(value_ptr + len value_ptr key_type) vstart b ;
        build_if
          ~cond:(build_eq key_tuple (Tuple lookup_expr) b)
          ~then_:
            (build_foreach ~persistent:false ~count:(Type.count value_type)
               value_iter value_callee_args (fun value_tup b ->
                 build_yield (build_concat [key_tuple; value_tup] b) b ))
          ~else_:(fun _ -> ())
          b )
      b ;
    build_func b

  [@@@warning "+8"]

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

  let scan_ordered_idx args =
    match args with
    | { ctx
      ; name
      ; scan
      ; layout=
          { node=
              AOrderedIdx
                ( _
                , value_layout
                , {oi_key_layout= Some key_layout; lookup_low; lookup_high; _} ); _
          } as r
      ; type_= OrderedIdxT (key_type, value_type, _) as t; _ } ->
        let open Builder in
        let hdr = Header.make_header t in
        let b =
          let ret_type =
            let schema = Meta.(find_exn r schema) in
            Type.PrimType.TupleT (List.map schema ~f:Name.type_exn)
          in
          create ~ctx ~name ~ret:ret_type ~fresh
        in
        let start = Ctx.find_exn ctx (Name.create "start") b in
        let kstart = build_var "kstart" Type.PrimType.int_t b in
        let key_callee_ctx, key_callee_args =
          let ctx = Ctx.bind ctx "start" Type.PrimType.int_t kstart in
          Ctx.make_callee_context ctx b
        in
        let key_iter = scan key_callee_ctx key_layout key_type in
        let key_tuple = build_var "key" key_iter.ret_type b in
        let ctx =
          let key_schema = Meta.(find_exn key_layout schema) in
          Ctx.bind_ctx ctx (Ctx.of_schema key_schema key_tuple)
        in
        let vstart = build_var "vstart" Type.PrimType.int_t b in
        let value_callee_ctx, value_callee_args =
          let ctx = Ctx.bind ctx "start" Type.PrimType.int_t vstart in
          Ctx.make_callee_context ctx b
        in
        let value_iter = scan value_callee_ctx value_layout value_type in
        let index_len = Header.make_access hdr "idx_len" start in
        let index_start = Header.make_position hdr "idx" start in
        let key_len = len index_start key_type in
        let ptr_len = Infix.(int 8) in
        let kp_len = Infix.(key_len + ptr_len) in
        let key_index i b =
          build_assign Infix.(index_start + (i * kp_len)) kstart b ;
          build_iter key_iter key_callee_args b ;
          let key = build_var ~persistent:false "key" key_iter.ret_type b in
          build_step key key_iter b ; key
        in
        let n = Infix.(index_len / kp_len) in
        build_bin_search key_index n
          (gen_pred ~scan ~ctx lookup_low b)
          (gen_pred ~scan ~ctx lookup_high b)
          (fun key idx b ->
            build_assign
              Infix.(Slice (index_start + (idx * kp_len) + key_len, 8))
              vstart b ;
            build_assign key key_tuple b ;
            build_foreach ~persistent:false ~count:(Type.count value_type)
              value_iter value_callee_args
              (fun value b -> build_yield (build_concat [key; value] b) b)
              b )
          b ;
        build_func b
    | _ -> failwith "Unexpected args."

  let printer ctx name func =
    let open Builder in
    let b = create ~ctx ~name ~ret:VoidT ~fresh in
    build_foreach ~persistent:false func [] (fun x b -> build_print x b) b ;
    build_func b

  let counter ctx name func =
    let open Builder in
    let open Infix in
    let b = create ~name ~ctx ~ret:(IntT {nullable= false}) ~fresh in
    let c = build_defn "c" Infix.(int 0) b in
    build_foreach ~persistent:false func []
      (fun _ b -> build_assign (c + int 1) c b)
      b ;
    build_return c b ;
    build_func b

  let scan_filter args =
    match args with
    | A.({ ctx
         ; name
         ; scan
         ; layout= {node= Filter (pred, child_layout); _} as r
         ; type_= FuncT ([child_type], _); _ }) ->
        let open Builder in
        let b =
          let ret_type =
            let schema = Meta.(find_exn r schema) in
            Type.PrimType.TupleT (List.map schema ~f:Name.type_exn)
          in
          create ~ctx ~name ~ret:ret_type ~fresh
        in
        let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
        let func = scan callee_ctx child_layout child_type in
        build_foreach ~persistent:false ~count:(Type.count child_type) func
          callee_args
          (fun tup b ->
            let ctx =
              let child_schema = Meta.(find_exn child_layout schema) in
              Ctx.bind_ctx ctx (Ctx.of_schema child_schema tup)
            in
            let cond = gen_pred ~scan ~ctx pred b in
            build_if ~cond
              ~then_:(fun b ->
                debug_print "filter selected" tup b ;
                build_yield tup b )
              ~else_:(fun _ -> ())
              b )
          b ;
        build_func b
    | _ -> failwith "Unexpected args."

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

  let agg_init scan ctx p b =
    let open Builder in
    match A.pred_remove_as p with
    | A.Count ->
        `Count
          (build_defn ~persistent:false "count" (const_int Type.PrimType.int_t 0) b)
    | A.Sum f ->
        let f = gen_pred ~scan ~ctx f b in
        let t = type_of f b in
        `Sum (f, build_defn ~persistent:false "sum" (const_int t 0) b)
    | A.Min f ->
        let f = gen_pred ~scan ~ctx f b in
        let t = type_of f b in
        `Min (f, build_defn ~persistent:false "min" (const_int t Int.max_value) b)
    | A.Max f ->
        let f = gen_pred ~scan ~ctx f b in
        let t = type_of f b in
        `Max (f, build_defn ~persistent:false "max" (const_int t Int.min_value) b)
    | A.Avg f ->
        let f = gen_pred ~scan ~ctx f b in
        let t = type_of f b in
        `Avg
          ( f
          , build_defn ~persistent:false "avg_num" (const_int t 0) b
          , build_defn ~persistent:false "avg_dem"
              (const_int Type.PrimType.int_t 0)
              b )
    | p -> `Passthru p

  let agg_step b acc =
    let open Builder in
    let one = const_int Type.PrimType.int_t 1 in
    match acc with
    | `Count x -> build_assign (build_add x one b) x b
    | `Sum (f, x) -> build_assign (build_add x f b) x b
    | `Min (v, x) -> build_assign (Ternary (build_lt v x b, v, x)) x b
    | `Max (v, x) -> build_assign (Ternary (build_lt v x b, x, v)) x b
    | `Avg (v, n, d) ->
        build_assign (build_add n v b) n b ;
        build_assign (build_add d one b) d b
    | `Passthru _ -> ()

  let agg_extract scan ctx b =
    let open Builder in
    function
    | `Count x | `Sum (_, x) | `Min (_, x) | `Max (_, x) -> x
    | `Avg (_, n, d) -> build_div n d b
    | `Passthru p ->
        [%sexp_of: A.pred * [`Agg | `Scalar]] (p, A.pred_kind p) |> print_s ;
        gen_pred ~scan ~ctx p b

  let scan_select args =
    match args with
    | { ctx
      ; name
      ; scan
      ; layout= {node= Select (args, child_layout); _} as layout
      ; type_= FuncT ([child_type], _); _ } ->
        let open Builder in
        let schema = Meta.(find_exn layout schema) in
        let b =
          let ret_type = Type.PrimType.TupleT (List.map schema ~f:Name.type_exn) in
          create ~ctx ~name ~ret:ret_type ~fresh
        in
        let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
        let func = scan callee_ctx child_layout child_type in
        ( match A.select_kind args with
        | `Scalar ->
            build_foreach ~persistent:false ~count:(Type.count child_type) func
              callee_args
              (fun tup b ->
                let ctx =
                  let child_schema = Meta.(find_exn child_layout schema) in
                  Ctx.bind_ctx ctx (Ctx.of_schema child_schema tup)
                in
                let output = List.map args ~f:(fun p -> gen_pred ~scan ~ctx p b) in
                build_yield (Tuple output) b )
              b
        | `Agg ->
            (* Extract all the aggregates from the arguments. *)
            let scalar_preds, agg_preds =
              List.map ~f:collect_aggs args |> List.unzip
            in
            let agg_preds = List.concat agg_preds in
            let agg_temps = ref [] in
            let last_tup = build_var ~persistent:false "tup" func.ret_type b in
            let found_tup =
              build_defn ~persistent:false "found_tup" (Bool false) b
            in
            (* Holds the state for each aggregate. *)
            build_foreach ~persistent:false ~count:(Type.count child_type)
              ~header:(fun tup b ->
                let ctx =
                  let child_schema = Meta.(find_exn child_layout schema) in
                  Ctx.bind_ctx ctx (Ctx.of_schema child_schema tup)
                in
                agg_temps :=
                  List.map agg_preds ~f:(fun (n, p) -> (n, agg_init scan ctx p b))
                )
              func callee_args
              (fun tup b ->
                List.iter !agg_temps ~f:(fun (_, p) -> agg_step b p) ;
                build_assign tup last_tup b ;
                build_assign (Bool true) found_tup b )
              b ;
            build_if ~cond:found_tup
              ~then_:(fun b ->
                let ctx =
                  let child_schema = Meta.(find_exn child_layout schema) in
                  Ctx.bind_ctx ctx (Ctx.of_schema child_schema last_tup)
                in
                let agg_temps =
                  List.map !agg_temps ~f:(fun (n, p) -> (n, agg_extract scan ctx b p)
                  )
                in
                let footer_ctx =
                  List.fold_left agg_temps ~init:ctx ~f:(fun ctx (n, v) ->
                      Ctx.bind ctx n (type_of v b) v )
                in
                let output =
                  Tuple
                    (List.map
                       ~f:(fun p -> gen_pred ~ctx:footer_ctx ~scan p b)
                       scalar_preds)
                in
                debug_print "select produced" output b ;
                build_yield output b )
              ~else_:(fun _ -> ())
              b ) ;
        build_func b
    | _ -> failwith "Unexpected args."

  let gen_abslayout ~ctx ~data_fn r =
    let type_ = Abslayout_db.to_type r in
    let writer = Bitstring.Writer.with_file data_fn in
    let r, len =
      if Config.code_only then (r, 0) else Serialize.serialize writer type_ r
    in
    Bitstring.Writer.flush writer ;
    Bitstring.Writer.close writer ;
    let rec gen_func ctx r t =
      let name = A.name r ^ "_" ^ Fresh.name fresh "%d" in
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
      let scan_args = {ctx; name; layout= r; type_= t; scan} in
      let func =
        match t with
        | Type.IntT _ -> scan_int scan_args
        | FixedT _ -> scan_fixed scan_args
        | BoolT _ -> scan_bool scan_args
        | StringT _ -> scan_string scan_args
        | EmptyT -> scan_empty scan_args
        | NullT -> scan_null scan_args
        | TupleT _ -> scan_tuple scan_args
        | ListT _ -> scan_unordered_list scan_args
        | HashIdxT _ -> scan_hash_idx scan_args
        | OrderedIdxT _ -> scan_ordered_idx scan_args
        | FuncT _ -> (
          match r.node with
          | Select _ -> scan_select scan_args
          | Filter _ -> scan_filter scan_args
          | Join _ | GroupBy (_, _, _) | OrderBy _ | Dedup _ | Scan _ | As (_, _) ->
              Error.create "Unsupported at runtime." r [%sexp_of: A.t]
              |> Error.raise
          | AEmpty | AScalar _ | AList _ | ATuple _ | AHashIdx _ | AOrderedIdx _ ->
              Error.create "Not functions." r [%sexp_of: A.t] |> Error.raise )
      in
      add_func func ; func
    and scan ctx r t =
      match r.node with As (_, r) -> scan ctx r t | _ -> gen_func ctx r t
    in
    (scan ctx r type_, len)

  let irgen ~params ~data_fn r =
    let ctx =
      List.map params ~f:(fun n -> (n, Ctx.Global (Var n.Name.name)))
      |> Map.of_alist_exn (module Name.Compare_no_type)
    in
    let top_func, len = gen_abslayout ~ctx ~data_fn r in
    { iters= List.rev !funcs
    ; funcs= [printer ctx "printer" top_func; counter ctx "counter" top_func]
    ; params
    ; buffer_len= len }

  let pp fmt {funcs; iters; _} =
    let open Caml.Format in
    pp_open_vbox fmt 0 ;
    List.iter (iters @ funcs) ~f:(pp_func fmt) ;
    pp_close_box fmt () ;
    pp_print_flush fmt ()
end
