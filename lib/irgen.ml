open Core
open Base
open Collections
open Implang
module A = Abslayout

type ir_module =
  {iters: func list; funcs: func list; params: A.Name.t list; buffer_len: int}
[@@deriving sexp]

type scan_args =
  { ctx: Ctx.t
  ; name: string
  ; scan: Ctx.t -> Abslayout.t -> Type.t -> func
  ; layout: Abslayout.t
  ; type_: Type.t }

exception IRGenError of Error.t [@@deriving sexp]

module Config = struct
  module type S = sig
    val code_only : bool
  end
end

module type S = sig
  val irgen :
    params:Abslayout.Name.t list -> data_fn:string -> Abslayout.t -> ir_module

  val pp : Formatter.t -> ir_module -> unit
end

module Make (Config : Config.S) (Eval : Eval.S) (Serialize : Serialize.S) () =
struct
  module Abslayout_db = Abslayout_db.Make (Eval)

  let fresh = Fresh.create ()

  let funcs = ref []

  let add_func (f : func) = funcs := f :: !funcs

  let gen_pred ~ctx pred b =
    let rec gen_pred = function
      | A.Null -> Null
      | A.Int x -> Int x
      | A.String x -> String x
      | A.Bool x -> Bool x
      | A.As_pred (x, _) -> gen_pred x
      | A.Name n -> (
        match Ctx.find ctx n b with
        | Some e -> e
        | None ->
            Error.create "Unbound variable." (n, ctx) [%sexp_of: A.Name.t * Ctx.t]
            |> Error.raise )
      | A.Binop (op, arg1, arg2) -> (
          let e1 = gen_pred arg1 in
          let e2 = gen_pred arg2 in
          match op with
          | A.Eq -> Infix.(e1 = e2)
          | A.Lt -> Infix.(e1 < e2)
          | A.Le -> Infix.(e1 <= e2)
          | A.Gt -> Infix.(e1 > e2)
          | A.Ge -> Infix.(e1 >= e2)
          | A.And -> Infix.(e1 && e2)
          | A.Or -> Infix.(e1 || e2)
          | A.Add -> Infix.(e1 + e2)
          | A.Sub -> Infix.(e1 - e2)
          | A.Mul -> Infix.(e1 * e2)
          | A.Div -> Infix.(e1 / e2)
          | A.Mod -> Infix.(e1 % e2) )
      | A.Count | A.Min _ | A.Max _ | A.Sum _ | A.Avg _ ->
          failwith "Not a scalar predicate."
      | A.If (p1, p2, p3) -> Ternary (gen_pred p1, gen_pred p2, gen_pred p3)
    in
    gen_pred pred

  let len start type_ =
    let hdr = Header.make_header type_ in
    match type_ with
    | StringT _ ->
        Infix.(
          int (Header.size_exn hdr "nchars") + Header.make_access hdr "nchars" start)
    | _ -> Header.make_access hdr "len" start

  let scan_empty {ctx; name; _} =
    let open Builder in
    let b = create ~ctx ~name ~ret:(Type.PrimType.TupleT []) in
    build_func b

  let scan_null {ctx; name; _} =
    let open Builder in
    let b = create ~ctx ~name ~ret:Type.PrimType.NullT in
    build_yield Null b ; build_func b

  let scan_int args =
    match args with
    | {ctx; name; type_= IntT {nullable; range= (_, h) as range}; _} ->
        let open Builder in
        let b =
          let ret_type = Type.PrimType.TupleT [IntT {nullable}] in
          create ~ctx ~name ~ret:ret_type
        in
        let start = Ctx.find_exn ctx (A.Name.create "start") b in
        let ival = Slice (start, Type.AbsInt.byte_width ~nullable range) in
        if nullable then
          let null_val = h + 1 in
          build_yield (Tuple [Tuple [ival; Infix.(ival = int null_val)]]) b
        else build_yield (Tuple [ival]) b ;
        build_func b
    | _ -> failwith "Unexpected args."

  let scan_bool args =
    match args with
    | {ctx; name; type_= BoolT meta; _} ->
        let open Builder in
        let b = create ~ctx ~name ~ret:(TupleT [BoolT {nullable= meta.nullable}]) in
        let start = Ctx.find_exn ctx (A.Name.create "start") b in
        let ival = Slice (start, 1) in
        if meta.nullable then
          let null_val = 2 in
          build_yield (Tuple [Tuple [ival; Infix.(ival = int null_val)]]) b
        else build_yield (Tuple [ival]) b ;
        build_func b
    | _ -> failwith "Unexpected args."

  let scan_string args =
    match args with
    | {ctx; name; type_= StringT {nchars= l, _; nullable; _} as t; _} ->
        let open Builder in
        let hdr = Header.make_header t in
        let b = create ~ctx ~name ~ret:(TupleT [StringT {nullable}]) in
        let start = Ctx.find_exn ctx (A.Name.create "start") b in
        let value_ptr = Header.make_position hdr "value" start in
        let nchars = Header.make_access hdr "nchars" start in
        let ret_val = Binop {op= LoadStr; arg1= value_ptr; arg2= nchars} in
        if nullable then
          let null_val = l - 1 in
          build_yield (Tuple [Tuple [ret_val; Infix.(nchars = int null_val)]]) b
        else build_yield (Tuple [ret_val]) b ;
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
          let ctx = Ctx.bind ctx "start" int_t cstart in
          let child_ctx, child_args = Ctx.make_callee_context ctx b in
          let child_iter = scan child_ctx clayout ctype in
          build_foreach ~fresh ~count:(Type.count ctype) child_iter child_args
            (fun tup b ->
              let next_ctx =
                let tuple_ctx =
                  Ctx.of_schema A.Meta.(find_exn clayout schema) tup
                in
                Ctx.bind_ctx ctx tuple_ctx
              in
              make_loops next_ctx (tup :: tuples) clayouts ctypes cstarts b )
            b
    in
    let b =
      let ret_type =
        let schema = A.Meta.(find_exn r schema) in
        Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
      in
      create ~ctx ~name ~ret:ret_type
    in
    let hdr = Header.make_header t in
    let start = Ctx.find_exn ctx (A.Name.create "start") b in
    let child_starts =
      let _, ret =
        List.fold_left child_types
          ~init:(Header.make_position hdr "value" start, [])
          ~f:(fun (cstart, ret) ctype ->
            let cstart = build_fresh_defn ~fresh "cstart" cstart b in
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
        let schema = A.Meta.(find_exn r schema) in
        Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
      in
      create ~ctx ~name ~ret:ret_type
    in
    let hdr = Header.make_header t in
    let start = Ctx.find_exn ctx (A.Name.create "start") b in
    let ctx = Ctx.bind ctx "start" int_t start in
    let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
    (* Build iterator initializers using the computed start positions. *)
    build_assign (Header.make_position hdr "value" start) start b ;
    let callee_funcs =
      List.map2_exn child_layouts child_types ~f:(fun callee_layout callee_type ->
          let callee = scan callee_ctx callee_layout callee_type in
          build_iter callee callee_args b ;
          let hdr = Header.make_header callee_type in
          build_assign Infix.(start + Header.make_access hdr "len" start) start b ;
          callee )
    in
    let child_tuples =
      List.map callee_funcs ~f:(fun f -> build_fresh_var ~fresh "t" f.ret_type b)
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
    | `Count x -> build_count_loop ~fresh Infix.(int x) build_body b
    | `Countable | `Unknown ->
        build_body b ;
        let not_done =
          List.fold_left callee_funcs ~init:(Bool true) ~f:(fun acc f ->
              Infix.(acc && not (Done f.name)) )
        in
        build_loop not_done build_body b ) ;
    build_func b

  let scan_unordered_list
      A.({ ctx
         ; name
         ; scan
         ; layout= {node= AList (_, child_layout); _} as r
         ; type_= ListT (child_type, _) as t; _ }) =
    let open Builder in
    let b =
      let ret_type =
        let schema = A.Meta.(find_exn r schema) in
        Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
      in
      create ~ctx ~name ~ret:ret_type
    in
    let hdr = Header.make_header t in
    let start = Ctx.find_exn ctx (A.Name.create "start") b in
    let cstart =
      build_defn "cstart" int_t (Header.make_position hdr "value" start) b
    in
    let callee_ctx, callee_args =
      let ctx = Ctx.bind ctx "start" int_t cstart in
      Ctx.make_callee_context ctx b
    in
    let func = scan callee_ctx child_layout child_type in
    let pcount =
      build_defn "pcount" int_t (Header.make_access hdr "count" start) b
    in
    build_loop
      Infix.(pcount > int 0)
      (fun b ->
        let clen =
          let child_hdr = Header.make_header child_type in
          Header.make_access child_hdr "len" cstart
        in
        build_foreach ~fresh ~count:(Type.count child_type) func callee_args
          build_yield b ;
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
        let schema = A.Meta.(find_exn r schema) in
        Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
      in
      create ~ctx ~name ~ret:ret_type
    in
    let hdr = Header.make_header t in
    let start = Ctx.find_exn ctx (A.Name.create "start") b in
    let kstart = build_var "kstart" int_t b in
    let key_callee_ctx, key_callee_args =
      let ctx = Ctx.bind ctx "start" int_t kstart in
      Ctx.make_callee_context ctx b
    in
    let key_iter = scan key_callee_ctx key_layout key_type in
    let key_tuple = build_var "key" key_iter.ret_type b in
    let ctx =
      let key_schema = A.Meta.(find_exn key_layout schema) in
      Ctx.bind_ctx ctx (Ctx.of_schema key_schema key_tuple)
    in
    let vstart = build_var "vstart" int_t b in
    let value_callee_ctx, value_callee_args =
      let ctx = Ctx.bind ctx "start" int_t vstart in
      Ctx.make_callee_context ctx b
    in
    let value_iter = scan value_callee_ctx value_layout value_type in
    let hash_data_start = Header.make_position hdr "hash_data" start in
    let mapping_start = Header.make_position hdr "hash_map" start in
    let mapping_len = Header.make_access hdr "hash_map_len" start in
    let lookup_expr = List.map lookup ~f:(fun p -> gen_pred ~ctx p b) in
    (* Compute the index in the mapping table for this key. *)
    let hash_key =
      match lookup_expr with
      | [] -> failwith "empty hash key"
      | [x] -> Infix.(hash hash_data_start x)
      | xs -> Infix.(hash hash_data_start (Tuple xs))
    in
    let hash_key = Infix.(hash_key * int 8) in
    (* Get a pointer to the value. *)
    let value_ptr = Infix.(Slice (mapping_start + hash_key, 8)) in
    (* If the pointer is null, then the key is not present. *)
    build_if
      ~cond:
        Infix.(hash_key < int 0 || hash_key >= mapping_len || value_ptr = int 0x0)
      ~then_:(fun _ -> ())
      ~else_:(fun b ->
        build_assign value_ptr kstart b ;
        build_iter key_iter key_callee_args b ;
        build_step key_tuple key_iter b ;
        build_assign Infix.(value_ptr + len value_ptr key_type) vstart b ;
        build_if
          ~cond:(build_eq key_tuple (Tuple lookup_expr) b)
          ~then_:
            (build_foreach ~fresh ~count:(Type.count value_type) value_iter
               value_callee_args (fun value_tup b ->
                 build_yield (build_concat [key_tuple; value_tup] b) b ))
          ~else_:(fun _ -> ())
          b )
      b ;
    build_func b

  [@@@warning "+8"]

  let build_bin_search build_key n low_target high_target callback b =
    let open Builder in
    let low = build_fresh_defn ~fresh "low" Infix.(int 0) b in
    let high = build_fresh_defn ~fresh "high" n b in
    build_loop
      Infix.(low < high)
      (fun b ->
        let mid = build_fresh_defn ~fresh "mid" Infix.((low + high) / int 2) b in
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
        let key = build_fresh_defn ~fresh "key" (build_key low b) b in
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
            let schema = A.Meta.(find_exn r schema) in
            Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
          in
          create ~ctx ~name ~ret:ret_type
        in
        let start = Ctx.find_exn ctx (A.Name.create "start") b in
        let kstart = build_var "kstart" int_t b in
        let key_callee_ctx, key_callee_args =
          let ctx = Ctx.bind ctx "start" int_t kstart in
          Ctx.make_callee_context ctx b
        in
        let key_iter = scan key_callee_ctx key_layout key_type in
        let key_tuple = build_var "key" key_iter.ret_type b in
        let ctx =
          let key_schema = A.Meta.(find_exn key_layout schema) in
          Ctx.bind_ctx ctx (Ctx.of_schema key_schema key_tuple)
        in
        let vstart = build_var "vstart" int_t b in
        let value_callee_ctx, value_callee_args =
          let ctx = Ctx.bind ctx "start" int_t vstart in
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
          let key = build_fresh_var ~fresh "key" key_iter.ret_type b in
          build_step key key_iter b ; key
        in
        let n = Infix.(index_len / kp_len) in
        build_bin_search key_index n (gen_pred ~ctx lookup_low b)
          (gen_pred ~ctx lookup_high b)
          (fun key idx b ->
            build_assign
              Infix.(Slice (index_start + (idx * kp_len) + key_len, 8))
              vstart b ;
            build_assign key key_tuple b ;
            build_foreach ~fresh ~count:(Type.count value_type) value_iter
              value_callee_args
              (fun value b -> build_yield (build_concat [key; value] b) b)
              b )
          b ;
        build_func b
    | _ -> failwith "Unexpected args."

  let printer ctx name func =
    let open Builder in
    let b = create ~ctx ~name ~ret:VoidT in
    build_foreach ~fresh func [] (fun x b -> build_print x b) b ;
    build_func b

  let counter ctx name func =
    let open Builder in
    let open Infix in
    let b = create ~name ~ctx ~ret:(IntT {nullable= false}) in
    let c = build_defn "c" int_t Infix.(int 0) b in
    build_foreach ~fresh func [] (fun _ b -> build_assign (c + int 1) c b) b ;
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
            let schema = A.Meta.(find_exn r schema) in
            Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
          in
          create ~ctx ~name ~ret:ret_type
        in
        let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
        let func = scan callee_ctx child_layout child_type in
        build_foreach ~fresh ~count:(Type.count child_type) func callee_args
          (fun tup b ->
            let ctx =
              let child_schema = A.Meta.(find_exn child_layout schema) in
              Ctx.bind_ctx ctx (Ctx.of_schema child_schema tup)
            in
            let cond = gen_pred ~ctx pred b in
            build_if ~cond
              ~then_:(fun b -> build_yield tup b)
              ~else_:(fun _ -> ())
              b )
          b ;
        build_func b
    | _ -> failwith "Unexpected args."

  let agg_init b =
    let open Builder in
    function
    | A.Count -> `Int (build_fresh_defn ~fresh "count" Infix.(int 0) b)
    | A.Sum _ -> `Int (build_fresh_defn ~fresh "sum" Infix.(int 0) b)
    | A.Min _ -> `Int (build_fresh_defn ~fresh "min" Infix.(int Int.max_value) b)
    | A.Max _ -> `Int (build_fresh_defn ~fresh "max" Infix.(int Int.min_value) b)
    | A.Avg _ ->
        `Avg
          ( build_fresh_defn ~fresh "avg_num" Infix.(int 0) b
          , build_fresh_defn ~fresh "avg_dem" Infix.(int 0) b )
    | _ -> failwith "Not an aggregate."

  let agg_step ctx b agg acc =
    let open Builder in
    match (agg, acc) with
    | A.Count, `Int x -> build_assign Infix.(x + int 1) x b
    | A.Sum f, `Int x -> build_assign Infix.(x + gen_pred ~ctx f b) x b
    | A.Min f, `Int x ->
        let v = gen_pred ~ctx f b in
        build_assign (Ternary (Infix.(v < x), v, x)) x b
    | A.Max f, `Int x ->
        let v = gen_pred ~ctx f b in
        build_assign (Ternary (Infix.(v > x), v, x)) x b
    | A.Avg f, `Avg (n, d) ->
        let v = gen_pred ~ctx f b in
        build_assign Infix.(n + v) n b ;
        build_assign Infix.(d + int 1) d b
    | _ -> failwith "Not an aggregate."

  let agg_extract = function `Int x -> x | `Avg (n, d) -> Infix.(n / d)

  let scan_select args =
    match args with
    | { ctx
      ; name
      ; scan
      ; layout= {node= Select (args, child_layout); _} as layout
      ; type_= FuncT ([child_type], _); _ } ->
        let open Builder in
        let b =
          let ret_type =
            let schema = A.Meta.(find_exn layout schema) in
            Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
          in
          create ~ctx ~name ~ret:ret_type
        in
        let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
        let func = scan callee_ctx child_layout child_type in
        ( match A.select_kind args |> Or_error.ok_exn with
        | `Scalar ->
            build_foreach ~fresh ~count:(Type.count child_type) func callee_args
              (fun tup b ->
                let ctx =
                  let child_schema = A.Meta.(find_exn child_layout schema) in
                  Ctx.bind_ctx ctx (Ctx.of_schema child_schema tup)
                in
                let output = List.map args ~f:(fun p -> gen_pred ~ctx p b) in
                build_yield (Tuple output) b )
              b
        | `Agg ->
            (* Holds the state for each aggregate. *)
            let agg_temps = List.map args ~f:(agg_init b) in
            build_foreach ~fresh ~count:(Type.count child_type) func callee_args
              (fun tup b ->
                let ctx =
                  let child_schema = A.Meta.(find_exn child_layout schema) in
                  Ctx.bind_ctx ctx (Ctx.of_schema child_schema tup)
                in
                List.iter2_exn args agg_temps ~f:(agg_step ctx b) )
              b ;
            build_yield (Tuple (List.map ~f:agg_extract agg_temps)) b ) ;
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
    Out_channel.with_file "scanner.sexp" ~f:(fun ch ->
        Sexp.pp_hum (Caml.Format.formatter_of_out_channel ch) ([%sexp_of: A.t] r) ) ;
    let rec gen_func ctx r t =
      let name = A.name r ^ "_" ^ Fresh.name fresh "%d" in
      let ctx =
        let start_name = A.Name.create ~type_:int_t "start" in
        if Config.code_only then
          Map.update ctx start_name ~f:(function
            | Some x -> x
            | None -> Ctx.(Global Infix.(int 0)) )
        else
          match A.Meta.(find r pos) with
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
        match (r.node, t) with
        | _, Type.IntT _ -> scan_int scan_args
        | _, BoolT _ -> scan_bool scan_args
        | _, StringT _ -> scan_string scan_args
        | _, EmptyT -> scan_empty scan_args
        | _, NullT -> scan_null scan_args
        | ATuple (_, Cross), TupleT _ -> scan_crosstuple scan_args
        | ATuple (_, Zip), TupleT _ -> scan_ziptuple scan_args
        | AList _, ListT _ -> scan_unordered_list scan_args
        | AHashIdx _, HashIdxT _ -> scan_hash_idx scan_args
        | AOrderedIdx _, OrderedIdxT _ -> scan_ordered_idx scan_args
        | Select _, FuncT _ -> scan_select scan_args
        | Filter _, FuncT _ -> scan_filter scan_args
        | _ ->
            Error.create "Unsupported at runtime." r [%sexp_of: A.t] |> Error.raise
      in
      add_func func ; func
    and scan ctx r t =
      match r.node with As (_, r) -> scan ctx r t | _ -> gen_func ctx r t
    in
    (scan ctx r type_, len)

  let irgen ~params ~data_fn r =
    let ctx =
      List.map params ~f:(fun n -> (n, Ctx.Global (Var n.A.Name.name)))
      |> Map.of_alist_exn (module A.Name.Compare_no_type)
    in
    let top_func, len = gen_abslayout ~ctx ~data_fn r in
    { iters= List.rev !funcs
    ; funcs= [printer ctx "printer" top_func; counter ctx "counter" top_func]
    ; params
    ; buffer_len= len }

  let pp fmt {funcs; iters; _} =
    let open Format in
    pp_open_vbox fmt 0 ;
    List.iter (iters @ funcs) ~f:(pp_func fmt) ;
    pp_close_box fmt () ;
    pp_print_flush fmt ()
end
