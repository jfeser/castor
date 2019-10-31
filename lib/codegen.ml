open! Core
open Collections
open Llvm_analysis
open Llvm_target
open Llvm_ext

module type S = Codegen_intf.S

(* Turn on some llvm error handling. *)
let () =
  enable_pretty_stacktrace ();
  install_fatal_error_handler (fun err ->
      let ocaml_trace = Backtrace.get () in
      print_endline (Backtrace.to_string ocaml_trace);
      print_endline "";
      print_endline err);
  Llvm_all_backends.initialize ()

module Project_config = Config

module Config = struct
  module type S = sig
    val debug : bool
  end
end

module Make (Config : Config.S) (IG : Irgen.S) () = struct
  module I = Implang
  open Config

  let triple = Target.default_triple ()

  let data_layout =
    let machine = TargetMachine.create ~triple Target.(by_triple triple) in
    TargetMachine.data_layout machine

  let clang =
    let configs =
      [ "/usr/local/Cellar/llvm/9*/bin/clang"; "clang-9"; "clang" ]
    in
    let c =
      List.find configs ~f:(fun c ->
          Sys.command (sprintf "which %s > /dev/null 2>&1" c) = 0)
    in
    Option.value_exn ~message:"Could not find a working clang." c

  let opt = Project_config.llvm_root ^ "/bin/opt"

  let ctx = create_context ()

  let module_ =
    let m = create_module ctx "scanner" in
    set_data_layout (DataLayout.as_string data_layout) m;
    set_target_triple triple m;
    m

  let Builtin.
        {
          llvm_lifetime_start;
          llvm_lifetime_end;
          cmph_search_packed;
          printf;
          strncmp;
          strncpy;
          strpos;
          extract_y;
          extract_m;
          extract_d;
          add_m;
          add_y;
        } =
    Builtin.create module_

  let builder = builder ctx

  let root = Tbaa.TypeDesc.Root (Some "castor_root")

  let db_val = Tbaa.TypeDesc.Scalar { name = "db"; parent = root }

  let db_int = Tbaa.TypeDesc.Scalar { name = "db_int"; parent = db_val }

  let db_bool = Tbaa.TypeDesc.Scalar { name = "db_bool"; parent = db_val }

  let param_val = Tbaa.TypeDesc.Scalar { name = "param"; parent = root }

  let runtime_val = Tbaa.TypeDesc.Scalar { name = "runtime"; parent = root }

  let consumer_val = Tbaa.TypeDesc.Scalar { name = "consumer"; parent = root }

  let string_val = Tbaa.TypeDesc.Scalar { name = "string"; parent = root }

  let tbaa_ctx =
    Tbaa.to_meta ctx
      [
        root;
        db_val;
        param_val;
        runtime_val;
        consumer_val;
        db_int;
        db_bool;
        string_val;
      ]

  let tag ?offset ?constant ?access base instr =
    set_metadata instr (Tbaa.kind ctx)
      (Tbaa.tag ?offset ?constant ?access ctx tbaa_ctx base);
    instr

  let apply_tag = tag

  (* Variables are either stored in the parameter struct or stored locally on
     the stack. Values that are stored in the parameter struct are also stored
     locally, but they are stored back to the parameter struct whenever the
     iterator returns to the caller. *)
  type var =
    | Param of { idx : int; alloca : llvalue option }
    | Local of llvalue
  [@@deriving sexp_of]

  module SymbolTable = struct
    type t = var Hashtbl.M(String).t list [@@deriving sexp_of]

    let lookup ?params maps key =
      let params =
        match (params, maps) with
        | Some p, _ -> p
        | None, m :: _ -> (
            match Hashtbl.find m "params" with
            | Some (Local v) -> v
            | Some (Param _) ->
                Error.create "Params mistagged." m
                  [%sexp_of: var Hashtbl.M(String).t]
                |> Error.raise
            | None ->
                Error.create "Params not found." (Hashtbl.keys m)
                  [%sexp_of: string list]
                |> Error.raise )
        | None, [] -> Error.of_string "Empty namespace." |> Error.raise
      in
      (* Look up a name in a scope. *)
      let lookup_single m k =
        Option.map (Hashtbl.find m k) ~f:(function
          | Param { alloca = Some x; _ } | Local x -> x
          | Param { idx; alloca = None } ->
              build_struct_gep params idx k builder)
      in
      (* Look up a name in a scope list. The first scope which contains the
         name's value is returned. *)
      let rec lookup_chain ms k =
        match ms with
        | [] -> assert false
        | [ m ] -> (
            match lookup_single m k with
            | Some v -> v
            | None ->
                Error.create "Unknown variable."
                  (k, List.map ~f:Hashtbl.keys maps)
                  [%sexp_of: string * string list list]
                |> Error.raise )
        | m :: ms -> (
            match lookup_single m k with
            | Some v -> v
            | None -> lookup_chain ms k )
      in
      lookup_chain maps key
  end

  class fctx func params =
    object
      val values = Hashtbl.create (module String)

      val tctx =
        let kv =
          List.map func.I.locals ~f:(fun { lname; type_; _ } -> (lname, type_))
          @ params
        in
        match Hashtbl.of_alist (module String) kv with
        | `Ok x -> x
        | `Duplicate_key k ->
            Error.create "Duplicate key." (k, func.I.locals, params)
              [%sexp_of:
                string * I.local list * (string * Type.PrimType.t) list]
            |> Error.raise

      method values : var Hashtbl.M(String).t = values

      method name : string = func.I.name

      method func : I.func = func

      method tctx : Type.PrimType.t Hashtbl.M(String).t = tctx

      val mutable llfunc = None

      method set_llfunc x = llfunc <- Some x

      method llfunc : llvalue = Option.value_exn llfunc
    end

  let funcs = Hashtbl.create (module String)

  let globals = Hashtbl.create (module String)

  let params_struct_t = ref (void_type ctx)

  let get_val ?params ctx = SymbolTable.lookup ?params [ ctx#values; globals ]

  let call_printf fmt_str args =
    let fmt_str_ptr =
      build_bitcast fmt_str (pointer_type (i8_type ctx)) "" builder
    in
    let fmt_args = Array.append [| fmt_str_ptr |] (Array.of_list args) in
    build_call printf fmt_args "" builder |> ignore

  let _debug_printf fmt args =
    if debug then
      let fmt_str =
        define_fresh_global (const_stringz ctx fmt) "fmt" module_
      in
      call_printf fmt_str args

  let int_type = i64_type ctx

  let int_size =
    const_int (i64_type ctx)
      (DataLayout.size_in_bits int_type data_layout |> Int64.to_int_exn)

  let str_pointer_type = pointer_type (i8_type ctx)

  let str_len_type = i64_type ctx

  let str_type = struct_type ctx [| str_pointer_type; str_len_type |]

  let bool_type = i1_type ctx

  let fixed_type = double_type ctx

  let rec codegen_type t =
    let open Type.PrimType in
    match t with
    | IntT _ | DateT _ -> int_type
    | BoolT _ -> bool_type
    | StringT _ -> str_type
    | TupleT ts ->
        struct_type ctx (List.map ts ~f:codegen_type |> Array.of_list)
    | VoidT | NullT -> void_type ctx
    | FixedT _ -> fixed_type

  (** Generate a list of lltypes suitable for a tuple consumer function.
     Flattens tuple and string structs into separate arguments. *)

  module Llstring = struct
    type t = { pos : llvalue; len : llvalue }

    let unpack v =
      {
        pos = build_extractvalue v 0 "pos" builder;
        len = build_extractvalue v 1 "len" builder;
      }

    let pack ?(tag = runtime_val) s =
      let struct_ = build_entry_alloca ctx str_type "str" builder in
      build_store s.pos (build_struct_gep struct_ 0 "str_ptr" builder) builder
      |> apply_tag tag |> ignore;
      build_store s.len (build_struct_gep struct_ 1 "str_len" builder) builder
      |> apply_tag tag |> ignore;
      build_load struct_ "strptr" builder |> apply_tag tag
  end

  module Lltuple = struct
    let pack vs =
      let ts = List.map vs ~f:type_of |> Array.of_list in
      let struct_t = struct_type ctx ts in
      let struct_ = build_entry_alloca ctx struct_t "tupleptrtmp" builder in
      List.iteri vs ~f:(fun i v ->
          let ptr = build_struct_gep struct_ i "ptrtmp" builder in
          build_store v ptr builder |> tag runtime_val |> ignore);
      build_load struct_ "tupletmp" builder |> tag runtime_val

    let unpack v =
      let typ = type_of v in
      let len =
        match classify_type typ with
        | Struct -> Array.length (struct_element_types typ)
        | _ ->
            Error.(
              createf "Expected a tuple but got %s." (string_of_llvalue v)
              |> raise)
      in
      List.init len ~f:(fun i -> build_extractvalue v i "elemtmp" builder)
  end

  let scmp =
    let func =
      declare_function "scmp"
        (function_type bool_type [| str_type; str_type |])
        module_
    in
    add_function_attr func
      (create_enum_attr ctx "speculatable" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "norecurse" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "nounwind" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "readonly" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "argmemonly" 0L)
      AttrIndex.Function;
    let bb = append_block ctx "entry" func in
    let eq_bb = append_block ctx "eq" func in
    let neq_bb = append_block ctx "neq" func in
    position_at_end bb builder;
    let Llstring.{ pos = p1; len = l1 } = Llstring.unpack (param func 0) in
    let Llstring.{ pos = p2; len = l2 } = Llstring.unpack (param func 1) in
    build_cond_br
      (build_icmp Icmp.Eq l1 l2 "len_cmp" builder)
      eq_bb neq_bb builder
    |> ignore;
    position_at_end eq_bb builder;
    let ret =
      build_call strncmp [| p1; p2; l1 |] "str_cmp" builder |> tag string_val
    in
    let ret =
      build_icmp Icmp.Eq ret (const_int (i32_type ctx) 0) "str_cmp" builder
    in
    let ret = build_intcast ret bool_type "str_cmp_bool" builder in
    build_ret ret builder |> ignore;
    position_at_end neq_bb builder;
    build_ret (const_int bool_type 0) builder |> ignore;
    assert_valid_function func;
    func

  let codegen_string x =
    Llstring.(
      pack
        {
          pos = build_global_stringptr x "" builder;
          len = const_int int_type (String.length x);
        })

  let codegen_slice codegen_expr fctx offset size_bytes =
    let size_bits = 8 * size_bytes in
    let offset = codegen_expr fctx offset in
    let buf_ptr =
      build_load (get_val fctx "buf") "buf_ptr" builder
      |> tag ~constant:true db_val
    in
    let buf_ptr =
      build_pointercast buf_ptr
        (pointer_type (i8_type ctx))
        "buf_ptr_cast" builder
    in
    let slice_ptr =
      build_in_bounds_gep buf_ptr [| offset |] "slice_ptr" builder
    in
    let slice_ptr =
      build_pointercast slice_ptr
        (pointer_type (integer_type ctx size_bits))
        "slice_ptr_cast" builder
    in
    let slice =
      build_load slice_ptr "slice_val" builder |> tag ~constant:true db_int
    in
    (* Convert the slice to a 64 bit int. *)
    let slice = build_intcast slice (i64_type ctx) "int_val" builder in
    slice

  let codegen_load_bool fctx offset =
    let size_bits = 8 in
    let buf_ptr =
      build_load (get_val fctx "buf") "buf_ptr" builder
      |> tag ~constant:true db_val
    in
    let buf_ptr =
      build_pointercast buf_ptr
        (pointer_type (i8_type ctx))
        "buf_ptr_cast" builder
    in
    let slice_ptr =
      build_in_bounds_gep buf_ptr [| offset |] "slice_ptr" builder
    in
    let slice_ptr =
      build_pointercast slice_ptr
        (pointer_type (integer_type ctx size_bits))
        "slice_ptr_cast" builder
    in
    let slice =
      build_load slice_ptr "slice_val" builder |> tag ~constant:true db_bool
    in
    (* Convert the slice to a 64 bit int. *)
    let slice = build_trunc slice (i1_type ctx) "bool_val" builder in
    slice

  let codegen_index codegen_expr tup idx =
    let lltup = codegen_expr tup in
    List.nth_exn (Lltuple.unpack lltup) idx

  let codegen_hash fctx hash_offset key_ptr key_size =
    let buf_ptr =
      build_load (get_val fctx "buf") "buf_ptr" builder
      |> tag ~constant:true db_val
    in
    let buf_ptr =
      build_pointercast buf_ptr (pointer_type (i8_type ctx)) "buf_ptr" builder
    in
    let hash_ptr =
      build_in_bounds_gep buf_ptr [| hash_offset |] "hash_ptr" builder
    in
    let key_ptr =
      build_pointercast key_ptr (pointer_type (i8_type ctx)) "key_ptr" builder
    in
    let key_size = build_intcast key_size (i32_type ctx) "key_size" builder in
    let args = [| hash_ptr; key_ptr; key_size |] in
    let hash_val =
      build_call cmph_search_packed args "hash_val" builder |> tag db_val
    in
    build_intcast hash_val (i64_type ctx) "hash_val_cast" builder

  let codegen_int_hash fctx hash_ptr key =
    let key_ptr = build_entry_alloca ctx (type_of key) "key_ptr" builder in
    let key_ptr_cast =
      build_pointercast key_ptr
        (pointer_type (i8_type ctx))
        "key_ptr_cast" builder
    in
    build_call llvm_lifetime_start [| int_size; key_ptr_cast |] "" builder
    |> ignore;
    build_store key key_ptr builder |> tag runtime_val |> ignore;
    let key_size =
      build_intcast (size_of (type_of key)) (i32_type ctx) "key_size" builder
    in
    let ret = codegen_hash fctx hash_ptr key_ptr_cast key_size in
    build_call llvm_lifetime_end [| int_size; key_ptr_cast |] "" builder
    |> ignore;
    ret

  let codegen_string_hash fctx hash_ptr key =
    let Llstring.{ pos = key_ptr; len = key_size } = Llstring.unpack key in
    let key_size = build_intcast key_size (i32_type ctx) "key_size" builder in
    codegen_hash fctx hash_ptr key_ptr key_size

  let codegen_tuple_hash fctx types hash_ptr key =
    let key_size =
      List.foldi types
        ~init:(const_int (i64_type ctx) 0)
        ~f:(fun idx size ->
          let open Type.PrimType in
          function
          | (NullT | VoidT | TupleT _) as t ->
              Error.create "Not supported as part of a composite key." t
                [%sexp_of: t]
              |> Error.raise
          | IntT _ | DateT _ -> build_add (size_of int_type) size "" builder
          | FixedT _ ->
              let size = build_add (size_of int_type) size "" builder in
              build_add (size_of int_type) size "" builder
          | StringT _ ->
              let str_struct = build_extractvalue key idx "" builder in
              let Llstring.{ len = str_size; _ } =
                Llstring.unpack str_struct
              in
              let str_size =
                build_intcast str_size int_type "key_size" builder
              in
              build_add str_size size "" builder
          | BoolT _ -> build_add (size_of bool_type) size "" builder)
    in
    let key_size =
      build_add key_size
        (const_int (i64_type ctx) (List.length types - 1))
        "" builder
    in
    let key_ptr = build_array_alloca (i8_type ctx) key_size "" builder in
    let key_offset = build_ptrtoint key_ptr (i64_type ctx) "" builder in
    List.foldi ~init:key_offset types ~f:(fun idx key_offset type_ ->
        let open Type.PrimType in
        let key_offset =
          match type_ with
          | IntT _ | DateT _ ->
              let key_ptr =
                build_inttoptr key_offset (pointer_type int_type) "" builder
              in
              let v = build_extractvalue key idx "" builder in
              build_store v key_ptr builder |> tag runtime_val |> ignore;
              let key_ptr = build_ptrtoint key_ptr (i64_type ctx) "" builder in
              build_add key_ptr (size_of int_type) "" builder
          | BoolT _ ->
              let key_ptr =
                build_inttoptr key_offset (pointer_type bool_type) "" builder
              in
              let v = build_extractvalue key idx "" builder in
              build_store v key_ptr builder |> tag runtime_val |> ignore;
              let key_ptr = build_ptrtoint key_ptr (i64_type ctx) "" builder in
              build_add key_ptr (size_of bool_type) "" builder
          | StringT _ ->
              let key_ptr =
                build_inttoptr key_offset
                  (pointer_type (i8_type ctx))
                  "" builder
              in
              let str_struct = build_extractvalue key idx "" builder in
              let Llstring.{ pos = str_ptr; len = str_size } =
                Llstring.unpack str_struct
              in
              let i32_str_size =
                build_intcast str_size (i32_type ctx) "" builder
              in
              build_call strncpy
                [| key_ptr; str_ptr; i32_str_size |]
                "" builder
              |> ignore;
              let key_ptr = build_ptrtoint key_ptr (i64_type ctx) "" builder in
              build_add key_ptr str_size "" builder
          | (NullT | VoidT | TupleT _ | FixedT _) as t ->
              Error.create "Not supported as part of a composite key." t
                [%sexp_of: t]
              |> Error.raise
        in
        let key_offset =
          if idx < List.length types - 1 then (
            let key_ptr =
              build_inttoptr key_offset (pointer_type (i8_type ctx)) "" builder
            in
            build_store
              (const_int (i8_type ctx) (Char.to_int '|'))
              key_ptr builder
            |> tag runtime_val |> ignore;
            let key_offset =
              build_ptrtoint key_ptr (i64_type ctx) "" builder
            in
            build_add key_offset (size_of (i8_type ctx)) "" builder )
          else key_offset
        in
        key_offset)
    |> ignore;
    let hash = codegen_hash fctx hash_ptr key_ptr key_size in
    hash

  let codegen_load_str fctx offset len =
    let buf =
      build_load (get_val fctx "buf") "buf_ptr" builder
      |> tag ~constant:true db_val
    in
    let buf = build_pointercast buf (pointer_type (i8_type ctx)) "" builder in
    let ptr = build_in_bounds_gep buf [| offset |] "" builder in
    let ptr =
      build_pointercast ptr (pointer_type (i8_type ctx)) "string_ptr" builder
    in
    Llstring.(pack { pos = ptr; len })

  let codegen_strpos _ x1 x2 =
    let s1 = Llstring.unpack x1 in
    let s2 = Llstring.unpack x2 in
    build_call strpos [| s1.pos; s1.len; s2.pos; s2.len |] "" builder
    |> tag string_val

  let codegen_binop codegen_expr fctx op arg1 arg2 =
    let x1 = codegen_expr fctx arg1 in
    let x2 = codegen_expr fctx arg2 in
    let x_out =
      match op with
      | `IntAdd -> build_nsw_add x1 x2 "addtmp" builder
      | `IntSub -> build_nsw_sub x1 x2 "subtmp" builder
      | `IntMul -> build_nsw_mul x1 x2 "multmp" builder
      | `IntDiv -> build_sdiv x1 x2 "divtmp" builder
      | `FlAdd -> build_fadd x1 x2 "addtmp" builder
      | `FlSub -> build_fsub x1 x2 "subtmp" builder
      | `FlMul -> build_fmul x1 x2 "multmp" builder
      | `FlDiv -> build_fdiv x1 x2 "divtmp" builder
      | `Mod -> build_srem x1 x2 "modtmp" builder
      | `Lsr -> build_lshr x1 x2 "" builder
      | `IntEq -> build_icmp Icmp.Eq x1 x2 "eqtmp" builder
      | `StrEq ->
          build_call scmp [| x1; x2 |] "eqtmp" builder |> tag string_val
      | `IntLt -> build_icmp Icmp.Slt x1 x2 "lttmp" builder
      | `FlLt -> build_fcmp Fcmp.Olt x1 x2 "lttmp" builder
      | `FlLe -> build_fcmp Fcmp.Ole x1 x2 "letmp" builder
      | `FlEq -> build_fcmp Fcmp.Oeq x1 x2 "eqtmp" builder
      | `And -> build_and x1 x2 "andtmp" builder
      | `Or -> build_or x1 x2 "ortmp" builder
      | `IntHash -> codegen_int_hash fctx x1 x2
      | `StrHash -> codegen_string_hash fctx x1 x2
      | `LoadStr -> codegen_load_str fctx x1 x2
      | `StrPos -> codegen_strpos fctx x1 x2
      | `AddY -> build_call add_y [| x1; x2 |] "" builder
      | `AddM -> build_call add_m [| x1; x2 |] "" builder
      | `AddD -> build_nsw_add x1 x2 "addtmp" builder
    in
    x_out

  let codegen_unop codegen_expr fctx op arg =
    let x = codegen_expr fctx arg in
    match op with
    | `Not -> build_not x "nottmp" builder
    | `Int2Fl -> build_sitofp x fixed_type "" builder
    | `Int2Date | `Date2Int -> x
    | `StrLen -> (Llstring.unpack x).len
    | `ExtractY -> build_call extract_y [| x |] "" builder
    | `ExtractM -> build_call extract_m [| x |] "" builder
    | `ExtractD -> build_call extract_d [| x |] "" builder
    | `LoadBool -> codegen_load_bool fctx x

  let codegen_tuple codegen_expr es =
    List.map es ~f:codegen_expr |> Lltuple.pack

  let codegen_ternary codegen_expr e1 e2 e3 =
    let v1 = codegen_expr e1 in
    let v2 = codegen_expr e2 in
    let v3 = codegen_expr e3 in
    build_select v1 v2 v3 "" builder

  let rec codegen_expr fctx = function
    | I.Null -> failwith "TODO: Pick a runtime null rep."
    | Int x -> const_int int_type x
    | Date x -> const_int int_type (Date.to_int x)
    | Fixed x -> const_float fixed_type Float.(of_int x.value / of_int x.scale)
    | Bool true -> const_int bool_type 1
    | Bool false -> const_int bool_type 0
    | Var n ->
        let v = get_val fctx n in
        build_load v n builder |> tag runtime_val
    | String x -> codegen_string x
    | Done _ -> failwith "Iterators are unsupported."
    | Slice (byte_idx, size_bytes) ->
        codegen_slice codegen_expr fctx byte_idx size_bytes
    | Index (tup, idx) -> codegen_index (codegen_expr fctx) tup idx
    | Binop { op; arg1; arg2 } -> codegen_binop codegen_expr fctx op arg1 arg2
    | Unop { op; arg } -> codegen_unop codegen_expr fctx op arg
    | Tuple es -> codegen_tuple (codegen_expr fctx) es
    | Ternary (e1, e2, e3) -> codegen_ternary (codegen_expr fctx) e1 e2 e3
    | TupleHash (ts, e1, e2) ->
        codegen_tuple_hash fctx ts (codegen_expr fctx e1)
          (codegen_expr fctx e2)
    | Substr (e1, e2, e3) ->
        let v1 = codegen_expr fctx e1 in
        let v2 = codegen_expr fctx e2 in
        let new_len = codegen_expr fctx e3 in
        let s = Llstring.unpack v1 in
        let new_pos =
          build_add
            (build_sub v2 (const_int int_type 1) "" builder)
            s.pos "" builder
        in
        Llstring.pack { pos = new_pos; len = new_len }

  let codegen_loop fctx codegen_prog cond body =
    let start_bb = insertion_block builder in
    let llfunc = block_parent start_bb in
    (* Create all loop blocks. *)
    let cond_bb = append_block ctx "loopcond" llfunc in
    let body_bb = append_block ctx "loopbody" llfunc in
    let end_bb = append_block ctx "loopend" llfunc in
    (* Create branch to head block. *)
    position_at_end start_bb builder;
    build_br cond_bb builder |> ignore;

    (* In loop header, check condition and branch to loop body. *)
    position_at_end cond_bb builder;
    let llcond = codegen_expr fctx cond in
    build_cond_br llcond body_bb end_bb builder |> ignore;

    (* Generate the loop body. *)
    position_at_end body_bb builder;
    codegen_prog fctx body;
    let llcond = codegen_expr fctx cond in
    build_cond_br llcond body_bb end_bb builder |> ignore;

    (* At the end of the loop body, check condition and branch. *)
    position_at_end end_bb builder

  let codegen_if fctx codegen_prog cond tcase fcase =
    let start_bb = insertion_block builder in
    let llfunc = block_parent start_bb in
    (* Create all if blocks. *)
    let if_bb = append_block ctx "if" llfunc in
    let then_bb = append_block ctx "then" llfunc in
    let else_bb = append_block ctx "else" llfunc in
    let merge_bb = append_block ctx "ifend" llfunc in
    (* Build branch to head block. *)
    position_at_end start_bb builder;
    build_br if_bb builder |> ignore;

    (* Generate conditional in head block. *)
    position_at_end if_bb builder;
    let llcond = codegen_expr fctx cond in
    (* let {data; null} = unpack_null llcond in *)
    (* let llcond = build_and data (build_not null "" builder) "" builder in *)
    build_cond_br llcond then_bb else_bb builder |> ignore;

    (* Create then block. *)
    position_at_end then_bb builder;
    codegen_prog fctx tcase;
    build_br merge_bb builder |> ignore;

    (* Create else block. *)
    position_at_end else_bb builder;
    codegen_prog fctx fcase;
    build_br merge_bb builder |> ignore;

    (* End with builder in merge block. *)
    position_at_end merge_bb builder

  let codegen_assign fctx lhs rhs =
    let val_ = codegen_expr fctx rhs in
    let var = get_val fctx lhs in
    build_store val_ var builder |> tag runtime_val |> ignore

  let true_str = build_global_stringptr "t" "true_str" builder

  let false_str = build_global_stringptr "f" "false_str" builder

  let null_str = build_global_stringptr "null" "null_str" builder

  let void_str = build_global_stringptr "()" "void_str" builder

  let sep_str = build_global_stringptr "|" "sep_str" builder

  let newline_str = build_global_stringptr "\n" "newline_str" builder

  let int_fmt = build_global_stringptr "%d" "int_fmt" builder

  let str_fmt = build_global_stringptr "%.*s" "str_fmt" builder

  let float_fmt = build_global_stringptr "%f" "float_fmt" builder

  let date_fmt = build_global_stringptr "%04d-%02d-%02d" "date_fmt" builder

  let codegen_print fctx type_ expr =
    let open Type.PrimType in
    let val_ = codegen_expr fctx expr in
    let rec gen val_ = function
      | NullT -> call_printf null_str []
      | IntT { nullable = false } -> call_printf int_fmt [ val_ ]
      | DateT { nullable = false } ->
          let year = build_call extract_y [| val_ |] "" builder in
          let mon = build_call extract_m [| val_ |] "" builder in
          let day = build_call extract_d [| val_ |] "" builder in
          call_printf date_fmt [ year; mon; day ]
      | BoolT { nullable = false } ->
          let fmt = build_select val_ true_str false_str "" builder in
          call_printf fmt []
      | StringT { nullable = false; _ } ->
          let Llstring.{ pos; len } = Llstring.unpack val_ in
          call_printf str_fmt [ len; pos ]
      | TupleT ts ->
          let last_i = List.length ts - 1 in
          List.zip_exn ts (Lltuple.unpack val_)
          |> List.iteri ~f:(fun i (t, v) ->
                 gen v t;
                 if i < last_i then call_printf sep_str [])
      | VoidT -> call_printf void_str []
      | FixedT _ -> call_printf float_fmt [ val_ ]
      | IntT { nullable = true }
      | DateT { nullable = true }
      | StringT { nullable = true; _ }
      | BoolT { nullable = true } ->
          failwith "Cannot print."
    in
    gen val_ type_;
    call_printf newline_str []

  (** Generate an argument list from a tuple and a type. *)
  let rec codegen_consume_args type_ tup =
    let open Type.PrimType in
    match type_ with
    | IntT _ | DateT _ | BoolT _ | FixedT _ -> [ tup ]
    | StringT _ ->
        let Llstring.{ pos; len } = Llstring.unpack tup in
        [ pos; len ]
    | TupleT ts ->
        let vs = Lltuple.unpack tup in
        List.map2_exn ts vs ~f:codegen_consume_args |> List.concat
    | VoidT | NullT -> []

  (** Ensure that LLVM does not optimize away a value. *)
  let use v =
    let func =
      const_inline_asm
        (function_type (void_type ctx) [| type_of v |])
        "" "X,~{memory}" true false
    in
    build_call func [| v |] "" builder |> tag consumer_val |> ignore

  (** Generate a dummy consumer function that LLVM will not optimize away. *)
  let codegen_consume fctx type_ expr =
    codegen_expr fctx expr |> codegen_consume_args type_ |> List.iter ~f:use

  let codegen_return fctx expr =
    let val_ = codegen_expr fctx expr in
    build_ret val_ builder |> ignore

  (** Create an alloca for each element of the params struct. *)
  let load_params ictx func =
    let params_ptr = param func 0 in
    Hashtbl.mapi_inplace ictx#values ~f:(fun ~key:name ~data ->
        match data with
        | Local _ | Param { alloca = Some _; _ } -> data
        | Param { idx; alloca = None } ->
            let param =
              build_load (get_val ~params:params_ptr ictx name) "" builder
            in
            let param_type = type_of param in
            let param_alloca =
              build_entry_alloca ctx param_type name builder
            in
            build_store param param_alloca builder |> ignore;
            Param { idx; alloca = Some param_alloca })

  let rec codegen_stmt fctx = function
    | I.Loop { cond; body } -> codegen_loop fctx codegen_prog cond body
    | If { cond; tcase; fcase } ->
        codegen_if fctx codegen_prog cond tcase fcase
    | Assign { lhs; rhs } -> codegen_assign fctx lhs rhs
    | Print (type_, expr) -> codegen_print fctx type_ expr
    | Consume (type_, expr) -> codegen_consume fctx type_ expr
    | Return expr -> codegen_return fctx expr
    | Iter _ | Step _ | Yield _ -> failwith "Iterators are unsupported."

  and codegen_prog fctx p = List.iter ~f:(codegen_stmt fctx) p

  let codegen_func fctx =
    let name = fctx#name in
    let I.{ args; locals; ret_type; body; _ } = fctx#func in
    Log.debug (fun m -> m "Codegen for func %s started." name);
    ( if
      (* Check that function is not already defined. *)
      Hashtbl.(mem funcs name)
    then Error.(of_string "Function already defined." |> raise) );

    (* Create function. *)
    let func_t =
      let args_t =
        pointer_type !params_struct_t
        :: List.map args ~f:(fun (_, t) -> codegen_type t)
        |> Array.of_list
      in
      function_type (codegen_type ret_type) args_t
    in
    fctx#set_llfunc (declare_function name func_t module_);
    add_function_attr fctx#llfunc
      (create_enum_attr ctx "readonly" 0L)
      AttrIndex.Function;
    add_function_attr fctx#llfunc
      (create_enum_attr ctx "argmemonly" 0L)
      AttrIndex.Function;
    add_function_attr fctx#llfunc
      (create_enum_attr ctx "nounwind" 0L)
      AttrIndex.Function;
    add_function_attr fctx#llfunc
      (create_enum_attr ctx "norecurse" 0L)
      AttrIndex.Function;
    add_function_attr fctx#llfunc
      (create_enum_attr ctx "noalias" 0L)
      (AttrIndex.Param 0);
    let bb = append_block ctx "entry" fctx#llfunc in
    position_at_end bb builder;
    Hashtbl.set funcs ~key:name ~data:fctx#llfunc;

    (* Create storage space for local variables & iterator args. *)
    List.iter locals ~f:(fun { lname = n; type_ = t; _ } ->
        let lltype = codegen_type t in
        let var = build_alloca lltype n builder in
        Hashtbl.set fctx#values ~key:n ~data:(Local var));

    (* Put arguments into symbol table. *)
    let param_ptr = param fctx#llfunc 0 in
    Hashtbl.set fctx#values ~key:"params" ~data:(Local param_ptr);

    (* Declare the parameters pointer (and all sub-pointers) invariant. *)
    load_params fctx fctx#llfunc;
    codegen_prog fctx body;
    match block_terminator (insertion_block builder) with
    | Some _ -> ()
    | None ->
        build_ret_void builder |> ignore;
        assert_valid_function fctx#llfunc;
        Log.debug (fun m -> m "Codegen for func %s completed." name)

  let codegen_create () =
    let func_t =
      function_type (pointer_type !params_struct_t) [| pointer_type int_type |]
    in
    let llfunc = declare_function "create" func_t module_ in
    let bb = append_block ctx "entry" llfunc in
    let ctx =
      object
        val values =
          Hashtbl.of_alist_exn
            (module String)
            [ ("bufp", Local (param llfunc 0)) ]

        method values = values
      end
    in
    position_at_end bb builder;
    let params = build_malloc !params_struct_t "paramstmp" builder in
    Hashtbl.set ctx#values ~key:"params" ~data:(Local params);
    let buf = get_val ctx "buf" in
    let bufp = get_val ctx "bufp" in
    let bufp =
      build_bitcast bufp (type_of buf |> element_type) "tmpbufp" builder
    in
    build_store bufp buf builder |> ignore;
    build_ret params builder |> ignore

  module ParamStructBuilder = struct
    type t = { mutable vars : lltype RevList.t }

    let create () = { vars = RevList.empty }

    let build tbl b n t =
      let idx = RevList.length b.vars in
      b.vars <- RevList.(b.vars ++ t);
      match Hashtbl.add tbl ~key:n ~data:(Param { idx; alloca = None }) with
      | `Duplicate -> Error.(of_string "Variable already defined." |> raise)
      | `Ok -> ()

    let build_global = build globals

    let build_param_struct : t -> string -> lltype =
     fun b n ->
      let t = named_struct_type ctx n in
      assert (RevList.length b.vars > 0);
      struct_set_body t (RevList.to_list b.vars |> Array.of_list) false;
      assert (not (is_opaque t));
      t
  end

  let codegen_param_setters params =
    List.iter params ~f:(fun (n, t) ->
        let lltype = codegen_type t in
        let name = sprintf "set_%s" n in
        let llfunc =
          let func_t =
            function_type (void_type ctx)
              [| pointer_type !params_struct_t; lltype |]
          in
          declare_function name func_t module_
        in
        let fctx =
          object
            val values =
              Hashtbl.of_alist_exn
                (module String)
                [ ("params", Local (param llfunc 0)) ]

            method values = values

            method llfunc = llfunc
          end
        in
        Hashtbl.set funcs ~key:name ~data:fctx#llfunc;
        let bb = append_block ctx "entry" llfunc in
        position_at_end bb builder;
        build_store (param llfunc 1) (get_val fctx n) builder |> ignore;
        build_ret_void builder |> ignore)

  let codegen Irgen.{ funcs = ir_funcs; params; buffer_len; _ } =
    Log.info (fun m -> m "Codegen started.");
    let module SB = ParamStructBuilder in
    let sb = SB.create () in
    (* Generate global constant for buffer. *)
    let buf_t = pointer_type (array_type int_type (buffer_len / 8)) in
    SB.build_global sb "buf" buf_t;
    let typed_params =
      List.map params ~f:(fun n -> Name.(name n, type_exn n))
    in
    (* Generate global constants for parameters. *)
    List.iter typed_params ~f:(fun (n, t) ->
        let lltype = codegen_type t in
        SB.build_global sb n lltype);
    let fctxs =
      List.map ir_funcs ~f:(fun func -> new fctx func typed_params)
    in
    params_struct_t := SB.build_param_struct sb "params";
    List.iter fctxs ~f:codegen_func;
    codegen_create ();
    codegen_param_setters typed_params;
    assert_valid_module module_;
    Log.info (fun m -> m "Codegen completed.");
    module_

  let write_header ch =
    let open Caml.Format in
    let rec pp_type fmt t =
      match classify_type t with
      | Struct ->
          Log.warn (fun m -> m "Outputting structure type as string.");
          fprintf fmt "string_t"
      | Void -> fprintf fmt "void"
      | Integer -> (
          match integer_bitwidth t with
          | 1 -> fprintf fmt "bool"
          | 8 -> fprintf fmt "char"
          | 16 -> fprintf fmt "short"
          | 32 -> fprintf fmt "int"
          | 64 -> fprintf fmt "long"
          | x -> Error.(create "Unknown bitwidth" x [%sexp_of: int] |> raise) )
      | Pointer ->
          let elem_t = element_type t in
          let elem_t =
            if [%compare.equal: TypeKind.t] (classify_type elem_t) Array then
              element_type elem_t
            else elem_t
          in
          fprintf fmt "%a *" pp_type elem_t
      | Half | Float | Double | X86fp80 | Fp128 | Ppc_fp128 | Label
      | Function | Array | Vector | Metadata | X86_mmx | Token ->
          Error.(create "Unknown type." t [%sexp_of: lltype] |> raise)
    and pp_params fmt ts =
      Array.iteri ts ~f:(fun i t ->
          if i = 0 then fprintf fmt "params*" else fprintf fmt "%a" pp_type t;
          if i < Array.length ts - 1 then fprintf fmt ",")
    and pp_value_decl fmt v =
      let t = type_of v in
      let n = value_name v in
      let ignore_val () =
        Log.debug (fun m -> m "Ignoring global %s." (string_of_llvalue v))
      in
      match classify_type t with
      | Pointer ->
          let elem_t = element_type t in
          if [%compare.equal: TypeKind.t] (classify_type elem_t) Function then
            let t = elem_t in
            fprintf fmt "%a %s(%a);@," pp_type (return_type t) n pp_params
              (param_types t)
          else ignore_val ()
      | Function ->
          fprintf fmt "%a %s(%a);@," pp_type (return_type t) n pp_params
            (param_types t)
      | Void | Half | Float | Double | X86fp80 | Fp128 | Ppc_fp128 | Label
      | Integer | Struct | Array | Vector | Metadata | X86_mmx | Token ->
          ignore_val ()
    in
    let fmt = Caml.Format.formatter_of_out_channel ch in
    pp_open_vbox fmt 0;
    fprintf fmt "typedef void params;@,";
    fprintf fmt "typedef struct { char *ptr; long len; } string_t;@,";
    fprintf fmt "params* create(void *);@,";
    fprintf fmt "void consumer(params *);@,";
    fprintf fmt "void printer(params *);@,";
    Hashtbl.data funcs |> List.iter ~f:(fun llfunc -> pp_value_decl fmt llfunc);
    pp_close_box fmt ();
    pp_print_flush fmt ()

  let c_template fn args =
    let args_strs = List.map args ~f:(fun (n, x) -> sprintf "-D%s=%s" n x) in
    Util.command_out_exn ([ clang; "-E" ] @ args_strs @ [ fn ])

  let from_fn fn n i =
    let template = Project_config.build_root ^ "/etc/" ^ fn in
    let func =
      c_template template [ ("PARAM_NAME", n); ("PARAM_IDX", Int.to_string i) ]
    in
    let call = sprintf "set_%s(params, input_%s(argv, optind));" n n in
    (func, call)

  let compile ?out_dir ~gprof ~params layout =
    let out_dir =
      match out_dir with Some x -> x | None -> Filename.temp_dir "bin" ""
    in
    if Sys.is_directory out_dir = `No then Unix.mkdir out_dir;
    let stdlib_fn = Project_config.build_root ^ "/etc/castorlib.c" in
    let date_fn = Project_config.build_root ^ "/etc/date.c" in
    let main_fn = out_dir ^ "/main.c" in
    let ir_fn = out_dir ^ "/scanner.ir" in
    let module_fn = out_dir ^ "/scanner.ll" in
    let exe_fn = out_dir ^ "/scanner.exe" in
    let opt_module_fn = out_dir ^ "/scanner-opt.ll" in
    let remarks_fn = out_dir ^ "/remarks.yml" in
    let header_fn = out_dir ^ "/scanner.h" in
    let data_fn = out_dir ^ "/data.bin" in
    let open Type.PrimType in
    (* Generate IR module. *)
    let ir_module =
      let unopt = IG.irgen ~params ~data_fn layout in
      Log.info (fun m -> m "Optimizing intermediate language.");
      Implang_opt.opt unopt
    in
    Out_channel.with_file ir_fn ~f:(fun ch ->
        let fmt = Caml.Format.formatter_of_out_channel ch in
        IG.pp fmt ir_module);

    (* Generate header. *)
    Out_channel.with_file header_fn ~f:write_header;

    (* Generate main file. *)
    let () =
      Log.debug (fun m -> m "Creating main file.");
      let funcs, calls =
        List.filter params ~f:(fun n ->
            List.exists ir_module.Irgen.params ~f:(fun n' -> Name.O.(n = n')))
        |> List.mapi ~f:(fun i n ->
               Log.debug (fun m -> m "Creating loader for %a." Name.pp n);
               let loader_fn =
                 match Name.type_exn n with
                 | NullT -> failwith "No null parameters."
                 | IntT _ -> "load_int.c"
                 | DateT _ -> "load_date.c"
                 | BoolT _ -> "load_bool.c"
                 | StringT _ -> "load_string.c"
                 | FixedT _ -> "load_float.c"
                 | VoidT | TupleT _ -> failwith "Unsupported parameter type."
               in
               (from_fn loader_fn) (Name.name n) i)
        |> List.unzip
      in
      let header_str = "#include \"scanner.h\"" in
      let funcs_str = String.concat (header_str :: funcs) ~sep:"\n" in
      let calls_str = String.concat calls ~sep:"\n" in
      let perf_template = Project_config.build_root ^ "/etc/perf.c" in
      let perf_c =
        let open In_channel in
        with_file perf_template ~f:(fun ch ->
            String.template (input_all ch) [ funcs_str; calls_str ])
      in
      Out_channel.(with_file main_fn ~f:(fun ch -> output_string ch perf_c))
    in
    (* Generate scanner module. *)
    let () =
      let module_ = codegen ir_module in
      Llvm.print_module module_fn module_
    in
    let cflags = [ "$CPPFLAGS"; "-g"; "-lcmph" ] in
    let cflags =
      (if gprof then [ "-pg" ] else [])
      @ (if debug then [ "-O0" ] else [ "-O3" ])
      @ cflags
    in
    if debug then
      Util.command_exn ~quiet:()
        ( [ clang ] @ cflags
        @ [ module_fn; stdlib_fn; date_fn; main_fn; "-o"; exe_fn ] )
    else (
      Util.command_exn ~quiet:()
        [
          opt;
          "-S";
          sprintf "-pass-remarks-output=%s" remarks_fn;
          "-O3 -enable-unsafe-fp-math";
          module_fn;
          ">";
          opt_module_fn;
          "2>/dev/null";
        ];
      Util.command_exn ~quiet:()
        ( [ clang ] @ cflags
        @ [
            opt_module_fn;
            stdlib_fn;
            date_fn;
            main_fn;
            "-o";
            exe_fn;
            "2>/dev/null";
          ] ) );
    (exe_fn, data_fn)
end
